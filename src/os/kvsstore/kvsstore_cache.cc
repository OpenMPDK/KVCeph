#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unordered_set>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <bitset>
#include <memory.h>
#include <functional>
#include <algorithm>

#include "osd/osd_types.h"
#include "os/kv.h"
#include "include/compat.h"
#include "include/mempool.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/safe_io.h"
#include "common/Formatter.h"
#include "common/EventTrace.h"

#include "kvsstore_types.h"
#include "kvsstore_db.h"

// set up dout_context and dout_prefix here
// -----------------------------------------
#define dout_context cct
#define dout_subsys ceph_subsys_kvs
#undef dout_prefix
#define dout_prefix *_dout << "[kvsstore] "

/// mempool
MEMPOOL_DEFINE_OBJECT_FACTORY(KvsStoreTypes::Buffer, kvsstore_buffer, kvsstore_cache_other);


ostream& operator<<(ostream& out, const KvsStoreTypes::Buffer& b)
{
    out << "buffer(" << &b << " space " << b.space << " 0x" << std::hex
        << b.offset << "~" << b.length << std::dec
        << " " << KvsStoreTypes::Buffer::get_state_name(b.state);
    if (b.flags)
        out << " " << KvsStoreTypes::Buffer::get_flag_name(b.flags);
    return out << ")";
}

/// ------------------------------------------------
/// LRU cache for Onode and Buffer
/// ------------------------------------------------

struct LruOnodeCacheShard: public KvsStoreTypes::OnodeCacheShard {
    typedef boost::intrusive::list<
            KvsStoreTypes::Onode,
            boost::intrusive::member_hook<KvsStoreTypes::Onode,
                    boost::intrusive::list_member_hook<>, &KvsStoreTypes::Onode::lru_item> > list_t;
    list_t lru;

    explicit LruOnodeCacheShard(CephContext *cct) :
            OnodeCacheShard(cct) {
    }

    void _add(KvsStoreTypes::OnodeRef &o, int level) override
    {
        (level > 0) ? lru.push_front(*o) : lru.push_back(*o);
        num = lru.size();
    }
    void _rm(KvsStoreTypes::OnodeRef &o) override
    {
        lru.erase(lru.iterator_to(*o));
        num = lru.size();
    }
    void _touch(KvsStoreTypes::OnodeRef &o) override
    {
        lru.erase(lru.iterator_to(*o));
        lru.push_front(*o);
        num = lru.size();
    }
    void _trim_to(uint64_t max) override
    {
        if (max >= lru.size()) {
            return; // don't even try
        }

        uint64_t n = lru.size() - max;

        auto p = lru.end();
        ceph_assert(p != lru.begin());
        --p;
        int skipped = 0;
        int max_skipped = ONODE_LRUCACHE_TRIM_MAX_SKIP_PINNED;
        while (n > 0) {
            KvsStoreTypes::Onode *o = &*p;
            int refs = o->nref.load();
            if (refs > 1) {
                if (++skipped >= max_skipped) {
                    break;
                }

                if (p == lru.begin()) {
                    break;
                } else {
                    p--;
                    n--;
                    continue;
                }
            }
            if (p != lru.begin()) {
                lru.erase(p--);
            } else {
                lru.erase(p);
                ceph_assert(n == 1);
            }
            o->get();  // paranoia
            o->c->onode_map.remove(o->oid);
            o->put();
            --n;
        }
        num = lru.size();
        //TR << "onode cache done: new size = " << num;
    }

    void add_stats(uint64_t *onodes) override
    {
        *onodes += num;
    }
};

// OnodeCacheShard::create
KvsStoreTypes::OnodeCacheShard *KvsStoreTypes::OnodeCacheShard::create(
        CephContext* cct,
        string type,
        PerfCounters *logger)
{
    OnodeCacheShard *c = nullptr;
    // Currently we only implement an LRU cache for onodes
    c = new LruOnodeCacheShard(cct);
    c->logger = logger;
    return c;
}

struct LruBufferCacheShard : public KvsStoreTypes::BufferCacheShard {
    typedef boost::intrusive::list<
            KvsStoreTypes::Buffer,
            boost::intrusive::member_hook<
                    KvsStoreTypes::Buffer,
                    boost::intrusive::list_member_hook<>,
                    &KvsStoreTypes::Buffer::lru_item> > list_t;
    list_t lru;

    explicit LruBufferCacheShard(CephContext *cct) : BufferCacheShard(cct) {}

    void _add(KvsStoreTypes::Buffer *b, int level, KvsStoreTypes::Buffer *near) override {
        if (near) {
            auto q = lru.iterator_to(*near);
            lru.insert(q, *b);
        } else if (level > 0) {
            lru.push_front(*b);
        } else {
            lru.push_back(*b);
        }
        buffer_bytes += b->length;
        num = lru.size();
    }
    void _rm(KvsStoreTypes::Buffer *b) override {
        ceph_assert(buffer_bytes >= b->length);
        buffer_bytes -= b->length;
        auto q = lru.iterator_to(*b);
        lru.erase(q);
        num = lru.size();
    }
    void _move(BufferCacheShard *src, KvsStoreTypes::Buffer *b) override {
        src->_rm(b);
        _add(b, 0, nullptr);
    }
    void _adjust_size(KvsStoreTypes::Buffer *b, int64_t delta) override {
        ceph_assert((int64_t)buffer_bytes + delta >= 0);
        buffer_bytes += delta;
    }
    void _touch(KvsStoreTypes::Buffer *b) override {
        auto p = lru.iterator_to(*b);
        lru.erase(p);
        lru.push_front(*b);
        num = lru.size();
        _audit("_touch_buffer end");
    }

    void _trim_to(uint64_t max) override
    {
        while (buffer_bytes > max) {
            auto i = lru.rbegin();
            if (i == lru.rend()) {
                // stop if lru is now empty
                break;
            }

            KvsStoreTypes::Buffer *b = &*i;
            ceph_assert(b->is_clean());
            dout(20) << __func__ << " rm " << *b << dendl;
            b->space->_rm_buffer(this, b);
        }
        num = lru.size();
    }

    void add_stats(uint64_t *buffers,
                   uint64_t *bytes) override {
        *buffers += num;
        *bytes += buffer_bytes;
    }
#ifdef DEBUG_CACHE
    void _audit(const char *s) override
  {
    dout(10) << __func__ << " " << when << " start" << dendl;
    uint64_t s = 0;
    for (auto i = lru.begin(); i != lru.end(); ++i) {
      s += i->length;
    }
    if (s != buffer_bytes) {
      derr << __func__ << " buffer_size " << buffer_bytes << " actual " << s
           << dendl;
      for (auto i = lru.begin(); i != lru.end(); ++i) {
        derr << __func__ << " " << *i << dendl;
      }
      ceph_assert(s == buffer_bytes);
    }
    dout(20) << __func__ << " " << when << " buffer_bytes " << buffer_bytes
             << " ok" << dendl;
  }
#endif
};

///BufferCacheShard::create
KvsStoreTypes::BufferCacheShard *KvsStoreTypes::BufferCacheShard::create(
        CephContext* cct,
        string type,
        PerfCounters *logger)
{
    BufferCacheShard *c = new LruBufferCacheShard(cct);
    c->logger = logger;
    return c;
}

///--------------------------------------------------------
/// OnodeSpace
///--------------------------------------------------------

KvsStoreTypes::OnodeRef KvsStoreTypes::OnodeSpace::add(const ghobject_t& oid, OnodeRef o)
{
    std::lock_guard l(cache->lock);
    auto p = onode_map.find(oid);
    if (p != onode_map.end()) {
        ldout(cache->cct, 30) << __func__ << " " << oid << " " << o
                              << " raced, returning existing " << p->second
                              << dendl;
        return p->second;
    }
    ldout(cache->cct, 30) << __func__ << " " << oid << " " << o << dendl;
    onode_map[oid] = o;
    cache->_add(o, 1);
    cache->_trim();
    return o;
}

KvsStoreTypes::OnodeRef KvsStoreTypes::OnodeSpace::lookup(const ghobject_t& oid)
{
    ldout(cache->cct, 30) << __func__ << dendl;
    OnodeRef o;

    {
        std::lock_guard l(cache->lock);
        ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
        if (p == onode_map.end()) {
            ldout(cache->cct, 30) << __func__ << " " << oid << " miss" << dendl;
        } else {
            ldout(cache->cct, 30) << __func__ << " " << oid << " hit " << p->second
                                  << dendl;
            cache->_touch(p->second);
            o = p->second;
        }
    }

    return o;
}

void KvsStoreTypes::OnodeSpace::clear()
{
    std::lock_guard l(cache->lock);
    ldout(cache->cct, 10) << __func__ << dendl;
    for (auto &p : onode_map) {
        cache->_rm(p.second);
    }
    onode_map.clear();
}

bool KvsStoreTypes::OnodeSpace::empty()
{
    std::lock_guard l(cache->lock);
    return onode_map.empty();
}

void KvsStoreTypes::OnodeSpace::rename(
        OnodeRef& oldo,
        const ghobject_t& old_oid,
        const ghobject_t& new_oid,
        const mempool::kvsstore_cache_other::string& new_okey)
{
    std::lock_guard l(cache->lock);
    ldout(cache->cct, 30) << __func__ << " " << old_oid << " -> " << new_oid
                          << dendl;
    ceph::unordered_map<ghobject_t,OnodeRef>::iterator po, pn;
    po = onode_map.find(old_oid);
    pn = onode_map.find(new_oid);
    ceph_assert(po != pn);

    ceph_assert(po != onode_map.end());
    if (pn != onode_map.end()) {
        ldout(cache->cct, 30) << __func__ << "  removing target " << pn->second
                              << dendl;
        cache->_rm(pn->second);
        onode_map.erase(pn);
    }
    OnodeRef o = po->second;

    // install a non-existent onode at old location
    oldo.reset(new Onode(o->c, old_oid));
    po->second = oldo;
    cache->_add(po->second, 1);
    // add at new position and fix oid, key
    onode_map.insert(make_pair(new_oid, o));
    cache->_touch(o);
    o->oid = new_oid;
    cache->_trim();
}

bool KvsStoreTypes::OnodeSpace::map_any(std::function<bool(KvsStoreTypes::OnodeRef)> f)
{
    std::lock_guard l(cache->lock);
    ldout(cache->cct, 20) << __func__ << dendl;
    for (auto& i : onode_map) {
        if (f(i.second)) {
            return true;
        }
    }
    return false;
}

template <int LogLevelV = 30>
void KvsStoreTypes::OnodeSpace::dump(CephContext *cct)
{
    for (auto& i : onode_map) {
        ldout(cct, LogLevelV) << i.first << " : " << i.second << dendl;
    }
}

///--------------------------------------------------------
/// BufferSpace
///--------------------------------------------------------


void KvsStoreTypes::BufferSpace::_clear(KvsStoreTypes::BufferCacheShard* cache)
{
    // note: we already hold cache->lock
    ldout(cache->cct, 20) << __func__ << dendl;
    while (!buffer_map.empty()) {
        _rm_buffer(cache, buffer_map.begin());
    }
}


inline uint32_t get_chunk_id(const uint32_t offset) {
    return (offset >> KVS_OBJECT_SPLIT_SHIFT);
}

inline uint32_t get_chunk_offset(uint32_t offset) {
    return (get_chunk_id(offset) << KVS_OBJECT_SPLIT_SHIFT);
}

bool KvsStoreTypes::BufferSpace::get_buffer_address(KvsStoreTypes::BufferCacheShard* cache, uint32_t offset, void **addr, uint64_t *length)
{
    auto i = _data_lower_bound(offset);
    if (i != buffer_map.end()) {
        Buffer *b = i->second.get();
        ceph_assert(b->is_writing());

        boost::container::small_vector<iovec,4> iov;

        b->data.prepare_iov(&iov);

        *addr   = iov[0].iov_base;
        *length = iov[0].iov_len;

        return true;
    }
    return false;
}


void KvsStoreTypes::BufferSpace::read(
        KvsStoreTypes::BufferCacheShard* cache,
        uint32_t offset,
        uint32_t length,
        KvsStoreTypes::ready_regions_t& res,
        interval_set<uint32_t>& res_intervals,
        int flags)
{
    res.clear();
    res_intervals.clear();
    //uint32_t want_bytes = length;
    uint32_t end = offset + length;

    {
        std::lock_guard l(cache->lock);
        for (auto i = _data_lower_bound(offset);
             i != buffer_map.end() && offset < end && i->first < end;
             ++i) {
            Buffer *b = i->second.get();
            ceph_assert(b->end() > offset);

            bool val = false;
            if (flags & BYPASS_CLEAN_CACHE)
                val = b->is_writing();
            else
                val = b->is_writing() || b->is_clean();
            if (val) {
                if (b->offset < offset) {
                    uint32_t skip = offset - b->offset;
                    uint32_t l = min(length, b->length - skip);
                    res[offset].substr_of(b->data, skip, l);
                    res_intervals.insert(offset, l);
                    offset += l;
                    length -= l;
                    if (!b->is_writing()) {
                        cache->_touch(b);
                    }
                    continue;
                }
                if (b->offset > offset) { // cache miss
                    uint32_t gap = b->offset - offset;
                    if (length <= gap) {
                        break;
                    }
                    offset += gap;
                    length -= gap;
                }
                if (!b->is_writing()) {
                    cache->_touch(b);
                }
                if (b->length > length) {
                    res[offset].substr_of(b->data, 0, length);
                    res_intervals.insert(offset, length);
                    break;
                } else {
                    res[offset].append(b->data);
                    res_intervals.insert(offset, b->length);
                    if (b->length == length)
                        break;
                    offset += b->length;
                    length -= b->length;
                }
            }
        }
    }
}

int KvsStoreTypes::BufferSpace::_discard(KvsStoreTypes::BufferCacheShard* cache, uint32_t offset, uint32_t length)
{
    // note: we already hold cache->lock
    ldout(cache->cct, 20) << __func__ << std::hex << " 0x" << offset << "~" << length
                          << std::dec << dendl;
    int cache_private = 0;
    cache->_audit("discard start");
    auto i = _data_lower_bound(offset);
    uint32_t end = offset + length;
    while (i != buffer_map.end()) {
        Buffer *b = i->second.get();
        if (b->offset >= end) {
            break;
        }
        if (b->cache_private > cache_private) {
            cache_private = b->cache_private;
        }
        if (b->offset < offset) {
            int64_t front = offset - b->offset;
            if (b->end() > end) {
                // drop middle (split)
                uint32_t tail = b->end() - end;
                if (b->data.length()) {
                    bufferlist bl;
                    bl.substr_of(b->data, b->length - tail, tail);
                    Buffer *nb = new Buffer(this, b->state, b->seq, end, bl);
                    nb->maybe_rebuild();
                    _add_buffer(cache, nb, 0, b);
                } else {
                    _add_buffer(cache, new Buffer(this, b->state, b->seq, end, tail),
                                0, b);
                }
                if (!b->is_writing()) {
                    cache->_adjust_size(b, front - (int64_t)b->length);
                }
                b->truncate(front);
                b->maybe_rebuild();
                cache->_audit("discard end 1");
                break;
            } else {
                // drop tail
                if (!b->is_writing()) {
                    cache->_adjust_size(b, front - (int64_t)b->length);
                }
                b->truncate(front);
                b->maybe_rebuild();
                ++i;
                continue;
            }
        }
        if (b->end() <= end) {
            // drop entire buffer
            _rm_buffer(cache, i++);
            continue;
        }
        // drop front
        uint32_t keep = b->end() - end;
        if (b->data.length()) {
            bufferlist bl;
            bl.substr_of(b->data, b->length - keep, keep);
            Buffer *nb = new Buffer(this, b->state, b->seq, end, bl);
            nb->maybe_rebuild();
            _add_buffer(cache, nb, 0, b);
        } else {
            _add_buffer(cache, new Buffer(this, b->state, b->seq, end, keep), 0, b);
        }
        _rm_buffer(cache, i);
        cache->_audit("discard end 2");
        break;
    }
    return cache_private;
}

void KvsStoreTypes::BufferSpace::_finish_write(KvsStoreTypes::BufferCacheShard* cache, uint64_t seq)
{
    auto i = writing.begin();
    while (i != writing.end()) {
        if (i->seq > seq) {
            break;
        }
        if (i->seq < seq) {
            ++i;
            continue;
        }

        Buffer *b = &*i;
        ceph_assert(b->is_writing());

        if (b->flags & Buffer::FLAG_NOCACHE) {
            writing.erase(i++);
            ldout(cache->cct, 20) << __func__ << " discard " << *b << dendl;
            buffer_map.erase(b->offset);
        } else {
            b->state = Buffer::STATE_CLEAN;
            writing.erase(i++);
            b->maybe_rebuild();
            b->data.reassign_to_mempool(mempool::mempool_bluestore_cache_data);
            cache->_add(b, 1, nullptr);
            ldout(cache->cct, 20) << __func__ << " added " << *b << dendl;
        }
    }
    cache->_trim();
    cache->_audit("finish_write end");
}

void KvsStoreTypes::BufferSpace::split(KvsStoreTypes::BufferCacheShard* cache, size_t pos, KvsStoreTypes::BufferSpace &r)
{
    std::lock_guard lk(cache->lock);
    if (buffer_map.empty())
        return;

    auto p = --buffer_map.end();
    while (true) {
        if (p->second->end() <= pos)
            break;

        if (p->second->offset < pos) {
            ldout(cache->cct, 30) << __func__ << " cut " << *p->second << dendl;
            size_t left = pos - p->second->offset;
            size_t right = p->second->length - left;
            if (p->second->data.length()) {
                bufferlist bl;
                bl.substr_of(p->second->data, left, right);
                r._add_buffer(cache, new Buffer(&r, p->second->state, p->second->seq, 0, bl),
                              0, p->second.get());
            } else {
                r._add_buffer(cache, new Buffer(&r, p->second->state, p->second->seq, 0, right),
                              0, p->second.get());
            }
            cache->_adjust_size(p->second.get(), -right);
            p->second->truncate(left);
            break;
        }

        ceph_assert(p->second->end() > pos);
        ldout(cache->cct, 30) << __func__ << " move " << *p->second << dendl;
        if (p->second->data.length()) {
            r._add_buffer(cache, new Buffer(&r, p->second->state, p->second->seq,
                                            p->second->offset - pos, p->second->data),
                          0, p->second.get());
        } else {
            r._add_buffer(cache, new Buffer(&r, p->second->state, p->second->seq,
                                            p->second->offset - pos, p->second->length),
                          0, p->second.get());
        }
        if (p == buffer_map.begin()) {
            _rm_buffer(cache, p);
            break;
        } else {
            _rm_buffer(cache, p--);
        }
    }
    ceph_assert(writing.empty());
    cache->_trim();
}
