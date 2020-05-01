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
#include "include/intarith.h"
#include "include/mempool.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/safe_io.h"
#include "common/Formatter.h"
#include "common/EventTrace.h"

#include "KvsStore.h"
#include "kvsstore_types.h"
#include "kvsstore_debug.h"
#include "kvsstore_db.h"
#include "kadi/kadi_types.h"

std::mutex debug_threadname_lock;
std::map<uint64_t, int> debug_threadnames;

// set up dout_context and dout_prefix here
// -----------------------------------------
#define dout_context cct
#define dout_subsys ceph_subsys_kvs

#undef dout_prefix
#define dout_prefix *_dout << "[kvs] "

#define SB_FLUSH_FREQUENCY 1024
#define OBJECT_MAX_SIZE 536870912

/// -----------------------------------------------------------------------------------------------
///  Constructor / Mount / Unmount
/// -----------------------------------------------------------------------------------------------

void KvsStore::_init_perf_logger(CephContext *cct) {
    FTRACE
    PerfCountersBuilder b(cct, "KvsStore", l_kvsstore_first, l_kvsstore_last);
    this->logger = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
}

KvsStore::KvsStore(CephContext *cct, const std::string &path) :
    ObjectStoreAdapter(cct, path), db(cct), finisher(cct, "kvs_commit_finisher", "kcfin"),
    kv_callback_thread(this),  kv_finalize_thread(this), kv_index_thread(this) {

    FTRACE
    // perf counter
    _init_perf_logger(cct);


    // create onode LRU cache
    set_cache_shards(1);
}

// configure onode and data caches
void KvsStore::set_cache_shards(unsigned num) {
    FTRACE
    // should be called one time
    if (onode_cache_shards.size() > 0 || buffer_cache_shards.size() > 0) return;

    onode_cache_shards.resize(num);
    buffer_cache_shards.resize(num);

    uint64_t max_shard_onodes = KVS_CACHE_MAX_ONODES / num;
    uint64_t max_shard_buffer = KVS_CACHE_MAX_DATA_SIZE / num;

    // KvsStore does not support a dynamic cache configuration
    // we set the max size of each cache here
    for (unsigned i = 0; i < num; ++i) {
        auto p = OnodeCacheShard::create(cct, "", logger);
        p->set_max(max_shard_onodes);
        onode_cache_shards[i] = p;
    }

    for (unsigned i = 0; i < num; ++i) {
        auto p = BufferCacheShard::create(cct, "", logger);
        p->set_max(max_shard_buffer);
        buffer_cache_shards[i] =p;
    }

    derr << "KvsStore Cache: max_shard_onodes: " << max_shard_onodes << " max_shard_buffer: " << max_shard_buffer << dendl;
}


KvsStore::~KvsStore() {
    FTRACE
    if (logger) {
        cct->get_perfcounters_collection()->remove(logger);
        delete logger;
    }

    assert(!mounted);

    { // remove caches
        for (auto i : onode_cache_shards) {
            delete i;
        }
        onode_cache_shards.clear();

        for (auto i : buffer_cache_shards) {
            delete i;
        }
        buffer_cache_shards.clear();
    }
}

/// =========================================================
/// ObjectStoreAdapter Implementation
/// =========================================================

// for mount & umount
int KvsStore::read_sb() {
    FTRACE
    bufferlist v;

    int ret = db.read_sb(v);

    if (v.length() == 0) {
        derr << "sb not found: ret = " << ret << dendl;
        return -1;
    } else {
        assert(ret == KV_SUCCESS);

        // not in the performance path
        bufferptr::const_iterator p = v.front().begin_deep();
        this->kvsb.decode(p);
    }
    return 0;
}

int KvsStore::write_sb() {
    FTRACE

    bufferlist bl;
    encode(this->kvsb, bl);

    derr << __func__ << " superblock bufferlist length = " << bl.length() << ", is up to date " << this->kvsb.is_uptodate << dendl;

    return db.write_sb(bl);
}

int KvsStore::open_db(bool create) {
    FTRACE

    kv_stop = false;

    if (cct->_conf->kvsstore_dev_path == "") {
        return -1;
    }

    finisher.start();

    if (this->db.open(cct->_conf->kvsstore_dev_path) != 0) {
        TR << "device is not opened :" << cct->_conf->kvsstore_dev_path;
        return -1;
    }
    {
        kv_iter_context ctx;
        for (int i = 0; i < 5; i++) {
            ctx.handle = (unsigned char)i;
            db.kadi.iter_close(&ctx, 0);
        }
    }

    kv_callback_thread.create("kvscallback");
    kv_index_thread.create("kvsindex");
    kv_finalize_thread.create("kvsfinalize");

    //if (!_check_db()) return -1;

    return 0;
}

bool KvsStore::_check_db() {
    int num_onodes = 10;
    IoContext ioc( 0);
    std::string prefix = "test_onode";
    std::vector<ghobject_t*> onodes;    onodes.reserve(num_onodes);
    std::vector<bufferlist*> bls;       bls.reserve(num_onodes);
    std::vector<bufferlist*> read_bls;  bls.reserve(num_onodes);

    for (int i =0 ;i < num_onodes; i++) {
        std::string name = prefix + std::to_string(i);
        ghobject_t *oid = new ghobject_t(hobject_t(sobject_t(name.c_str(), CEPH_NOSNAP)));
        bufferlist *bl   = new bufferlist();
        bl->append(name);
        db.aio_write_onode(*oid, *bl, &ioc, true);
        onodes.push_back(oid);
        bls.push_back(bl);
        read_bls.push_back(new bufferlist());
    }

    int r = db.syncio_submit_and_wait(&ioc);
    if (r != 0) {
        TR << "onode write failed r = " << r;
        ceph_abort_msg("onode write failed");
    }

    IoContext read_ioc(0);
    for (int i =0 ;i < num_onodes; i++) {
        db.aio_read_onode(*onodes[i], *read_bls[i], &read_ioc);
    }
    r = db.aio_submit_and_wait(&read_ioc);
    if (r != 0) {
        TR << "onode read failed: r = " << r;
        ceph_abort_msg("onode write failed");
    }

    db.compact();

    auto it = db.get_iterator(GROUP_PREFIX_ONODE);
    TR << "it valid = ? " << it->valid();

    while (true) {
        if (it->valid()) {
            kv_key key = it->key();
            TR << " key list " << print_kvssd_key(key.key, key.length) ;
            it->next();
        }
        else
            break;
    }


    return true;


}


void KvsStore::close_db() {
    FTRACE
    TR << "kv_finalize ";
    {
        std::unique_lock<std::mutex> l ( kv_finalize_lock );
        while (!kv_finalize_started) {
            kv_finalize_cond.wait(l);
        }
        kv_finalize_stop = true;
        kv_finalize_cond.notify_all();
    }

    kv_finalize_thread.join();

    TR << "kv_index_thread ";

    {
        std::unique_lock<std::mutex> l ( kv_lock );
        kv_index_stop = true;
    }

    kv_index_thread.join();

    TR << "finisher ";
    finisher.wait_for_empty();

    finisher.stop();

    TR << "kv_callback_thread ";
    {
        kv_stop = true;
        kv_callback_thread.join();
    }

    TR << "db_close ";
    this->db.close();
    kv_finalize_stop = false;
    kv_stop = false;
    kv_index_stop = false;
}


int KvsStore::mkfs_kvsstore() {
    FTRACE
    this->kvsb.is_uptodate = 1;

    return 0;
}

int KvsStore::mount_kvsstore() {
    FTRACE
    // load nid_last for atomic accesses
    this->nid_last = this->kvsb.nid_last;

    // to update superblock
    this->kvsb.is_uptodate = 0;

    return 0;
}

int KvsStore::umount_kvsstore() {
    FTRACE
    this->kvsb.is_uptodate = 1;
    this->kvsb.nid_last = this->nid_last;   // atomic -> local

    int r = write_sb();
    if (r < 0) {
        derr << __func__ << "err: could not write a superblock. retcode = " << r << dendl;
        ceph_abort();
    }

    dout(20) << __func__ << " stopping kv thread" << dendl;

    close_db();
    return 0;
}

int KvsStore::flush_cache_impl(bool collmap_clear) {
    FTRACE
    dout(10) << __func__ << dendl;
    for (auto i : onode_cache_shards) {
        i->flush();
    }
    for (auto i : buffer_cache_shards) {
        i->flush();
    }

    if (collmap_clear)
        coll_map.clear();
    return 0;
}


void KvsStore::osr_drain_all() {
    FTRACE
    dout(10) << __func__ << dendl;

    set<OpSequencerRef> s;
    vector<OpSequencerRef> zombies;
    {
        std::shared_lock l(coll_lock);
        for (auto &i : coll_map) {
            s.insert(i.second->osr);
        }
    }

    {
        std::lock_guard l(zombie_osr_lock);
        for (auto &i : zombie_osr_set) {
            s.insert(i.second);
            zombies.push_back(i.second);
        }
    }

    {
        std::lock_guard<std::mutex> l(kv_finalize_lock);
        kv_finalize_cond.notify_one();
    }

    for (auto osr : s) {
        dout(20) << __func__ << " drain " << osr << dendl;
        osr->drain();
    }

    {
        std::lock_guard l(zombie_osr_lock);
        for (auto &osr : zombies) {
            if (zombie_osr_set.erase(osr->cid)) {
                dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
                ceph_assert(osr->q.empty());
            } else if (osr->zombie) {
                dout(10) << __func__ << " empty zombie osr " << osr << " already reaped" << dendl;
                ceph_assert(osr->q.empty());
            } else {
                dout(10) << __func__ << " empty zombie osr " << osr << " resurrected" << dendl;
            }
        }
    }

    dout(10) << __func__ << " done" << dendl;

}

int KvsStore::fsck_impl() {
    return 0;
}

int KvsStore::fiemap_impl(CollectionHandle &c_, const ghobject_t &oid,
                          uint64_t offset, size_t len, map<uint64_t, uint64_t> &destmap) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    if (!c->exists)
        return -ENOENT;


    std::shared_lock l(c->lock);
    
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
        return -ENOENT;
    }

    if (offset > o->onode.size)
        goto out;

    if (offset + len > o->onode.size) {
        len = o->onode.size - offset;
    }

    dout(20) << __func__ << " " << offset << "~" << len << " size "
             << o->onode.size << dendl;

    destmap[0] = o->onode.size;

    out:
    dout(20) << __func__ << " " << offset << "~" << len << " size = 0 ("
             << destmap << ")" << dendl;
    
    return 0;
}

int KvsStore::open_collections() {
    FTRACE

    db.compact();

    KvsIterator *it = db.get_iterator(GROUP_PREFIX_COLL);
    for (it->begin(); it->valid(); it->next()) {
        coll_t cid;
        kv_key collkey = it->key();
        //TR << "returned key : " << print_kvssd_key(std::string((char *) collkey.key, collkey.length)) ;
        std::string name((char *) collkey.key + sizeof(kvs_coll_key), collkey.length - sizeof(kvs_coll_key));
        //TR << "found collection: " << name ;
        if (cid.parse(name)) {
            auto c = ceph::make_ref<Collection>(this,
                                                onode_cache_shards[cid.hash_to_shard(onode_cache_shards.size())],
                                                buffer_cache_shards[cid.hash_to_shard(
                                                        buffer_cache_shards.size())],
                                                cid);

            bufferlist bl;
            int r = db.read_kvkey(&collkey, bl, true);
            if (r != 0) {
                TR << "read failed, r= " << r;
            }

            auto p = bl.cbegin();
            try {
                decode(c->cnode, p);
            } catch (buffer::error &e) {
                TR << "decode error " << e.what();
                TR << " failed to decode cnode, key:"
                     << print_kvssd_key((char *) collkey.key, collkey.length) << ", length = " << bl.length() << ", hash = " << ceph_str_hash_linux(bl.c_str(), bl.length());
                //workaround for fw issue
                {
                    kvsstore_cnode_t cnode;
                    auto p = bl.cbegin();
                    try {
                        decode(cnode, p);
                    } catch (buffer::error &e) {
                        TR << "failed to decode cnode during write , key:"
                           << print_kvssd_key((char *) collkey.key, collkey.length);
                        ceph_assert(0 == "failed to decode cnode" );
                    }

                    TR << print_kvssd_key((char *) collkey.key, collkey.length) << ", passed decoding test";
                }

                if (it) delete it;
                return -EIO;
            }

            dout(20) << __func__ << " opened " << cid << " " << c << dendl;
            _osr_attach(c.get());
            coll_map[cid] = c;

        } else {
            TR << " unrecognized collection " << print_kvssd_key(it->key().key, it->key().length);
            derr << __func__ << " unrecognized collection " << print_kvssd_key(it->key().key, it->key().length)
                 << dendl;
            ceph_abort_msg("unrecognized collection");
        }
    }
    if (it) delete it;

    return 0;
}

void KvsStore::reap_collections() {
    FTRACE
    using ceph::decode;

    list<CollectionRef> removed_colls;
    {
        // _queue_reap_collection and this in the same thread.
        // So no need a lock.
        if (!removed_collections.empty())
            removed_colls.swap(removed_collections);
        else
            return;
    }

    list<CollectionRef>::iterator p = removed_colls.begin();
    while (p != removed_colls.end()) {
        CollectionRef c = *p;
        dout(10) << __func__ << " " << c << " " << c->cid << dendl;
        if (c->onode_map.map_any([&](OnodeRef o) {
            ceph_assert(!o->exists);
            if (o->flushing_count.load()) {
                dout(10) << __func__ << " " << c << " " << c->cid << " " << o->oid
                         << " flush_txns " << o->flushing_count << dendl;
                return true;
            }
            return false;
        })) {
            ++p;
            continue;
        }
        c->onode_map.clear();
        p = removed_colls.erase(p);
        dout(10) << __func__ << " " << c << " " << c->cid << " done" << dendl;
    }
    if (removed_colls.empty()) {
        dout(10) << __func__ << " all reaped" << dendl;
    } else {
        removed_collections.splice(removed_collections.begin(), removed_colls);
    }
}

KvsStoreTypes::CollectionRef KvsStore::get_collection(const coll_t &cid) {
    FTRACE
    //TRBACKTRACE;
    std::shared_lock l(coll_lock);
    ceph::unordered_map<coll_t, CollectionRef>::iterator cp = coll_map.find(cid);
    if (cp == coll_map.end()) {
        TR << "couldn't find collection " << cid;
        return CollectionRef();
    }

    TR << "found collection cid = " << cp->second->cid << ", requested cid = " << cid;

    return cp->second;
}


/// =========================================================
/// ObjectStore Implementation
/// =========================================================

ObjectStore::CollectionHandle KvsStore::create_new_collection(
        const coll_t &cid) {
    FTRACE

    std::unique_lock l{coll_lock};
    auto c = ceph::make_ref<Collection>(this,
                                        onode_cache_shards[cid.hash_to_shard(onode_cache_shards.size())],
                                        buffer_cache_shards[cid.hash_to_shard(buffer_cache_shards.size())],
                                        cid);
    new_coll_map[cid] = c;

    _osr_attach(c.get());

    return c;
}

int KvsStore::set_collection_opts(CollectionHandle &ch,
                                  const pool_opts_t &opts) {
    FTRACE
    dout(15) << __func__ << " " << ch->cid << " options " << opts << dendl;
    Collection *c = static_cast<Collection*>(ch.get());
    if (!c->exists)
        return -ENOENT;
    return 0;
}


int KvsStore::list_collections(vector<coll_t> &ls) {
    FTRACE
    std::shared_lock l(coll_lock);
    for (ceph::unordered_map<coll_t, CollectionRef>::iterator p =
            coll_map.begin(); p != coll_map.end(); ++p)
        ls.push_back(p->first);
    return 0;
}

bool KvsStore::collection_exists(const coll_t &c) {
    FTRACE
    std::shared_lock l(coll_lock);
    return coll_map.count(c);
}

int KvsStore::collection_list(CollectionHandle &c_, const ghobject_t &start,
                              const ghobject_t &end, int max, vector<ghobject_t> *ls,
                              ghobject_t *pnext) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    c->flush();

    dout(15) << __func__ << "-MAIN: " << c->cid << " bits " << c->cnode.bits
             << " start_oid " << start << " end_oid " << end
             << " max " << max << dendl;

    int r;
    {
        
        std::shared_lock l(c->lock);
        
        r = _collection_list(c, start, end, max, ls, pnext);
        //TR << "collection list result = " << ls->size() ;
    }
    

    dout(15) << __func__ << "-DONE: " << c->cid << " start " << start
             << " end " << end << " max " << max << " = " << r
             << ", ls.size() = " << ls->size() << ", next = "
             << (pnext ? *pnext : ghobject_t()) << dendl;
    return r;
}


int KvsStore::statfs(struct store_statfs_t *buf, osd_alert_list_t *alerts) {

    buf->reset();

    uint64_t bytesused, capacity;
    double utilization;

    this->db.get_freespace(bytesused, capacity, utilization);
    buf->total = capacity ;
    buf->available = capacity - bytesused;

    //TR << "total = " << buf->total << ", available " << buf->available ;
    return 0;
}

int KvsStore::stat(CollectionHandle &c_, const ghobject_t &oid, struct stat *st, bool allow_eio) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    if (!c->exists)
        return -ENOENT;
    dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;

    {
        
        std::shared_lock l(c->lock);
        
        OnodeRef o = c->get_onode(oid, false);
        if (!o || !o->exists)
            return -ENOENT;
        st->st_size = o->onode.size;
        st->st_blksize = 4096;
        st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
        st->st_nlink = 1;
    }
    
    return 0;
}

bool KvsStore::exists(CollectionHandle &c_, const ghobject_t &oid) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
    if (!c->exists)
        return false;

    bool r = true;

    {
        
        std::shared_lock l(c->lock);
        
        OnodeRef o = c->get_onode(oid, false);

        if (!o || !o->exists)
            r = false;
    }
    
    return r;
}

int KvsStore::read(CollectionHandle &c_, const ghobject_t &oid, uint64_t offset,
                   size_t length, bufferlist &bl, uint32_t op_flags) {
    FTRACE
    //TR << "read: oid " << oid << ", offset " << offset << ", length " << length ;

    Collection *c = static_cast<Collection*>(c_.get());

    // const coll_t &cid = c->get_cid();

    if (!c->exists)
        return -ENOENT;

    bl.clear();

    int ret = 0;
    {
        std::shared_lock l(c->lock);
        
        OnodeRef o = c->get_onode(oid, false);
        if (!o || !o->exists) {
            
            return -ENOENT;
        }

        if (offset == length && offset == 0) {
            length = o->onode.size;
        }

        //TR << "read : oid = " << oid << " offset " << offset << ", length " << length;
        ret = _do_read(c, o, offset, length, bl, op_flags);
        //TR << "read done : oid = " << oid << " offset " << offset << ", length " << length << ", hash = " << ceph_str_hash_linux(bl.c_str(), bl.length()) << ", result = " << ret ;

        
        return ret;
    }
}

int KvsStore::_do_read_chunks_async(OnodeRef &o, ready_regions_t &ready_regions, chunk2read_t &chunk2read, BufferCacheShard *cache) {
    IoContext ioc( NULL);
    FTRACE
    _prepare_read_chunk_ioc(o->oid, ready_regions, chunk2read, &ioc);

    int r = db.aio_submit_and_wait(&ioc);

    // update cache if needed

    if (KVS_CACHE_BUFFERED_READ && cache) {
        for (uint16_t &chunkid : chunk2read) {
            o->bc.did_read(cache, (chunkid << KVS_OBJECT_SPLIT_SHIFT) /* offset */, ready_regions[chunkid]);
        }
    }

    return r;
}

int KvsStore::_do_read(Collection *c,OnodeRef o,uint64_t offset,size_t length,bufferlist& bl,uint32_t op_flags, uint64_t retry_count) {
    int r = 0;
    bl.clear();

    if (offset >= o->onode.size) {
        return r;
    }
    if (offset + length > o->onode.size) {
        length = o->onode.size - offset;
    }

    ready_regions_t ready_regions;
    chunk2read_t chunk2read;

    _read_cache(c->cache, o, offset, length, 0, ready_regions, chunk2read);

    if (chunk2read.size()) {
        r = _do_read_chunks_async(o, ready_regions, chunk2read, c->cache);
        if (r != 0) return r;
    }

    _generate_read_result_bl(o, offset, length, ready_regions, bl);

    r = bl.length();

    return r;
}

int KvsStore::_prepare_read_chunk_ioc(const ghobject_t &oid, ready_regions_t& ready_regions,chunk2read_t& chunk2read, IoContext *ioc)
{
    FTRACE
    int r = 0;
    for (const uint16_t &chunkid : chunk2read) {
        bufferlist &bl = ready_regions[chunkid];
        db.aio_read_chunk(oid, chunkid, KVS_OBJECT_SPLIT_SIZE, bl, ioc);
    }
    return r;
}

int KvsStore::_generate_read_result_bl(OnodeRef o,uint64_t offset,size_t length, ready_regions_t& ready_regions, bufferlist& bl)
{
    auto pr = ready_regions.begin();
    auto pr_end = ready_regions.end();
    uint64_t pos = 0;
    while (pos < length) {
        if (pr != pr_end && pr->first == pos + offset) {
            pos += pr->second.length();
            bl.claim_append(pr->second);
            ++pr;
        } else {
            uint64_t l = length - pos;
            if (pr != pr_end) {
                l = pr->first - (pos + offset);
            }
            bl.append_zero(l);
            pos += l;
        }
    }
    ceph_assert(bl.length() == length);
    ceph_assert(pos == length);
    ceph_assert(pr == pr_end);
    return 0;
}

// bufferspace - contains buffers (linked list)
// buffercacheshard - per-collection lru data cache (history)
// cache works only with chunks (partial chunk will not be cached) unlike the bluestore's implementation

void KvsStore::_read_cache(BufferCacheShard *cache, OnodeRef o, uint64_t offset , size_t length, int read_cache_policy,
        ready_regions_t& ready_regions,chunk2read_t& chunk2read)
{
    FTRACE
    // find chunks in the cache

    interval_set<uint32_t> cache_interval;
    o->bc.read(cache, offset, length, ready_regions, cache_interval, read_cache_policy);

    // find chunks to read

    unsigned current_off = offset;
    unsigned remaining_bytes = length;

    auto pc = ready_regions.begin();
    while (remaining_bytes > 0) {
        unsigned l;
        if (pc != ready_regions.end() && pc->first == current_off) {  // continuous
            l = pc->second.length();
            //ready_regions[current_off].claim(pc->second);
            ++pc;
        } else {
            // cache miss

            l = remaining_bytes;
            if (pc != ready_regions.end()) {
                ceph_assert(pc->first > current_off);
                l = pc->first - current_off;                    // length of a gap
            }

            // assume that all cached data size is aligned with a chunk size except the last one.

            uint32_t num_chunks_to_read = ((l -1) / KVS_OBJECT_SPLIT_SIZE) + 1;

            uint32_t r_off = current_off;
            for (unsigned i = 0 ; i < num_chunks_to_read; i++) {
                chunk2read.push_back(get_chunk_index(r_off));
                r_off += KVS_OBJECT_SPLIT_SIZE;
            }
        }

        current_off += l;
        remaining_bytes -= l;
    }
}

/// -------------------------------------------------------------------------
/// Transaction
/// -------------------------------------------------------------------------

int KvsStore::queue_transactions(CollectionHandle &ch, vector<Transaction> &tls, TrackedOpRef op, ThreadPool::TPHandle *handle) {
    FTRACE
    list<Context *> on_applied, on_commit, on_applied_sync;
    ObjectStore::Transaction::collect_contexts(
            tls, &on_applied, &on_commit, &on_applied_sync);

    Collection *c = static_cast<Collection*>(ch.get());
    OpSequencer *osr = c->osr.get();


    // prepare
    TransContext *txc = _txc_create(static_cast<Collection*>(ch.get()), osr, &on_commit);


    for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
        txc->bytes += (*p).get_num_bytes();
        _txc_add_transaction(txc, &(*p));
    }

    // synchronously write metadata to protect the transaction
    int r;
    do { r = _txc_write_nodes(txc); } while (r != 0);

    _txc_state_proc(txc);

    // we're immediately readable (unlike FileStore)
    for (auto c : on_applied_sync) {
        c->complete(0);
    }
    if (!on_applied.empty()) {
        if (c->commit_queue) {
            c->commit_queue->queue(on_applied);
        } else {
            finisher.queue(on_applied);
        }
    }

    return 0;
}


bool KvsStore::_check_onode_validity(kvsstore_onode_t &ori_onode, bufferlist&bl) {
    FTRACE
    kvsstore_onode_t decoded_onode;
    auto p = bl.cbegin();
    try {
        decode(decoded_onode, p);

        if (!ori_onode.equals(decoded_onode)) {
            ceph_assert(0 == "decoded but the contents are not the same");
            return false;
        }

    } catch (buffer::error &e) {
        TR << "failed to decode";
        ceph_assert("failed_to_decode");
        return false;
    }
    TR << "successfully decoded";
    return true;
}

int KvsStore::_txc_write_nodes(TransContext *txc) {
    FTRACE
    dout(20) << __func__ << " txc " << txc << " onodes " << txc->onodes << dendl;

    // sync write onodes, omap values, superblocks
TR << "1";
    bufferlist sbbl;
    std::vector<bufferlist> bls;
    bls.reserve(txc->onodes.size());
    TR << "2";
    IoContext ioc(0);
    TR << "3";
    for (const OnodeRef& o : txc->onodes) {
        TR << "4";
        size_t bound = 0;
        denc(o->onode, bound);
        TR << "4-1";
        bls.emplace_back();
        bufferlist &bl = bls.back();
        TR << "4-2";
        {
            auto p = bl.get_contiguous_appender(bound, true);
            denc(o->onode, p);
        }
        TR << "4-3";

        //_check_onode_validity(o->onode, bl);

        TR << "write onode: data = " << ceph_str_hash_linux(bl.c_str(), bl.length()) << ", length = " << bl.length();
        db.aio_write_onode(o->oid, bl, &ioc, true);
        //o->flushing_count++;
        TR << "5 o->nref = " << o->nref.load();
    }
    TR << "6";

    // objects we modified but didn't affect the onode
    auto p = txc->modified_objects.begin();
    while (p != txc->modified_objects.end()) {
        if (txc->onodes.count(*p) == 0) {
            //(*p)->flushing_count++;
            TR << "7";
            ++p;
        } else {
            TR << "8";
            // remove dups with onodes list to avoid problems in _txc_finish
            p = txc->modified_objects.erase(p);
        }
    }
    TR << "9";

    if (this->nid_last > this->kvsb.nid_last + SB_FLUSH_FREQUENCY) {
        this->kvsb.nid_last = this->nid_last;
        encode(this->kvsb, sbbl);
        db.aio_write_sb(sbbl, &ioc);
    }
    TR << "10";

    TR << "issue sync IO ";

    int r =  db.syncio_submit_and_wait(&ioc);
    TR << "issue sync IO done";
    if (r == 0) {
        //_txc_finish_writes(txc);
        //_txc_apply_kv(txc);
    }
    return r;
}

KvsStore::TransContext* KvsStore::_txc_create(Collection *c, OpSequencer *osr, list<Context*> *on_commits) {
    FTRACE
    TransContext *txc = new TransContext(this, cct, c, osr, on_commits);
    osr->queue_new(txc);
    return txc;
}


void KvsStore::_txc_add_transaction(TransContext *txc, Transaction *t) {
    FTRACE
    Transaction::iterator i = t->begin();

    vector<CollectionRef> cvec(i.colls.size());

    unsigned j = 0;
    for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
         ++p, ++j) {
        cvec[j] = get_collection(*p);
    }

    TR << "collections are loaded";

    vector<OnodeRef> ovec(i.objects.size());

    for (int pos = 0; i.have_op(); ++pos) {
        Transaction::Op *op = i.decode_op();
        int r = 0;

        // no coll or obj
        if (op->op == Transaction::OP_NOP) {
            continue;
        }

        // collection operations
        CollectionRef &c = cvec[op->cid];

        TR << "Collection TR op= " << op->op << ", op cid = " << op->cid;


        switch (op->op) {
            case Transaction::OP_RMCOLL: {
                const coll_t &cid = i.get_cid(op->cid);
                r = _remove_collection(txc, cid, &c);
                if (!r)
                    continue;
            }
                break;

            case Transaction::OP_MKCOLL: {
                const coll_t &cid = i.get_cid(op->cid);
                r = _create_collection(txc, cid, op->split_bits, &c);
                if (!r) {
                    continue;
                }
            }
                break;

            case Transaction::OP_SPLIT_COLLECTION:
                assert(0 == "deprecated");
                break;

            case Transaction::OP_SPLIT_COLLECTION2: {
                uint32_t bits = op->split_bits;
                uint32_t rem = op->split_rem;

                r = _split_collection(txc, c, cvec[op->dest_cid], bits, rem);
                if (!r)
                    continue;
            }
                break;

            case Transaction::OP_MERGE_COLLECTION: {
                uint32_t bits = op->split_bits;
                r = _merge_collection(txc, &c, cvec[op->dest_cid], bits);
                if (!r)
                    continue;
            }
                break;
            case Transaction::OP_COLL_HINT: {
                uint32_t type = op->hint_type;
                bufferlist hint;
                i.decode_bl(hint);
                auto hiter = hint.cbegin();
                if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
                    uint32_t pg_num;
                    uint64_t num_objs;
                    decode(pg_num, hiter);
                    decode(num_objs, hiter);
                    dout(10) << __func__ << " collection hint objects is a no-op, "
                             << " pg_num " << pg_num << " num_objects " << num_objs << dendl;
                } else {
                    // Ignore the hint
                    dout(10) << __func__ << " unknown collection hint " << type
                             << dendl;
                }
                continue;
            }
                break;

            case Transaction::OP_COLL_SETATTR:
                r = -EOPNOTSUPP;
                break;

            case Transaction::OP_COLL_RMATTR:
                r = -EOPNOTSUPP;
                break;

            case Transaction::OP_COLL_RENAME:
                assert(0 == "not implemented");
                break;
        }

        if (r < 0) {
            TRERR << __func__ << " 1. error " << cpp_strerror(r) << " not handled on operation " << op->op
                     << " (op " << pos << ", counting from 0)";
            derr << __func__ << " 1. error " << cpp_strerror(r) << " not handled on operation " << op->op
                 << " (op " << pos << ", counting from 0)"
                 << dendl;
            assert(0 == "unexpected error");
        }

        // these operations implicity create the object
        bool create = false;
        if (op->op == Transaction::OP_TOUCH
            || op->op == Transaction::OP_CREATE
            || op->op == Transaction::OP_WRITE
            || op->op == Transaction::OP_ZERO) {
            create = true;
        }

        TR << "Object TR op= " << op->op << ", create = " << create;

        // object operations
        std::unique_lock l(c->lock);

        TR << "collection lock is acquired";

        OnodeRef &o = ovec[op->oid];
        if (!o) {
            ghobject_t oid = i.get_oid(op->oid);
            TR << "loading onode " << oid;
            o = c->get_onode(oid, create, op->op == Transaction::OP_CREATE);
        }
        TR << "loading onode " << op->oid << "done";

        if (!create && (!o || !o->exists)) {
            TRERR << "not a create nor exist";
            dout(10) << __func__ << " op " << op->op << " got ENOENT on "
                     << i.get_oid(op->oid) << dendl;
            TR << __func__ << " op " << op->op << " got ENOENT on "
                  << i.get_oid(op->oid);
            r = -ENOENT;
            goto endop;
        }

        TR << "processing op";

        switch (op->op) {
            case Transaction::OP_CREATE:
            case Transaction::OP_TOUCH:
                r = _touch(txc, c, o);
                break;

            case Transaction::OP_WRITE:
                {
                    uint64_t off = op->off;
                    uint64_t len = op->len;
                    uint32_t fadvise_flags = i.get_fadvise_flags();
                    bufferlist bl;
                    i.decode_bl(bl);
                    r = _write(txc, c, o, off, len, bl, fadvise_flags);
                }
                break;

            case Transaction::OP_ZERO:
                {
                    uint64_t off = op->off;
                    uint64_t len = op->len;
                    r = _zero(txc, c, o, off, len);
                }
                break;

            case Transaction::OP_TRIMCACHE: {
                // deprecated, no-op
                }
                break;

            case Transaction::OP_TRUNCATE: {
                uint64_t off = op->off;
                r = _truncate(txc, c, o, off);
            }
                break;

            case Transaction::OP_REMOVE: {
                r = _remove(txc, c, o);
            }
                break;

            case Transaction::OP_SETATTR: {
                string name = i.decode_string();
                bufferptr bp;
                i.decode_bp(bp);
                r = _setattr(txc, c, o, name, bp);
            }
                break;

            case Transaction::OP_SETATTRS: {
                map<string, bufferptr> aset;
                i.decode_attrset(aset);
                r = _setattrs(txc, c, o, aset);
            }
                break;

            case Transaction::OP_RMATTR: {
                string name = i.decode_string();
                r = _rmattr(txc, c, o, name);
            }
                break;

            case Transaction::OP_RMATTRS: {
                r = _rmattrs(txc, c, o);
            }
                break;

            case Transaction::OP_CLONE: {
                OnodeRef &no = ovec[op->dest_oid];
                if (!no) {
                    const ghobject_t &noid = i.get_oid(op->dest_oid);
                    no = c->get_onode(noid, true);
                }
                r = _clone(txc, c, o, no);
            }
                break;

            case Transaction::OP_CLONERANGE:
                assert(0 == "deprecated");
                break;

            case Transaction::OP_CLONERANGE2: {
                OnodeRef &no = ovec[op->dest_oid];
                if (!no) {
                    const ghobject_t &noid = i.get_oid(op->dest_oid);
                    no = c->get_onode(noid, true);
                }
                uint64_t srcoff = op->off;
                uint64_t len = op->len;
                uint64_t dstoff = op->dest_off;
                r = _clone_range(txc, c, o, no, srcoff, len, dstoff);
            }
                break;

            case Transaction::OP_COLL_ADD:
                ceph_abort_msg("not implemented");
                break;

            case Transaction::OP_COLL_REMOVE:
                ceph_abort_msg("not implemented");
                break;

            case Transaction::OP_COLL_MOVE:
                ceph_abort_msg("deprecated");
                break;

            case Transaction::OP_COLL_MOVE_RENAME:
            case Transaction::OP_TRY_RENAME: {
                assert(op->cid == op->dest_cid);
                const ghobject_t &noid = i.get_oid(op->dest_oid);
                OnodeRef &no = ovec[op->dest_oid];
                if (!no) {
                    no = c->get_onode(noid, true);
                }
                r = _rename(txc, c, o, no, noid);
            }
                break;

            case Transaction::OP_OMAP_CLEAR: {

                r = _omap_clear(txc, c, o);
            }
                break;
            case Transaction::OP_OMAP_SETKEYS: {
                bufferlist aset_bl;
                i.decode_attrset_bl(&aset_bl);
                r = _omap_setkeys(txc, c, o, aset_bl);
            }
                break;
            case Transaction::OP_OMAP_RMKEYS: {
                bufferlist keys_bl;
                i.decode_keyset_bl(&keys_bl);
                r = _omap_rmkeys(txc, c, o, keys_bl);
            }
                break;
            case Transaction::OP_OMAP_RMKEYRANGE: {
                string first, last;
                first = i.decode_string();
                last = i.decode_string();
                r = _omap_rmkey_range(txc, c, o, first, last);
            }
                break;
            case Transaction::OP_OMAP_SETHEADER: {
                bufferlist bl;
                i.decode_bl(bl);
                r = _omap_setheader(txc, c, o, bl);
            }
                break;

            case Transaction::OP_SETALLOCHINT: {
                r = 0; // alloc hint not needed
            }
                break;

            default:
                derr << __func__ << "bad op " << op->op << dendl;
                ceph_abort();
        }
        endop:
        if (r < 0) {
            bool ok = false;

            if (r == -ENOENT
                && !(op->op == Transaction::OP_CLONERANGE
                     || op->op == Transaction::OP_CLONE
                     || op->op == Transaction::OP_CLONERANGE2
                     || op->op == Transaction::OP_COLL_ADD
                     || op->op == Transaction::OP_SETATTR
                     || op->op == Transaction::OP_SETATTRS
                     || op->op == Transaction::OP_RMATTR
                     || op->op == Transaction::OP_OMAP_SETKEYS
                     || op->op == Transaction::OP_OMAP_RMKEYS
                     || op->op == Transaction::OP_OMAP_RMKEYRANGE
                     || op->op == Transaction::OP_OMAP_SETHEADER))
                // -ENOENT is usually okay
                ok = true;
            if (r == -ENODATA)
                ok = true;

            /*if (r == -E2BIG
                && (op->op == Transaction::OP_WRITE
                    || op->op == Transaction::OP_TRUNCATE
                    || op->op == Transaction::OP_ZERO))
                ok = true;*/

            if (!ok) {
                const char *msg = "unexpected error code";

                if (r == -ENOENT
                    && (op->op == Transaction::OP_CLONERANGE
                        || op->op == Transaction::OP_CLONE
                        || op->op == Transaction::OP_CLONERANGE2))
                    msg = "ENOENT on clone suggests osd bug";

                if (r == -ENOSPC)
                    // For now, if we hit _any_ ENOSPC, crash, before we do any damage
                    // by partially applying transactions.
                    msg = "ENOSPC from bluestore, misconfigured cluster";

                if (r == -ENOTEMPTY) {
                    msg = "ENOTEMPTY suggests garbage data in osd data dir";
                }

                derr << __func__ << " 2. error: code = " << r << "("
                     << cpp_strerror(r) << ") not handled on operation " << op->op
                     << " (op " << pos << ", counting from 0)"
                     << dendl;
                derr << msg << dendl;
                ceph_abort_msg("unexpected error");
            }
        }
    }
}

void KvsStore::_assign_nid(TransContext *txc, OnodeRef o)
{
    FTRACE
    if (o->onode.nid) {
        ceph_assert(o->exists);
        return;
    }
    uint64_t nid = ++nid_last;
    dout(20) << __func__ << " " << nid << dendl;
    o->onode.nid = nid;
    txc->last_nid = nid;
    o->exists = true;
}


///--------------------------------------------------------
/// Write Functions
///--------------------------------------------------------

int KvsStore::_touch(TransContext *txc, CollectionRef &c, OnodeRef &o) {
    FTRACE
    _assign_nid(txc, o);
    txc->write_onode(o);
    return 0;
}

int KvsStore::_write(TransContext *txc, CollectionRef &c, OnodeRef &o,
                     uint64_t offset, size_t length, bufferlist &bl, uint32_t fadvise_flags) {
    FTRACE
    int r = 0;
    if (offset + length >= OBJECT_MAX_SIZE) {
        r = -E2BIG;
    } else {
        _assign_nid(txc, o);
        bufferlist::iterator p = bl.begin();
        r = _do_write(txc, c, o, offset, length, &p);
        txc->write_onode(o);
    }

    return r;
}


int KvsStore::_do_write_read_chunks_if_needed(CollectionRef& c, OnodeRef &o, const uint64_t object_length,
        uint64_t offset, uint64_t length, ready_regions_t &readyregions, zero_regions_t &zeroregions, const uint64_t chunksize)
{
    FTRACE
    uint64_t e = offset + length;
    int32_t head_off = -1;
    int32_t tail_off = -1;
    chunk2read_t chunk2read;
    std::set<uint16_t> uchunk2read;

    // find all the chunks to read

    if (object_length > e) {
        tail_off = p2align(e, chunksize) ;
    }

    if (offset > 0) {
        if (object_length >= offset && object_length != 0) {
            head_off = p2align(offset, chunksize);
            if (tail_off == head_off) tail_off = -1;
        }

        if (object_length < offset) {
            // pad from length to offset
            uint64_t pad_soff = object_length;

            uint64_t remaining_bytes = offset - pad_soff;
            while (remaining_bytes > 0) {
                uint64_t chunk_soff = p2align(pad_soff, chunksize);
                uint64_t pad_eoff = std::min(chunk_soff + chunksize, offset);
                uint64_t pad_len  = pad_eoff - pad_soff;

                if (chunk_soff < object_length) {
                    //std::cout << "issue zero read: chunk off " << chunk_soff  << std::endl;
                    uchunk2read.insert(chunk_soff / chunksize);
                }

                zeroregions.push_back(std::make_pair(pad_soff, pad_len));

                pad_soff        += pad_len;
                remaining_bytes -= pad_len;
            }
        }
    }

    if (head_off != -1)
        uchunk2read.insert(head_off / chunksize);
    if (tail_off != -1)
        uchunk2read.insert(tail_off / chunksize);

    chunk2read.assign(uchunk2read.begin(), uchunk2read.end());

    // read the chunks asynchronously

    return _do_read_chunks_async(o, readyregions, chunk2read, 0);  // do not update the cache yet
}


void KvsStore::_do_write_pad_zeros(ready_regions_t &readyregions, zero_regions_t &zeroregions, const uint64_t chunksize) {
    FTRACE
    for (const auto &p : zeroregions) {
        const uint64_t c_off = p2align(p.first, chunksize);
        const uint64_t p_off = p2phase(p.first, chunksize);

        bufferlist &bl = readyregions[c_off];
        if (bl.length() == 0) {
            bufferptr p = buffer::create_small_page_aligned(chunksize);
            bl.append(std::move(p));
        }

        bl.zero(p_off, p.second);
        //std::cout << " zero: chunk offset " << c_off << " start offset " << p_off << " len " << p.second << " " << print_chunk(c_off, readyregions, chunksize)<< std::endl;
    }
}



int KvsStore::_do_write(TransContext *txc, CollectionRef& c, OnodeRef &o,
        uint64_t offset, uint64_t length, bufferlist::iterator* blp)
{
    if (length == 0) {
        return 0;
    }

    zero_regions_t zero_regions;
    ready_regions_t ready_regions;

    const uint64_t object_length = o->onode.size;
    const uint64_t e = offset + length;
    const uint64_t chunksize = KVS_OBJECT_SPLIT_SIZE;


    TR << "do_write: " << o->oid << ", obj len " << object_length << " off " << offset << " len " << length;

    int r = _do_write_read_chunks_if_needed(c, o, object_length, offset, length, ready_regions, zero_regions, chunksize);
    TR << "do_write: "  << o->oid << " 1 r= " << r;
    if (r != 0) return r;

    TR << "do_write: " << o->oid  << " 2";
    _do_write_pad_zeros( ready_regions, zero_regions, chunksize);

    TR << "do_write: " << o->oid  << " 3";

    const uint64_t start_c_off = p2align(offset, chunksize);
    const uint64_t end_c_off   = p2align(e     , chunksize);

    uint64_t bl_offset  = 0;

    uint64_t sum = 0;
    uint64_t nc = 0;

    for (uint32_t c_off = start_c_off; c_off <= end_c_off && length > 0; c_off += chunksize) {
        uint64_t b_off     = offset - c_off;
        uint64_t b_remains = chunksize - b_off;
        uint64_t to_write  = std::min(b_remains, length);

        TR << "send write : " << o->oid  << " 4 chunk offset " << c_off << " start offset " << b_off << " len " << to_write  ;

        bufferlist &bl = ready_regions[c_off];
        if (bl.length() == 0) {
            bufferptr p = buffer::create_small_page_aligned(chunksize);
            bl.append(std::move(p));
        }

        if (blp) {
            bufferlist t;
            blp->copy(to_write, t);
            bl.copy_in(b_off, to_write, t);
        } else {
            bl.zero(b_off, to_write);
        }

        // buffer cache write


        o->bc.write(c->cache, txc->seq, offset, bl, 0);

        length    -= to_write;
        offset    += to_write;
        bl_offset += to_write;
        nc++;
        sum += to_write;
    }


    // send write to KVSSD
    void *buf_addr;
    uint64_t buf_len;
    uint32_t to_write;

    unsigned i =0;
    uint32_t c_off = start_c_off;
    uint16_t chunkid = p2align(start_c_off, chunksize);
    TR << "send write : " << o->oid  << " 5 start chunk id = ? " << chunkid << ", nc = ? " << nc;
    while (sum > 0) {
        o->bc.get_buffer_address(c->cache, c_off, &buf_addr, &buf_len);
        to_write = std::min(sum, buf_len);

        TR << "send write : " << o->oid  << " 6 chunk id = ? " << chunkid+ i << ", buf length" << to_write;
        db.aio_write_chunk(o->oid, chunkid, buf_addr, to_write, &txc->ioc);

        sum   -= to_write;
        c_off += to_write;
        chunkid++;
    }

    TR << "written = " << bl_offset << "bytes, end offset = " << offset ;

    if (offset > o->onode.size) {
        o->onode.size = offset;
    }

    return 0;
}


int KvsStore::_zero(TransContext *txc, CollectionRef &c, OnodeRef &o,
                    uint64_t offset, size_t length) {
    FTRACE
    dout(15) << __func__ << " " << c->cid << " " << o->oid << " 0x"
             << std::hex << offset << "~" << length << std::dec
             << dendl;

    int r = _do_write(txc, c, o, offset, length, 0);
    dout(10) << __func__ << " " << c->cid << " " << o->oid << " 0x"
             << std::hex << offset << "~" << length << std::dec
             << " = " << r << dendl;
    return r;
}

int KvsStore::_truncate(TransContext *txc, CollectionRef &c, OnodeRef &o,
                        uint64_t offset) {
    FTRACE

    int r = 0;
    if (offset >= OBJECT_MAX_SIZE) {
        r = -E2BIG;
    } else {
        _do_truncate(txc, c, o, offset);
    }
    return r;
}

void KvsStore::_do_truncate(TransContext *txc, CollectionRef &c, OnodeRef o,
                           uint64_t offset) {
    FTRACE
    dout(15) << __func__  << " " << o->oid << " 0x"
             << std::hex << offset << std::dec << dendl;

    const uint64_t chunksize = KVS_OBJECT_SPLIT_SIZE;

    if (offset == o->onode.size)
        return;

    if (offset < o->onode.size) {
        uint64_t start_c_off = p2align(offset, chunksize);
        uint64_t end_c_off   = p2align(o->onode.size, chunksize);

        if (start_c_off != offset) {
            start_c_off++;
        }

        uint16_t chunkid = p2align(start_c_off, chunksize);
        for (uint32_t c_off = start_c_off; c_off <= end_c_off; c_off += chunksize, ++chunkid) {
            // remove from a cache
            o->bc.discard(c->cache, c_off, chunksize);
            db.aio_remove_chunk(o->oid, chunkid, &txc->ioc);
        }
    }

    o->onode.size = offset;
    txc->write_onode(o);

    dout(10) << __func__ << " truncate size to " << offset << dendl;
}

int KvsStore::_remove(TransContext *txc, CollectionRef &c, OnodeRef &o) {
    FTRACE
    dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
    int r = _do_remove(txc, c, o);
    dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r
             << dendl;
    return r;
}

int KvsStore::_do_remove(TransContext *txc, CollectionRef &c, OnodeRef o) {
    FTRACE
    if (!o->exists)
        return 0;

    _do_truncate(txc, c, o, 0);

    o->onode.size = 0;
    if (o->onode.has_omap()) {
        o->flush();
        _do_omap_clear(txc, o);
    }

    o->exists = false;
    o->onode = kvsstore_onode_t();
    txc->note_removed_object(o);

    db.aio_remove_onode(o->oid, &txc->ioc);

    // cached onode will be removed by LRU cache

    return 0;
}


int KvsStore::_clone(TransContext *txc, CollectionRef &c, OnodeRef &oldo, OnodeRef &newo) {

    dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
             << newo->oid << dendl;
    int r = 0;
    if (oldo->oid.hobj.get_hash() != newo->oid.hobj.get_hash()) {
        derr << __func__ << " mismatched hash on " << oldo->oid
             << " and " << newo->oid << dendl;
        return -EINVAL;
    }

    _assign_nid(txc, newo);

    oldo->flush();

    _do_truncate(txc, c, newo, 0);

    bufferlist bl;

    r = _do_read(c.get(), oldo, 0, oldo->onode.size, bl, 0);
    if (r < 0) return r;

    bufferlist::iterator blp = bl.begin();

    r = _do_write(txc, c, newo, 0, oldo->onode.size, &blp);
    if (r < 0) return r;

    newo->onode.attrs = oldo->onode.attrs;

    // clear newo's omap
    if (newo->onode.has_omap()) {
        dout(20) << __func__ << " clearing old omap data" << dendl;
        newo->flush();
        _do_omap_clear(txc, newo);
    }

    // clone attrs, omap , omap header
    newo->onode.omaps = oldo->onode.omaps;
    newo->onode.omap_header = oldo->onode.omap_header;

    // copy omap data
    {
        txc->omap_data.reserve(newo->onode.omaps.size());
        {
            IoContext ioc(0);
            int i = 0;
            for (const std::string &name : newo->onode.omaps) {
                txc->omap_data.emplace_back();
                bufferlist &bl = txc->omap_data.back();
                db.aio_read_omap(oldo->onode.nid, name, bl, &ioc);
                i++;
            }
            int r = db.aio_submit_and_wait(&ioc);
            if (r != 0) {
                derr << "omap_get_values failed: ret = " << r << dendl;
                return -EIO;
            }
        }
        {
            int i = 0;
            for (const std::string &name : newo->onode.omaps) {
                txc->omap_data.emplace_back();
                bufferlist &bl = txc->omap_data.back();
                db.aio_write_omap(newo->onode.nid, name, bl, &txc->ioc, true);
                i++;
            }
        }
    }

    txc->write_onode(newo);

    return 0;
}


int KvsStore::_clone_range(TransContext *txc, CollectionRef &c,
                           OnodeRef &oldo, OnodeRef &newo, uint64_t srcoff, uint64_t length,
                           uint64_t dstoff) {
    FTRACE
    dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
             << newo->oid << " from " << srcoff << "~" << length
             << " to offset " << dstoff << dendl;
    int r = 0;

    if (srcoff + length >= OBJECT_MAX_SIZE ||
        dstoff + length >= OBJECT_MAX_SIZE) {
        return -E2BIG;
    }
    if (srcoff + length > oldo->onode.size) {
        return -EINVAL;
    }

    _assign_nid(txc, newo);

    if (length > 0) {
        bufferlist bl;

        r = _do_read(c.get(), oldo, srcoff, length, bl, 0);
        if (r < 0) return r;

        bufferlist::iterator blp = bl.begin();

        r = _do_write(txc, c, newo, dstoff, bl.length(), &blp);
        if (r < 0) return r;
    }

    txc->write_onode(newo);

    r = 0;

    return r;
}


int KvsStore::_rename(TransContext *txc, CollectionRef &c, OnodeRef &oldo,
                      OnodeRef &newo, const ghobject_t &new_oid) {
    FTRACE
    dout(10) << __func__ << " cid = " << c->cid << ", old->oid =  "
             << oldo->oid << " -> " << ", new->oid =  " << newo->oid
             << dendl;

    int r = 0;

    ghobject_t old_oid = oldo->oid;
    bufferlist bl;
    string old_key, new_key;

    if (newo) {
        if (newo->exists) {
            r = -EEXIST;
            if (r < 0) {
                derr << __func__ << " New Object " << new_oid
                     << " exists" << " r = " << r << dendl;
                goto release;
            }
        }
    }

    // copy old object to new object
    r = this->_clone(txc, c, oldo, newo);
    if (r < 0) {
        derr << __func__ << " clone failed" << r << dendl;
        return r;
    }

    txc->note_modified_object(oldo);

    r = _do_remove(txc, c, oldo);
    if (r < 0) {
        derr << __func__ << " _do_remove old object = " << oldo->oid
             << ", r = " << r << dendl;
        goto release;
    }

    r = 0;

release:

    return r;
}

/// ------------------------------------------------------------------------------------------------
/// Attributes
/// ------------------------------------------------------------------------------------------------

int KvsStore::_setattr(TransContext *txc, CollectionRef &c, OnodeRef &o,
                       const string &name, bufferptr &val) {
    FTRACE
    dout(15) << __func__ << " " << c->cid << " " << o->oid << " " << name
             << " (" << val.length() << " bytes)" << dendl;
    int r = 0;

    if (val.is_partial()) {
        auto &b = o->onode.attrs[name.c_str()] = bufferptr(val.c_str(),
                                                           val.length());
        b.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
    } else {
        auto &b = o->onode.attrs[name.c_str()] = val;
        b.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
    }
    txc->write_onode(o);
    dout(10) << __func__ << " " << c->cid << " " << o->oid << " " << name
             << " (" << val.length() << " bytes)" << " = " << r
             << dendl;
    return r;
}

int KvsStore::_setattrs(TransContext *txc, CollectionRef &c, OnodeRef &o,
                        const map<string, bufferptr> &aset) {
    FTRACE
    dout(15) << __func__ << " " << c->cid << " " << o->oid << " "
             << aset.size() << " keys" << dendl;
    int r = 0;

    for (map<string, bufferptr>::const_iterator p = aset.begin();
         p != aset.end(); ++p) {

        if (p->second.is_partial()) {
            auto &b = o->onode.attrs[p->first.c_str()] = bufferptr(
                    p->second.c_str(), p->second.length());
            b.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
        } else {
            auto &b = o->onode.attrs[p->first.c_str()] = p->second;
            b.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
        }
    }
    txc->write_onode(o);
    dout(10) << __func__ << " " << c->cid << " " << o->oid << " "
             << aset.size() << " keys" << " = " << r << dendl;
    return r;
}

int KvsStore::_rmattr(TransContext *txc, CollectionRef &c, OnodeRef &o,
                      const string &name) {
    FTRACE
    dout(15) << __func__ << " " << c->cid << " " << o->oid << " " << name
             << dendl;
    int r = 0;
    auto it = o->onode.attrs.find(name.c_str());
    if (it == o->onode.attrs.end())
        goto out;

    o->onode.attrs.erase(it);
    txc->write_onode(o);

    out:
    dout(10) << __func__ << " " << c->cid << " " << o->oid << " " << name
             << " = " << r << dendl;
    return r;
}

int KvsStore::_rmattrs(TransContext *txc, CollectionRef &c, OnodeRef &o) {
    FTRACE
    dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
    int r = 0;

    if (o->onode.attrs.empty())
        goto out;

    o->onode.attrs.clear();
    txc->write_onode(o);

    out:
    dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r
             << dendl;
    return r;
}

int KvsStore::getattr(CollectionHandle &c_, const ghobject_t &oid,
                      const char *name, bufferptr &value) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    dout(15) << __func__ << " " << c->cid << " " << oid << " " << name
             << dendl;
    if (!c->exists)
        return -ENOENT;

    int r;
    {
        std::shared_lock l(c->lock);
        
        mempool::kvsstore_cache_other::string k(name);

        OnodeRef o = c->get_onode(oid, false);
        if (!o || !o->exists) {
            TR << "not exist, returning " << r;
            TRBACKTRACE;
            r = -ENOENT;
            goto out;
        }

        if (!o->onode.attrs.count(k)) {
            r = -ENODATA;
            goto out;
        }
        value = o->onode.attrs[k];

        r = 0;
    }
out:
    TR << "returning " << r;

    return r;
}

int KvsStore::getattrs(CollectionHandle &c_, const ghobject_t &oid,
                       map<string, bufferptr> &aset) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
    if (!c->exists)
        return -ENOENT;

    int r;
    {
        
        std::shared_lock l(c->lock);

        OnodeRef o = c->get_onode(oid, false);
        if (!o || !o->exists) {
            r = -ENOENT;
            goto out;
        }
        for (auto &i : o->onode.attrs) {
            aset.emplace(i.first.c_str(), i.second);
        }
        r = 0;
    }

    out:
    
    return r;
}

/// ------------------------------------------------------------------------------------------------
/// OMAP
/// ------------------------------------------------------------------------------------------------

void KvsStore::_do_omap_clear(TransContext *txc, OnodeRef &o) {
    FTRACE
    for (const std::string &user_key :o->onode.omaps) {
        db.aio_remove_omap(o->onode.nid, user_key, &txc->ioc);
    }

    o->onode.omaps.clear();
    o->onode.omap_header.clear();
}

int KvsStore::_omap_clear(TransContext *txc, CollectionRef &c, OnodeRef &o) {
    FTRACE
    dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
    int r = 0;
    if (o->onode.has_omap()) {
        o->flush();
        _do_omap_clear(txc, o);
        //TODO: check if we need this: o->onode.clear_omap_flag();
        txc->write_onode(o);
    }
    dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r
             << dendl;
    return r;
}

int KvsStore::_omap_setkeys(TransContext *txc, CollectionRef &c,
                            OnodeRef &o, bufferlist &bl) {
    FTRACE
    auto p = bl.cbegin();

    // if not found, bluestore creates omap prefix and tail but it is not necessary
    __u32 num;
    decode(num, p);
    if (num <= 0) return 0;
    txc->write_onode(o);
    while (num > 0) {
        string key;
        txc->omap_data.emplace_back();

        bufferlist &list = txc->omap_data.back();
        decode(key, p);
        decode(list, p);
        // key: stored in onode
        o->onode.omaps.insert(key);

        if (list.length() > 0)
            db.aio_write_omap(o->onode.nid, key, list, &txc->ioc, true);
        // value

        num--;
    }
    return 0;
}

int KvsStore::_omap_setheader(TransContext *txc, CollectionRef &c,
                              OnodeRef &o, bufferlist &bl) {
    FTRACE
    o->onode.omap_header = bl;
    txc->write_onode(o);

    return 0;
}

int KvsStore::_omap_rmkeys(TransContext *txc, CollectionRef &c, OnodeRef &o,
                           bufferlist &bl) {
    FTRACE
    if (!o->onode.has_omap()) {
        return 0;
    }

    auto p = bl.cbegin();
    __u32 num;
    decode(num, p);

    if (num <= 0) return 0;

    txc->write_onode(o);

    while (num--) {
        string key;
        decode(key, p);

        o->onode.omaps.erase(key);
        db.aio_remove_omap(o->onode.nid, key, &txc->ioc);
    }

    return 0;
}

int KvsStore::_omap_rmkey_range(TransContext *txc, CollectionRef &c,
                                OnodeRef &o, const string &first, const string &last) {
    FTRACE
    if (!o->onode.has_omap())
        return 0;

    set<string>::iterator p = o->onode.omaps.lower_bound(first);
    set<string>::iterator e = o->onode.omaps.lower_bound(last);

    for (auto it = p; it != e; it++) {
        const string &key = *it;
        db.aio_remove_omap(o->onode.nid, key, &txc->ioc);
    }

    o->onode.omaps.erase(p, e);

    return 0;
}



/// Get omap header
int KvsStore::omap_get_header(CollectionHandle &c_, const ghobject_t &oid,
                              bufferlist *header, bool allow_eio) {
    FTRACE

    Collection *c = static_cast<Collection*>(c_.get());
    if (!c->exists)
        return -ENOENT;

    
    std::shared_lock l(c->lock);
    
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
        return -ENOENT;
    }
    if (!o->onode.has_omap()) return 0;

    o->flush();

    *header = o->onode.omap_header;
    //header->append(o->onode.omap_header);
    
    return 0;
}


/// Get keys defined on oid
int KvsStore::omap_get_keys(CollectionHandle &c_, const ghobject_t &oid,
                            set<string> *keys) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    if (!c->exists)
        return -ENOENT;

    
    std::shared_lock l(c->lock);
    
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
        
        return -ENOENT;
    }

    if (!o->onode.has_omap()) {
        
        return 0;
    }

    o->flush();

    for (const auto &p: o->onode.omaps) {
        keys->insert(p);
    }
    
    return 0;
}

/// Filters keys into out which are defined on oid
int KvsStore::omap_check_keys(CollectionHandle &c_, const ghobject_t &oid,
                              const set<string> &keys, set<string> *out) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    if (!c->exists)
        return -ENOENT;

    
    std::shared_lock l(c->lock);
    
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
        
        return -ENOENT;
    }

    if (!o->onode.has_omap()) {
        
        return 0;
    }

    for (const std::string &p : keys) {
        auto it = o->onode.omaps.find(p);
        if (it != o->onode.omaps.end()) {
            out->insert(p);
        }
    }
    
    return 0;
}

/// Get key values
int KvsStore::omap_get_values(CollectionHandle &c_, const ghobject_t &oid,
                              const set<string> &keys, map<string, bufferlist> *out) {
    FTRACE

    Collection *c = static_cast<Collection*>(c_.get());
    if (!c->exists)
        return -ENOENT;

    
    std::shared_lock l(c->lock);
    
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
        
        return -ENOENT;
    }

    if (!o->onode.has_omap()) {
        
        return 0;
    }

    IoContext ioc (0);

    for (const std::string &p : keys) {
        if (o->onode.omaps.find(p) != o->onode.omaps.end()) {
            bufferlist &bl = (*out)[p];
            db.aio_read_omap(o->onode.nid, p, bl, &ioc);
        }
    }
    
    return db.aio_submit_and_wait(&ioc);
}



int KvsStore::omap_get_impl(Collection *c, const ghobject_t &oid,
                       bufferlist *header, map<string, bufferlist> *out) {
    FTRACE

    if (!c->exists)
        return -ENOENT;

    
    std::shared_lock l(c->lock);
    
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
        
        return -ENOENT;
    }

    if (!o->onode.has_omap()) {
        
        return 0;
    }

    *header = o->onode.omap_header;

    IoContext ioc (0);

    for (const std::string &p : o->onode.omaps) {
        bufferlist &bl = (*out)[p];
        db.aio_read_omap(o->onode.nid, p, bl, &ioc);
    }
    
    return db.aio_submit_and_wait(&ioc);
}



class KvsStore::OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    KvsStore *store;
    CollectionRef c;
    OnodeRef o;
    map<string,bufferlist> omap;
    set<std::string>::iterator it;
public:
    OmapIteratorImpl(KvsStore *store_, CollectionRef c, OnodeRef o)
            : store(store_), c(c), o(o) {

        seek_to_first();
    }

    int seek_to_first() override {
        FTRACE
        std::shared_lock l(c->lock);
        
        if (o->onode.has_omap()) {
            it = o->onode.omaps.begin();
        } else {
            it = set<std::string>::iterator();
        }
        
        return 0;
    }
    int upper_bound(const string &after) override {
        FTRACE
        std::shared_lock l(c->lock);
        
        if (o->onode.has_omap()) {
            it = o->onode.omaps.upper_bound(after);
        } else {
            it = set<std::string>::iterator();
        }
        
        return 0;
    }
    int lower_bound(const string &to) override {
        FTRACE
        std::shared_lock l(c->lock);
        
        if (o->onode.has_omap()) {
            it = o->onode.omaps.lower_bound(to);
        } else {
            it = set<std::string>::iterator();
        }
        
        return 0;
    }
    bool valid() override {
        FTRACE
        std::shared_lock l(c->lock);
        
        
        return o->onode.has_omap() && it != o->onode.omaps.end();
    }

    int next() override {
        FTRACE
        int r = -1;
        
        std::shared_lock l(c->lock);
        
        if (o->onode.has_omap()) {
            ++it;
            r = 0;
        }
        
        return r;
    }
    string key() override {
        FTRACE
        std::shared_lock l(c->lock);
        
        ceph_assert(valid());
        
        return *it;
    }

    bufferlist value() override {
        FTRACE
        std::shared_lock l(c->lock);
        
        bufferlist bl;

        IoContext ioc ( 0);

        const std::string &key = *it;

        store->db.aio_read_omap(o->onode.nid, key, bl, &ioc);

        int r = store->db.aio_submit_and_wait(&ioc);
        ceph_assert(r == 0);
        
        return bl;
    }

    int status() override {
        return 0;
    }
};

ObjectMap::ObjectMapIterator KvsStore::get_omap_iterator(CollectionHandle &c_,
                                                         const ghobject_t &oid) {
    FTRACE
    Collection *c = static_cast<Collection*>(c_.get());
    if (!c->exists) {
        return ObjectMap::ObjectMapIterator();
    }

    
    std::shared_lock l(c->lock);
    
    OnodeRef o = c->get_onode(oid, false);
    if (!o || !o->exists) {
        
        dout(10) << __func__ << " " << oid << "doesn't exist" <<dendl;
        return ObjectMap::ObjectMapIterator();
    }
    o->flush();
    
    return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(this, c, o));
}


/// ------------------------------------------------------------------------------------------------
/// Collections
/// ------------------------------------------------------------------------------------------------

void KvsStore::_queue_reap_collection(CollectionRef &c) {
    FTRACE
    dout(10) << __func__ << " " << c << " " << c->cid << dendl;
    removed_collections.push_back(c);
}

int KvsStore::_create_collection(TransContext *txc, const coll_t &cid,
                                 unsigned bits, CollectionRef *c) {
    FTRACE
    dout(15) << __func__ << " " << cid << " bits " << bits << dendl;

    {
        std::unique_lock l(coll_lock);
        if (*c) {
            derr << " the collection already exists: " << cid << dendl;
            return -EEXIST;
        }
        auto p = new_coll_map.find(cid);
        ceph_assert(p != new_coll_map.end());
        *c = p->second;
        (*c)->cnode.bits = bits;
        coll_map[cid] = *c;
        new_coll_map.erase(p);
    }

    txc->coll_data.emplace_back();
    bufferlist &bl = txc->coll_data.back();
    encode((*c)->cnode, bl);
    TR << "create COLL addr = " << (void*)bl.c_str() << ", value hash = " << ceph_str_hash_linux(bl.c_str(), bl.length());
    db.aio_write_coll(cid, bl, &txc->ioc);

    return 0;
}

int KvsStore::_remove_collection(TransContext *txc, const coll_t &cid,
                                 CollectionRef *c) {
    FTRACE
    int r;

    (*c)->flush_all_but_last();

    {
        std::unique_lock l(coll_lock);
        if (!*c) {
            r = -ENOENT;
            goto out;
        }

        size_t nonexistent_count = 0;
        assert((*c)->exists);
        if ((*c)->onode_map.map_any([&](OnodeRef o) {
            if (o->exists) {
                derr << __func__ << " " << o->oid << "(" << &o->oid <<") " << o
                     << " exists in onode_map" << dendl;
                return true;
            }
            ++nonexistent_count;
            return false;
        })) {
            r = -ENOTEMPTY;
            goto out;
        }

        vector<ghobject_t> ls;
        ghobject_t next;

        // Enumerate onodes in db, up to nonexistent_count + 1
        // then check if all of them are marked as non-existent.
        // Bypass the check if returned number is greater than nonexistent_count
        r = _collection_list(c->get(), ghobject_t(), ghobject_t::get_max(),
                             nonexistent_count + 1, &ls, &next);

        if (r >= 0) {
            bool exists = (!next.is_max());

            for (auto it = ls.begin(); !exists && it < ls.end(); ++it) {
                dout(10) << __func__ << " oid " << *it << dendl;
                auto onode = (*c)->onode_map.lookup(*it);
                exists = !onode || onode->exists;
                if (exists) {
                    dout(1) << __func__ << " " << *it
                            << " exists in db, "
                            << (!onode ? "not present in ram" : "present in ram")
                            << dendl;
                }
            }
            if (!exists) {
                _do_remove_collection(txc, c);
                r = 0;
            } else {
                derr << __func__ << " " << cid << " is non-empty"
                     << dendl;
                r = -ENOTEMPTY;
            }
        }
    }

    out:
    dout(10) << __func__ << " " << cid << " = " << r << dendl;
    return r;
}

int KvsStore::_do_remove_collection(TransContext *txc, CollectionRef *c)
{
    FTRACE
    coll_map.erase((*c)->cid);
    txc->removed_collections.push_back(*c);
    (*c)->exists = false;
    _osr_register_zombie((*c)->osr.get());
    db.aio_remove_coll((*c)->cid, &txc->ioc);
    c->reset();
    return 0;
}

int KvsStore::_split_collection(TransContext *txc, CollectionRef &c,
                                CollectionRef &d, unsigned bits, int rem) {
    FTRACE
    dout(20) << __func__ << " " << c->cid << " to " << d->cid << " "
             << " bits " << bits << dendl;
    std::unique_lock l(c->lock);
    
    std::unique_lock l2(d->lock);

    // flush all previous deferred writes on this sequencer.  this is a bit
    // heavyweight, but we need to make sure all deferred writes complete
    // before we split as the new collection's sequencer may need to order
    // this after those writes, and we don't bother with the complexity of
    // moving those TransContexts over to the new osr.
    {
        OpSequencer *osr = txc->osr.get();
        osr->drain_preceding(txc);
    }

    // move any cached items (onodes and referenced shared blobs) that will
    // belong to the child collection post-split.  leave everything else behind.
    // this may include things that don't strictly belong to the now-smaller
    // parent split, but the OSD will always send us a split for every new
    // child.

    spg_t pgid, dest_pgid;
    bool is_pg = c->cid.is_pg(&pgid);
    assert(is_pg);
    is_pg = d->cid.is_pg(&dest_pgid);
    assert(is_pg);

    // the destination should initially be empty.
    assert(d->onode_map.empty());
    assert(d->cnode.bits == bits);

    c->split_cache(d.get());

    // adjust bits.  note that this will be redundant for all but the first
    // split call for this parent (first child).
    c->cnode.bits = bits;
    assert(d->cnode.bits == bits);

    txc->coll_data.emplace_back();
    bufferlist &bl = txc->coll_data.back();

    encode(c->cnode, bl);
    db.aio_write_coll(c->cid, bl, &txc->ioc);

    
    return 0;
}

// new feature
int KvsStore::_merge_collection(TransContext *txc, CollectionRef *c,
                                CollectionRef &d, unsigned bits) {
    FTRACE
    dout(15) << __func__ << " " << (*c)->cid << " to " << d->cid << " bits "
             << bits << dendl;

    std::unique_lock l((*c)->lock);
    std::unique_lock l2(d->lock);
    int r;

    coll_t cid = (*c)->cid;

    // flush all previous deferred writes on the source collection to ensure
    // that all deferred writes complete before we merge as the target collection's
    // sequencer may need to order new ops after those writes.

    _osr_drain((*c)->osr.get());

    // move any cached items (onodes and referenced shared blobs) that will
    // belong to the child collection post-split.  leave everything else behind.
    // this may include things that don't strictly belong to the now-smaller
    // parent split, but the OSD will always send us a split for every new
    // child.

    spg_t pgid, dest_pgid;
    bool is_pg = cid.is_pg(&pgid);
    ceph_assert(is_pg);
    is_pg = d->cid.is_pg(&dest_pgid);
    ceph_assert(is_pg);

    // adjust bits.  note that this will be redundant for all but the first
    // merge call for the parent/target.
    d->cnode.bits = bits;

    // behavior depends on target (d) bits, so this after that is updated.
    (*c)->split_cache(d.get());

    // remove source collection
    {
        std::unique_lock l3(coll_lock);
        _do_remove_collection(txc, c);
    }

    r = 0;

    txc->coll_data.emplace_back();
    bufferlist &bl = txc->coll_data.back();
    encode(d->cnode, bl);
    db.aio_write_coll(d->cid, bl, &txc->ioc);

    dout(10) << __func__ << " " << cid << " to " << d->cid << " " << " bits "
             << bits << " = " << r << dendl;
    return r;
}

static void get_coll_key_range(const coll_t& cid, int bits, kv_key *temp_start, kv_key *temp_end, kv_key *start, kv_key *end ) {
    FTRACE
    //TR << "coll key range " << cid << ", bits = " << bits;
    struct kvs_onode_key* temp_s_key = (struct kvs_onode_key*)temp_start->key;
    struct kvs_onode_key* temp_e_key = (struct kvs_onode_key*)temp_end->key;
    struct kvs_onode_key* s_key = (struct kvs_onode_key*)start->key;
    struct kvs_onode_key* e_key = (struct kvs_onode_key*)end->key;

    s_key->prefix = GROUP_PREFIX_ONODE;
    e_key->prefix = GROUP_PREFIX_ONODE;
    temp_s_key->prefix = GROUP_PREFIX_ONODE;
    temp_e_key->prefix = GROUP_PREFIX_ONODE;

    spg_t pgid;
    if (cid.is_pg(&pgid)) {
        // shard id

        s_key->shardid 	    = int8_t(pgid.shard) + 0x80;
        e_key->shardid 		= s_key->shardid;
        temp_s_key->shardid = s_key->shardid;
        temp_e_key->shardid = s_key->shardid;

        // poolid
        s_key->poolid = pgid.pool() + 0x8000000000000000ull;
        e_key->poolid = s_key->poolid;
        temp_s_key->poolid = (-2ll - pgid.pool()) + 0x8000000000000000ull;
        temp_e_key->poolid = temp_s_key->poolid;

        // hash, start key
        uint32_t reverse_hash = hobject_t::_reverse_bits(pgid.ps());
        s_key->bitwisekey = reverse_hash;
        temp_s_key->bitwisekey = reverse_hash;

        // hash, end key
        uint64_t end_hash = reverse_hash + (1ull << (32 - bits));
        if (end_hash > 0xffffffffull)
            end_hash = 0xffffffffull;

        //TR << "PG end hash = " << end_hash;
        e_key->bitwisekey = end_hash;
        temp_e_key->bitwisekey = end_hash;

    } else {
        // shardid
        s_key->shardid 	    = int8_t(shard_id_t::NO_SHARD) + 0x80;
        e_key->shardid 		= s_key->shardid;

        // poolid
        s_key->poolid = -1ull + 0x8000000000000000ull;
        e_key->poolid = s_key->poolid;

        // hash
        s_key->bitwisekey = 0;
        e_key->bitwisekey = 0xffffffff;

        // no separate temp section
        temp_s_key->shardid = e_key->shardid;
        temp_e_key->shardid = e_key->shardid;

        temp_s_key->poolid  = e_key->poolid;
        temp_e_key->poolid  = e_key->poolid;

        temp_s_key->bitwisekey = e_key->bitwisekey;
        temp_e_key->bitwisekey = e_key->bitwisekey;
    }
}


int KvsStore::_collection_list(Collection *c, const ghobject_t &start,
                               const ghobject_t &end, int max, vector<ghobject_t> *ls,
                               ghobject_t *pnext) {
    FTRACE
    int r = 0;
    bool set_next = false;
    kv_key pend;
    bool temp;
    // key buffers;
    char buf1[17], buf2[17], buf3[17], buf4[17], tempbuf[256];
    kv_key temp_start_key = {buf1, 17}, temp_end_key = {buf2, 17};
    kv_key start_key= {buf3, 17}, end_key= {buf4, 17};
    KvsIterator *it = 0;
    ghobject_t static_next;
    if (!pnext)
        pnext = &static_next;

    if (start == ghobject_t::get_max() || start.hobj.is_max()) {
        goto out;
    }

    get_coll_key_range(c->cid, c->cnode.bits, &temp_start_key, &temp_end_key, &start_key, &end_key);



    dout(20) << __func__ << " range " << print_kvssd_key(temp_start_key.key, temp_start_key.length)
             << " to " << print_kvssd_key(temp_end_key.key, temp_end_key.length) << " and "
             << print_kvssd_key(start_key.key, start_key.length) << " to "
             << print_kvssd_key(end_key.key, end_key.length) << " start " << start << dendl;

    {
        db.compact();

        TR << " range " << print_kvssd_key(temp_start_key.key, temp_start_key.length)
           << " to " << print_kvssd_key(temp_end_key.key, temp_end_key.length) << " and "
           << print_kvssd_key(start_key.key, start_key.length) << " to "
           << print_kvssd_key(end_key.key, end_key.length) << " start " << start;

        it = db.get_iterator(GROUP_PREFIX_ONODE);


        TR << "it valid = ? " << it->valid();
/*
        while (true) {
            if (it->valid()) {
                kv_key key = it->key();
                TR << " key list " << print_kvssd_key(key.key, key.length) << end;
                it->next();
            }
            else
                break;
        }*/

        if (start == ghobject_t() || start == c->cid.get_min_hobj()) {
            it->upper_bound(temp_start_key);
            temp = true;
        } else {
            kv_key k = {tempbuf, 255} ;
            k.length = construct_onode_key(cct,start, tempbuf);
            if (start.hobj.is_temp()) {
                temp = true;
            } else {
                temp = false;
            }

            it->lower_bound(k);
        }

        if (end.hobj.is_max()) {
            pend = temp ? temp_end_key : end_key;
        } else {
            end_key.length = construct_onode_key(cct, end, end_key.key);

            if (end.hobj.is_temp()) {
                if (temp)
                    pend = end_key;
                else
                    goto out;
            } else {
                pend = temp ? temp_end_key : end_key;
            }
        }


        while (true) {
            TR  << " current key " << print_kvssd_key(it->key().key, it->key().length);
            TR  << " pend    key " << print_kvssd_key(pend.key, pend.length);
            TR << "  greater than pend ? " << db.is_key_ge(it->key(), pend);
            if (!it->valid() || db.is_key_ge(it->key(), pend)) {
                TR << "iter: key not valid : greater than pend ? " << db.is_key_ge(it->key(), pend);
                if (!it->valid())
                    dout(20) << __func__ << " iterator not valid (end of db?)" << dendl;
                else {
                    kv_key key = it->key();
                    dout(20) << __func__ << " key " << print_kvssd_key(key.key, key.length) << " > " << end << dendl;
                }

                if (temp) {
                    if (end.hobj.is_temp()) {
                        break;
                    }
                    TR << "upper bound normal key  ";
                    dout(30) << __func__ << " switch to non-temp namespace" << dendl;

                    temp = false;
                    it->upper_bound(start_key);
                    pend = end_key;
                    dout(30) << __func__ << " pend " << print_kvssd_key(pend.key, pend.length) << dendl;
                    continue;
                }
                TR << "iter: exit ";
                break;
            }
            TR << __func__ << " key " << it->key().key << ", length " << (int)it->key().length;
            TR << __func__ << " key " << print_kvssd_key(it->key().key, it->key().length);
            kv_key key = it->key();
            if (key.length > 0) {
                ghobject_t oid;
                construct_onode_ghobject_t(cct, key, &oid);
                TR << __func__ << " oid = " << oid << max;
                ceph_assert(r == 0);
                if (ls->size() >= (unsigned) max) {

                    *pnext = oid;
                    set_next = true;
                    break;
                }
                ls->push_back(oid);
            }
            TR << __func__ << "ls size = " << ls->size() << "\n";
            it->next();
        }
        TR << __func__ << "iter finished" << "\n";
    }
    out:
    TR << __func__ << "iter returns" << "\n";
    if (!set_next) {
        *pnext = ghobject_t::get_max();
    }
    if (it) delete it;
    return r;
}

/// ------------------------------------------------------------------------------------------------
/// Transaction
/// ------------------------------------------------------------------------------------------------

void KvsStore::txc_aio_finish(TransContext *txc) {
    //FTRACE
    _txc_state_proc(txc);
}

void KvsStore::_txc_state_proc(TransContext *txc) {
    FTRACE
    while (true) {

        switch (txc->state) {
            case TransContext::STATE_PREPARE:
                TR << "TXC 1 " << (void*) txc << ", STATE: PREPARE";

                if (txc->ioc.has_pending_aios()) {
                    txc->state = TransContext::STATE_AIO_SUBMITTED;
                    txc->had_ios = true;
                    _txc_aio_submit(txc);
                    return;
                }
                // ** fall-thru **

            case TransContext::STATE_AIO_SUBMITTED:
                TR << "TXC 2 " << (void*) txc <<  "_txc_finish_io start";
                // io finished
                _txc_finish_io(txc);  // called by a IO callback function
                TR << "TXC 2 " << (void*) txc <<  "_txc_finish_io done";
                return;

            case TransContext::STATE_AIO_DONE:
                TR << "TXC 3 " << (void*) txc <<  " STATE AIO DONE start";
                txc->state = TransContext::STATE_FINALIZE;
                {
                    std::lock_guard l(kv_finalize_lock);
                    kv_finalize_queue.push_back(txc);
                    kv_finalize_cond.notify_one();
                }
                TR << "TXC 3 " << (void*) txc <<  " STATE AIO DONE end";
                return;

            case TransContext::STATE_FINISHING:
                TR << "TXC 4" << (void*) txc <<  "STATE FINISHING start";
                _txc_finish(txc);    // called by a finalize thread
                TR << "TXC 4" << (void*) txc <<  "STATE FINISHING DONE end";
                return;
            case TransContext::STATE_FINALIZE:
                TR << "shouldn't be called";
                assert(0 == "unexpected txc state");
                return;
            default:
                derr << __func__ << " unexpected txc " << txc << " state "
                     << txc->get_state_name() << dendl;
                assert(0 == "unexpected txc state");
                return;
        }
    }
}

void KvsStore::_txc_finish_io(TransContext *txc)
{
    FTRACE
    OpSequencer *osr = txc->osr.get();
    TR << "TXC 2 try getting a qlock";
    std::lock_guard l(osr->qlock);
    TR << "TXC 2 qlock acquired, txc = " << (void*)txc << ", state -> AIO_DONE";
    txc->state = TransContext::STATE_AIO_DONE;
    TR << "TXC 2 release";
    txc->ioc.release_running_aios();
    TR << "TXC 2 check";
    {
        OpSequencer::q_list_t::iterator p = osr->q.begin();
        while (p != osr->q.end()) {
            TransContext *t = &*p;
            TR << "dump txc " << (void *) t << ", state = " << t->state;
            p++;
        }
    }

    TR << "TXC 2 Status check";
    OpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
    while (p != osr->q.begin()) {
        --p;
        TR << "p " << (void*)&(*p) << ", state = " << p->state;
        if (p->state < TransContext::STATE_AIO_DONE) {
            TR << "return";
            return;
        }
        if (p->state > TransContext::STATE_AIO_DONE) {
            ++p;
            TR << "move next and break ";
            break;
        }
        TR << "move next";
    }

    TR << "TXC 2 TXC State Proc";
    // process the stored transactions
    do {
        TR << "process a pending txc_state_proc: state = " << (int) p->state;
        _txc_state_proc(&*p++);
        TR << "process a pending txc_state_proc - done ";
    } while (p != osr->q.end() && p->state == TransContext::STATE_AIO_DONE);

    TR << "notify all";
    if (osr->kv_submitted_waiters) {
        osr->qcond.notify_all();
    }
    TR << "TXC 2 TXC State Proc done";
}

void KvsStore::_txc_committed_kv(TransContext *txc) {
    FTRACE
    TR << "osr qlock try txc = " << (void*)txc;
    if (txc->osr) {

    } else {
        TR << "osr qlock try txc->osr is null";
    }

    std::lock_guard l(txc->osr->qlock);
    TR << "osr qlock acquired";
    txc->state = TransContext::STATE_FINISHING;
    if (txc->ch->commit_queue) {
        txc->ch->commit_queue->queue(txc->oncommits);
    } else {
        finisher.queue(txc->oncommits);
    }
}


void KvsStore::_kv_finalize_thread() {
    FTRACE
    deque<TransContext *> kv_committed;

    std::unique_lock<std::mutex> l(kv_finalize_lock);
    assert(!kv_finalize_started);
    kv_finalize_started = true;
    kv_finalize_cond.notify_all();

    while (true) {

        assert(kv_committed.empty());
        if (kv_finalize_queue.empty()) {
            if (kv_finalize_stop)
                break;
            kv_finalize_cond.wait(l);
        } else {
            kv_committed.swap(kv_finalize_queue);
            l.unlock();
            TR << "kv_finalize start";
            /*
            for (auto txc : kv_committed) {

                TR << "before commited_kv";

                TR << "after commited_kv";

                if (txc->state == TransContext::STATE_AIO_DONE) {
                    _txc_apply_kv(txc);
                    txc->state = TransContext::STATE_FINISHING;
                    if (txc->osr->kv_submitted_waiters) {
                        TR << "before kv_submitted_waiters";
                        std::lock_guard l(txc->osr->qlock);
                        TR << "OSR QUEUE LOCK";
                        txc->osr->qcond.notify_all();
                        TR << "after kv_submitted_waiters";
                    }

                } else {
                    ceph_assert(txc->state == TransContext::STATE_FINISHING);
                }
            }
             */
            TR << "calling txc_state_proc";

            while (!kv_committed.empty()) {
                TransContext *txc = kv_committed.front();

                TR << "before _txc_committed_kv: txc " << (void*)txc;
                _txc_committed_kv(txc);
                TR << "after _txc_committed_kv";

                assert(txc->state == TransContext::STATE_FINISHING);
                TR << "calling txc_state_proc -3";
                _txc_state_proc(txc);
                TR << "calling txc_state_proc -4";
                kv_committed.pop_front();
                TR << "calling txc_state_proc -5";
            }

            kv_committed.clear();

            TR << "reap collections";
            // this is as good a place as any ...
            reap_collections();
            TR << "kv_finalize finished";

            l.lock();
        }
    }

    kv_finalize_started = false;
}

void KvsStore::_txc_finish_writes(TransContext *txc) {
    FTRACE
    TR << "txc = " << (void*)txc;
    TR << "txc onode size = " << txc->onodes.size();
    for (const OnodeRef& o : txc->onodes) {
        TR << "1";
        if (!o) {
            TR << "onode is destroyed";
        }
        else {
            TR << "nref = " << o->nref.load();
            TR << "c = " << (void*)o->c;
            TR << "cache = " << (void*)o->c->cache;
        }
        BufferCacheShard *cache = o->c->cache;
        TR << "cache->lock, cache = ? " << (void*)cache;
        std::lock_guard l(cache->lock);
        TR << "cache->lock acquired";
        if (o->c->cache != cache) {
            continue;
        }
        TR << "finish write ";
        o->bc._finish_write(cache, txc->seq);
        TR << "finish done";
        break;
    }
    TR << "txc2 = " << (void*)txc;
    // objects we modified but didn't affect the onode
    auto p = txc->modified_objects.begin();
    while (p != txc->modified_objects.end()) {
        if (txc->onodes.count(*p) == 0) {
            ++p;
        } else {
            TR << "1";
            auto o = *p;
            BufferCacheShard *cache = o->c->cache;
            std::lock_guard l(cache->lock);
            if (o->c->cache != cache) {
                TR << "next";
                ++p;
                continue;
            }
            TR << "finish write 1";
            o->bc._finish_write(cache, txc->seq);
            TR << "finish write 2";
            break;
        }
    }
}

void KvsStore::_txc_finish(TransContext *txc) {
    FTRACE
    dout(20) << __func__ << " " << txc << " onodes " << txc->onodes << dendl;
    assert(txc->state == TransContext::STATE_FINISHING);
    TR << "TXC 4 before finish writes";

    _txc_finish_writes(txc);
    TR << "TXC 4 after finish writes";

    while (!txc->removed_collections.empty()) {
        _queue_reap_collection(txc->removed_collections.front());
        txc->removed_collections.pop_front();
    }

    OpSequencerRef osr = txc->osr;
    bool empty = false;
    OpSequencer::q_list_t releasing_txc;
    {
        TR << "qlock try";
        std::lock_guard l(osr->qlock);
        TR << "qlock acquired";
        txc->state = TransContext::STATE_DONE;
        bool notify = false;
        while (!osr->q.empty()) {
            TransContext *txc = &osr->q.front();

            if (txc->state != TransContext::STATE_DONE) {
                if (osr->kv_drain_preceding_waiters && txc->state == TransContext::STATE_PREPARE) {
                    // for _osr_drain_preceding()
                    notify = true;
                }
                break;
            }

            osr->q.pop_front();
            releasing_txc.push_back(*txc);
        }

        if (osr->q.empty()) {
            dout(20) << __func__ << " osr " << osr << " q now empty" << dendl;
            empty = true;
        }

        if (notify || empty) {
            osr->qcond.notify_all();
        }
    }

    while (!releasing_txc.empty()) {

        auto txc = &releasing_txc.front();
        //TR << "4.2 txc = " << (void*) txc;
        //_txc_release_alloc(txc);

        releasing_txc.pop_front();
        TR << "txc deleted = " << (void*) txc;
        delete txc;
    }

    if (empty && osr->zombie) {
        std::lock_guard l(zombie_osr_lock);
        if (zombie_osr_set.erase(osr->cid)) {
            dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
        } else {
            dout(10) << __func__ << " empty zombie osr " << osr << " already reaped" << dendl;
        }
    }

}

// do nothing
void KvsStore::_txc_release_alloc(TransContext *txc) {}

void KvsStore::_txc_apply_kv(TransContext *txc)
{
    /*
    FTRACE
    for (auto ls : { &txc->onodes, &txc->modified_objects }) {
        for (auto& o : *ls) {
            dout(20) << __func__ << " onode " << o << " had " << o->flushing_count
                     << dendl;
            if (--o->flushing_count == 0 && o->waiting_count.load()) {
                TR << "flush_lock, flushing count = " << o->flushing_count.load();
                std::lock_guard l(o->flush_lock);
                TR << "flush_lock acquired";
                o->flush_cond.notify_all();
            }
        }
    }
    */
}


void KvsStore::_txc_aio_submit(TransContext *txc) {
    FTRACE

    db.aio_submit(&txc->ioc);
}

/// ------------------------------------------------------------------------------------------------
/// OP Sequencer
/// ------------------------------------------------------------------------------------------------

void KvsStore::_osr_attach(Collection *c) {
	FTRACE
// caller has RWLock on coll_map
	auto q = coll_map.find(c->cid);
	if (q != coll_map.end()) {
		c->osr = q->second->osr;
        //TR << "reusing osr " << c->cid ;
		ldout(cct, 10) << __func__ << " " << c->cid << " reusing osr " << c->osr << " from existing coll " << q->second << dendl;

	} else {
		std::lock_guard l(zombie_osr_lock);
		auto p = zombie_osr_set.find(c->cid);
		if (p == zombie_osr_set.end()) {
			c->osr = ceph::make_ref<OpSequencer>(this, next_sequencer_id++, c->cid);
			ldout(cct, 10) << __func__ << " " << c->cid << " fresh osr " << c->osr << dendl;

		} else {
			c->osr = p->second;
            zombie_osr_set.erase(p);
			ldout(cct, 10) << __func__ << " " << c->cid << " resurrecting zombie osr " << c->osr << dendl;
			c->osr->zombie = false;
		}
	}
}

void KvsStore::_osr_register_zombie(OpSequencer *osr) {
	FTRACE
	std::lock_guard l(zombie_osr_lock);
	dout(10) << __func__ << " " << osr << " " << osr->cid << dendl;
	osr->zombie = true;
	auto i = zombie_osr_set.emplace(osr->cid, osr);
	// this is either a new insertion or the same osr is already there
	ceph_assert(i.second || i.first->second == osr);
}

void KvsStore::_osr_drain(OpSequencer *osr) {
	FTRACE
	osr->drain();
}

void KvsStore::_osr_drain_preceding(TransContext *txc)
{
    FTRACE
    OpSequencer *osr = txc->osr.get();
    dout(10) << __func__ << " " << txc << " osr " << osr << dendl;
    osr->kv_drain_preceding_waiters++;
    osr->drain_preceding(txc);
    osr->kv_drain_preceding_waiters--;
    dout(10) << __func__ << " " << osr << " done" << dendl;
}



///--------------------------------------------------------
/// Index Threads
///--------------------------------------------------------

void KvsStore::_kv_index_thread() {
#if 0
    FTRACE
    derr << "index thread..." << dendl;
    // load pages from the AOL
    static double index_interval_us = 1000000.0; // 1 second
    while(true) {
        //db.compact();

        {
            std::unique_lock l (kv_lock );
            if (kv_index_stop) break;
        }

        usleep(index_interval_us);
    }

#endif
}

///--------------------------------------------------------------
/// Callback Thread
///--------------------------------------------------------------

void KvsStore::_kv_callback_thread() {
    FTRACE
    uint32_t toread = 128;

    while (!kv_stop) {
        if (kv_stop) {
            derr << "kv_callback_thread: stop requested" << dendl;
            break;
        }

        if (this->db.is_opened()) {
            this->db.poll_completion(toread, 100);
        }
    }

}