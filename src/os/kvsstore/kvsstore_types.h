/*
 * kvsstore_types.h
 *
 *  Created on: Nov 17, 2019
 *      Author: root
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_TYPES_H_
#define SRC_OS_KVSSTORE_KVSSTORE_TYPES_H_

#include <unistd.h>
#include <memory.h>

#include <atomic>
#include <mutex>
#include <vector>
#include <list>
#include <map>
#include <condition_variable>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/intrusive_ptr.hpp>

#include "include/types.h"
#include "include/utime.h"
#include "include/ceph_assert.h"
#include "include/unordered_map.h"
#include "include/mempool.h"
#include "include/hash.h"
#include "common/hobject.h"
#include "common/ref.h"
#include "common/bloom_filter.hpp"
#include "common/Finisher.h"
#include "common/ceph_mutex.h"
#include "common/Throttle.h"
#include "common/perf_counters.h"
#include "common/PriorityCache.h"
#include "common/RefCountedObj.h"
#include "os/ObjectStore.h"


#include "keyencoder.h"
#include "kvssd_types.h"
#include "kvsstore_omap.h"

class KvsStore;
class KvsStoreCache;

using namespace ceph;

#define MAX_BUFFER_SLOP_RATIO_DEN 8

struct kvsstore_sb_t {
    uint64_t nid_last;
    uint64_t is_uptodate;

    explicit kvsstore_sb_t(): nid_last(0), is_uptodate(0) {}

    DENC(kvsstore_sb_t, v, p) {
        DENC_START(1, 1, p);
            denc(v.nid_last, p);
            denc(v.is_uptodate, p);
        DENC_FINISH(p);
    }
    void dump(Formatter *f) const{
        f->dump_unsigned("nid_last", nid_last);
        f->dump_unsigned("is_uptodate", is_uptodate);
    }
    static void generate_test_instances(list<kvsstore_sb_t*>& o){}

};


/// collection metadata
struct kvsstore_cnode_t {
    uint32_t bits;   ///< how many bits of coll pgid are significant

    explicit kvsstore_cnode_t(int b=0) : bits(b) {}

    DENC(kvsstore_cnode_t, v, p) {
        DENC_START(1, 1, p);
            denc(v.bits, p);
        DENC_FINISH(p);
    }
    void dump(Formatter *f) const{f->dump_unsigned("bits", bits);}
    static void generate_test_instances(list<kvsstore_cnode_t*>& o){}

};

template <typename Map>
bool map_compare (Map const &lhs, Map const &rhs) {
    // No predicate needed because there is operator== for pairs already.
    return lhs.size() == rhs.size()
           && std::equal(lhs.begin(), lhs.end(),
                         rhs.begin());
}

template <typename Map>
bool map_compare2 (Map const &lhs, Map const &rhs) {
    // No predicate needed because there is operator== for pairs already.
    bool ret =  lhs.size() == rhs.size();
    if (!ret) return ret;

    const auto it = rhs.begin();
    for (const auto &p : lhs) {
        std::string s (p.second.c_str(), p.second.length());
        std::string s2 (it->second.c_str(), it->second.length());

        if (!(p.first == it->first && s == s2)) return false;
    }
    return true;
}

/// onode: per-object metadata
struct kvsstore_onode_t {
    uint64_t nid = 0;
    uint64_t size = 0;
    bufferlist omap_header;
    // omap
    bool omap_loaded = false;
    bool omap_dirty  = false;
    //std::set<std::string> omaps_write_buffer;
    std::set<std::string> omaps;
    bufferptr omap_wb;
    //std::set<std::string> omaps_cache;
    uint8_t  num_omap_extents = 0;
    bufferlist omap_keys;
    //bufferlist omap_wb;
//
    map<mempool::kvsstore_cache_other::string, bufferptr>  attrs;        ///< attrs

    inline bool has_omap() const {
        if (omap_loaded) return !omaps.empty();
        return (omap_wb.have_raw() && omap_wb.length() > 0) || num_omap_extents > 0;
    }

    DENC(kvsstore_onode_t, v, p) {
        DENC_START(1, 1, p);
            denc_varint(v.nid, p);
            denc_varint(v.size, p);
            denc(v.attrs, p);
            //denc(v.num_omap_extents, p);
            denc(v.omap_keys, p);
            denc(v.omap_wb, p);
            denc(v.omap_header, p);
            //denc(v.flags, p);
        DENC_FINISH(p);
    }

    void dump(Formatter *f) const {}
    static void generate_test_instances(list<kvsstore_onode_t*>& o) {}
};

///  ====================================================
///  Object Store Data Types
///  ====================================================

class KvsStoreTypes {
public:

    // forward declarations
    class OpSequencer;
    struct Collection;
    struct TransContext;
    struct OnodeSpace;
    struct Onode;
    typedef boost::intrusive_ptr<Collection> CollectionRef;
    typedef boost::intrusive_ptr<Onode> OnodeRef;
    using OpSequencerRef = ceph::ref_t<OpSequencer>;
    typedef map<uint64_t, bufferlist> ready_regions_t;
    typedef vector<std::pair<uint64_t,uint64_t> > zero_regions_t;
    typedef vector<uint16_t> chunk2read_t;

    struct BufferSpace;
    struct BufferCacheShard;

public:
    ///------------------------------------------------------------
    /// on-disk data structures
    ///------------------------------------------------------------

    /// superblock



public:

    ///------------------------------------------------------------
    /// LRU cache for onodes and buffers, borrowed from Bluestore
    ///------------------------------------------------------------

    struct Buffer {
        MEMPOOL_CLASS_HELPERS();

        enum {
            STATE_EMPTY,     ///< empty buffer -- used for cache history
            STATE_CLEAN,     ///< clean data that is up to date
            STATE_WRITING,   ///< data that is being written (io not yet complete)
        };
        static const char *get_state_name(int s) {
            switch (s) {
                case STATE_EMPTY: return "empty";
                case STATE_CLEAN: return "clean";
                case STATE_WRITING: return "writing";
                default: return "???";
            }
        }
        enum {
            FLAG_NOCACHE = 1,  ///< trim when done WRITING (do not become CLEAN)
            // NOTE: fix operator<< when you define a second flag
        };
        static const char *get_flag_name(int s) {
            switch (s) {
                case FLAG_NOCACHE: return "nocache";
                default: return "???";
            }
        }

        BufferSpace *space;
        uint16_t state;             ///< STATE_*
        uint16_t cache_private = 0; ///< opaque (to us) value used by Cache impl
        uint32_t flags;             ///< FLAG_*
        uint64_t seq;
        uint32_t offset, length;
        bufferlist data;

        boost::intrusive::list_member_hook<> lru_item;
        boost::intrusive::list_member_hook<> state_item;

        Buffer(BufferSpace *space, unsigned s, uint64_t q, uint32_t o, uint32_t l,
               unsigned f = 0)
                : space(space), state(s), flags(f), seq(q), offset(o), length(l) {}
        Buffer(BufferSpace *space, unsigned s, uint64_t q, uint32_t o, bufferlist& b,
               unsigned f = 0)
                : space(space), state(s), flags(f), seq(q), offset(o),
                  length(b.length()), data(b) {}

        bool is_empty() const {
            return state == STATE_EMPTY;
        }
        bool is_clean() const {
            return state == STATE_CLEAN;
        }
        bool is_writing() const {
            return state == STATE_WRITING;
        }

        uint32_t end() const {
            return offset + length;
        }

        void truncate(uint32_t newlen) {
            ceph_assert(newlen < length);
            if (data.length()) {
                bufferlist t;
                t.substr_of(data, 0, newlen);
                data.claim(t);
            }
            length = newlen;
        }
        void maybe_rebuild() {
            if (data.length() &&
                (data.get_num_buffers() > 1 ||
                 data.front().wasted() > data.length() / MAX_BUFFER_SLOP_RATIO_DEN)) {
                data.rebuild();
            }
        }

        void dump(Formatter *f) const {
            f->dump_string("state", get_state_name(state));
            f->dump_unsigned("seq", seq);
            f->dump_unsigned("offset", offset);
            f->dump_unsigned("length", length);
            f->dump_unsigned("data_length", data.length());
        }
    };

// buffer map, read/write/remove/add buffers
    struct BufferSpace {
        enum {
            BYPASS_CLEAN_CACHE = 0x1,  // bypass clean cache
        };

        typedef boost::intrusive::list<
                Buffer,
                boost::intrusive::member_hook<
                        Buffer,
                        boost::intrusive::list_member_hook<>,
                        &Buffer::state_item> > state_list_t;

        mempool::kvsstore_cache_other::map<uint32_t, std::unique_ptr<Buffer>>
                buffer_map;

        // we use a bare intrusive list here instead of std::map because
        // it uses less memory and we expect this to be very small (very
        // few IOs in flight to the same Blob at the same time).
        state_list_t writing;   ///< writing buffers, sorted by seq, ascending

        ~BufferSpace() {
            ceph_assert(buffer_map.empty());
            ceph_assert(writing.empty());
        }

        bool get_buffer_address(BufferCacheShard* cache, uint32_t offset, void **addr, uint64_t *length);

        void _add_buffer(BufferCacheShard* cache, Buffer *b, int level, Buffer *near) {
            cache->_audit("_add_buffer start");
            buffer_map[b->offset].reset(b);
            if (b->is_writing()) {
                b->data.reassign_to_mempool(mempool::mempool_kvsstore_writing);
                if (writing.empty() || writing.rbegin()->seq <= b->seq) {
                    writing.push_back(*b);
                } else {
                    auto it = writing.begin();
                    while (it->seq < b->seq) {
                        ++it;
                    }

                    ceph_assert(it->seq >= b->seq);
                    // note that this will insert b before it
                    // hence the order is maintained
                    writing.insert(it, *b);
                }
            } else {
                b->data.reassign_to_mempool(mempool::mempool_kvsstore_cache_data);
                cache->_add(b, level, near);
            }
            cache->_audit("_add_buffer end");
        }
        void _rm_buffer(BufferCacheShard* cache, Buffer *b) {
            _rm_buffer(cache, buffer_map.find(b->offset));
        }
        void _rm_buffer(BufferCacheShard* cache,
                        map<uint32_t, std::unique_ptr<Buffer>>::iterator p) {
            ceph_assert(p != buffer_map.end());
            cache->_audit("_rm_buffer start");
            if (p->second->is_writing()) {
                writing.erase(writing.iterator_to(*p->second));
            } else {
                cache->_rm(p->second.get());
            }
            buffer_map.erase(p);
            cache->_audit("_rm_buffer end");
        }

        map<uint32_t,std::unique_ptr<Buffer>>::iterator _data_lower_bound(
                uint32_t offset) {
            auto i = buffer_map.lower_bound(offset);
            if (i != buffer_map.begin()) {
                --i;
                if (i->first + i->second->length <= offset)
                    ++i;
            }
            return i;
        }

        // must be called under protection of the Cache lock
        void _clear(BufferCacheShard* cache);

        // return value is the highest cache_private of a trimmed buffer, or 0.
        int discard(BufferCacheShard* cache, uint32_t offset, uint32_t length) {
            std::lock_guard l(cache->lock);
            int ret = _discard(cache, offset, length);
            cache->_trim();
            return ret;
        }
        int _discard(BufferCacheShard* cache, uint32_t offset, uint32_t length);

        void write(BufferCacheShard* cache, uint64_t seq, uint32_t offset, bufferlist& bl,
                   unsigned flags) {
            std::lock_guard l(cache->lock);
            Buffer *b = new Buffer(this, Buffer::STATE_WRITING, seq, offset, bl,
                                   flags);
            b->cache_private = _discard(cache, offset, bl.length());
            _add_buffer(cache, b, (flags & Buffer::FLAG_NOCACHE) ? 0 : 1, nullptr);
            cache->_trim();
        }
        void _finish_write(BufferCacheShard* cache, uint64_t seq);
        void did_read(BufferCacheShard* cache, uint32_t offset, bufferlist& bl) {
            std::lock_guard l(cache->lock);
            Buffer *b = new Buffer(this, Buffer::STATE_CLEAN, 0, offset, bl);
            b->cache_private = _discard(cache, offset, bl.length());
            _add_buffer(cache, b, 1, nullptr);
            cache->_trim();
        }

        void read(BufferCacheShard* cache, uint32_t offset, uint32_t length,
                  ready_regions_t& res,
                  interval_set<uint32_t>& res_intervals,
                  int flags = 0);

        void truncate(BufferCacheShard* cache, uint32_t offset) {
            discard(cache, offset, (uint32_t)-1 - offset);
        }

        void split(BufferCacheShard* cache, size_t pos, BufferSpace &r);

        void dump(BufferCacheShard* cache, Formatter *f) const {
            std::lock_guard l(cache->lock);
            f->open_array_section("buffers");
            for (auto& i : buffer_map) {
                f->open_object_section("buffer");
                ceph_assert(i.first == i.second->offset);
                i.second->dump(f);
                f->close_section();
            }
            f->close_section();
        }
    };


    struct CacheShard {
        CephContext *cct;
        PerfCounters *logger;

        /// protect lru and other structures
        std::recursive_mutex lock; // = { ceph::make_recursive_mutex("KvsStore::CacheShard::lock") };

        std::atomic<uint64_t> max = { 0 };
        std::atomic<uint64_t> num = { 0 };

        CacheShard(CephContext *cct) : cct(cct), logger(nullptr) {}
        virtual ~CacheShard() {}

        void set_max(uint64_t max_) {
            max = max_;
        }

        uint64_t _get_num() {
            return num;
        }

        virtual void _trim_to(uint64_t max) = 0;
        void _trim() {
            _trim_to(max);
        }
        void trim() {
            std::lock_guard l(lock);
            _trim();
        }
        void flush() {
            std::lock_guard l(lock);
            // we should not be shutting down after the blackhole is enabled
            _trim_to(0);
        }

#ifdef DEBUG_CACHE
        virtual void _audit(const char *s) = 0;
#else
        void _audit(const char *s) { /* no-op */
        }
#endif
    };


    /// A Generic buffer Cache Shard
    struct BufferCacheShard : public CacheShard {
        uint64_t buffer_bytes = 0;

    public:
        BufferCacheShard(CephContext* cct) : CacheShard(cct) {}
        static BufferCacheShard *create(CephContext* cct, string type,
                                        PerfCounters *logger);
        virtual void _add(Buffer *b, int level, Buffer *near) = 0;
        virtual void _rm(Buffer *b) = 0;
        virtual void _move(BufferCacheShard *src, Buffer *b) = 0;
        virtual void _touch(Buffer *b) = 0;
        virtual void _adjust_size(Buffer *b, int64_t delta) = 0;

        uint64_t _get_bytes() {
            return buffer_bytes;
        }

        virtual void add_stats(uint64_t *buffers, uint64_t *bytes) = 0;

        bool empty() {
            std::lock_guard l(lock);
            return _get_bytes() == 0;
        }
    };


    /// A Generic onode Cache Shard
    struct OnodeCacheShard: public CacheShard {
        std::array<std::pair<ghobject_t, mono_clock::time_point>, 64> dumped_onodes;
    public:
        OnodeCacheShard(CephContext *cct) :
                CacheShard(cct) {
        }
        static OnodeCacheShard* create(CephContext *cct, string type,
                                       PerfCounters *logger);
        virtual void _add(OnodeRef &o, int level) = 0;
        virtual void _rm(OnodeRef &o) = 0;
        virtual void _touch(OnodeRef &o) = 0;
        virtual void add_stats(uint64_t *onodes) = 0;

        bool empty() {
            return _get_num() == 0;
        }
    };

    ///------------------------------------------------------------
    /// OnodeSpace
    ///------------------------------------------------------------

    struct OnodeSpace {
        OnodeCacheShard *cache;

    public:
        /// forward lookups
        mempool::kvsstore_cache_other::unordered_map<ghobject_t, OnodeRef> onode_map;

        friend class Collection; // for split_cache()

    public:

        OnodeSpace(OnodeCacheShard *c) : cache(c) {}

        ~OnodeSpace() {
            clear();
        }

        OnodeRef add(const ghobject_t &oid, OnodeRef o);
        OnodeRef lookup(const ghobject_t &o);
        void remove(const ghobject_t &oid) {
            onode_map.erase(oid);
        }
        void rename(OnodeRef &o, const ghobject_t &old_oid,
                    const ghobject_t &new_oid,
                    const mempool::kvsstore_cache_other::string &new_okey);

        void clear();
        bool empty();

        template <int LogLevelV>
        void dump(CephContext *cct);

        /// return true if f true for any item
        bool map_any(std::function<bool(OnodeRef)> f);
    };

    ///  ====================================================
    ///  ONODE
    ///  ====================================================

    struct Onode {
        MEMPOOL_CLASS_HELPERS();

        std::atomic_int nref;  ///< reference count
        Collection *c;

        ghobject_t oid;

        boost::intrusive::list_member_hook<> lru_item;

        kvsstore_onode_t onode;   ///< metadata stored as value in kv store
        bool exists;              ///< true if object logically exists

        BufferSpace bc;           ///< buffer cache

        // track txc's that have not been committed to kv store (and whose
        // effects cannot be read via the kvdb read methods)
        std::atomic<int> flushing_count = {0};
        std::atomic<int> waiting_count = {0};

        std::mutex flush_lock; // = ceph::make_mutex("KvsStore::flush_lock");  ///< protect flush_txns
        std::condition_variable flush_cond;   ///< wait here for uncommitted txns

        Onode(Collection *c, const ghobject_t& o)
                : nref(0), c(c), oid(o), exists(false) {
        }

        void flush();
        void get() {
            ++nref;
        }
        void put() {
            if (--nref == 0) {
                std::lock_guard l(c->cache->lock);
                bc._clear(c->cache);

                delete this;
            }
        }



        static Onode* decode(CollectionRef c, const ghobject_t& oid, const bufferlist& v);
    };

    ///  ====================================================
    ///  Collections
    ///  ====================================================

    struct Collection : public ObjectStore::CollectionImpl {
        KvsStore *store;
        OpSequencerRef osr;
        BufferCacheShard *cache;     ///< our cache shard
        kvsstore_cnode_t cnode;
        std::shared_mutex lock; // =  ceph::make_shared_mutex("KvsStore::Collection::lock", true, false);

        bool exists;

        // cache onodes on a per-collection basis to avoid lock
        // contention.
        OnodeSpace onode_map;

        ContextQueue *commit_queue;

        OnodeRef get_onode(const ghobject_t& oid, bool create, bool is_createop=false);


        void split_cache(Collection *dest);

        //int get_data(TransContext *txc, const ghobject_t& oid, uint64_t offset, size_t length, bufferlist &bl);

        bool contains(const ghobject_t& oid) {
            if (cid.is_meta())
                return oid.hobj.pool == -1;
            spg_t spgid;
            if (cid.is_pg(&spgid))
                return spgid.pgid.contains(cnode.bits, oid) && oid.shard_id == spgid.shard;
            return false;
        }

        int64_t pool() const {
            return cid.pool();
        }

        bool flush_commit(Context *c) override;
        void flush() override;
        void flush_all_but_last();

        Collection(KvsStore *ns, OnodeCacheShard *oc, BufferCacheShard *bc, coll_t c);
    };


    ///  ====================================================
    ///  Transaction Context
    ///  ====================================================

    struct TransContext  {
        //MEMPOOL_CLASS_HELPERS();

        typedef enum {
            STATE_PREPARE = 0,
            STATE_AIO_SUBMITTED,		// submitted data. not yet synced
            STATE_AIO_DONE,
            STATE_FINALIZE,
            STATE_FINISHING,
            STATE_DONE,
        } state_t;

        state_t state = STATE_PREPARE;

        const char *get_state_name() {
            switch (state) {
                case STATE_PREPARE: return "STATE_PREPARE";
                case STATE_AIO_SUBMITTED: return "STATE_AIO_WAIT - submitted IO are done(called by c)";
                case STATE_AIO_DONE: return "STATE_IO_DONE - processing IO done events (called by cb)";
                case STATE_FINALIZE: return "STATE_FINALIZE - added to the finalize queue";
                case STATE_FINISHING: return "STATE_FINISHING - releasing resources for IO (called by cb)";
                case STATE_DONE: return "done";
            }
            return "???";
        }

        CollectionRef ch;
        OpSequencerRef osr; // this should be ch->osr
        boost::intrusive::list_member_hook<> sequencer_item;

        uint64_t bytes = 0, ios = 0;

        set<OnodeRef> onodes;     ///< these need to be updated/written
        set<OnodeRef> modified_objects;  ///< objects we modified (and need a ref)

        list<Context*> 		oncommits;  		 ///< more commit completions
        list<CollectionRef> removed_collections; ///< colls we removed
        std::vector<bufferlist*> omap_data;       /// temporary write buffer for omap
        std::vector<bufferlist*> coll_data;       /// temporary write buffer for collection

        IoContext *ioc;	// I/O operations

        uint64_t seq = 0;

        uint64_t last_nid = 0;     ///< if non-zero, highest new nid we allocated
        void *parent;
        explicit TransContext(void *parent_, CephContext *cct_, Collection *c,  OpSequencer *o, list<Context*> *on_commits)
                : ch(c), osr(o), parent(parent_)
        {
            ioc = new IoContext(this,__func__);

            if (on_commits) {
                oncommits.swap(*on_commits);
            }
        }

        ~TransContext() {
            for (bufferlist* list : omap_data) {
                delete list;
            }
            for (bufferlist* list : coll_data) {
                delete list;
            }

            delete ioc;
        }

        void write_onode(OnodeRef &o) {
            onodes.insert(o);
        }

        /// note we logically modified object (when onode itself is unmodified)
        void note_modified_object(OnodeRef &o) {
            // onode itself isn't written, though
            modified_objects.insert(o);
        }

        void note_removed_object(OnodeRef& o) {
            onodes.erase(o);
            modified_objects.insert(o);
        }

        // callback for data
        void aio_finish(KvsStore *store);
    };


    ///  ====================================================
    ///  OpSequencer
    ///  ====================================================

    class OpSequencer : public RefCountedObject {
    public:
        typedef boost::intrusive::list<TransContext, boost::intrusive::member_hook<TransContext,boost::intrusive::list_member_hook<>,&TransContext::sequencer_item> > q_list_t;

    public:
        std::mutex qlock;// = ceph::make_mutex("KvsStore::OpSequencer::qlock");
        std::condition_variable qcond;

        q_list_t q;  ///< transactions

        KvsStore *store;
        coll_t cid;

        uint64_t last_seq = 0;

        std::atomic_int txc_with_unstable_io = {0};  ///< num txcs with unstable io
        std::atomic_int kv_committing_serially = {0};
        std::atomic_int kv_submitted_waiters = {0};
        std::atomic_int kv_drain_preceding_waiters = {0};

        std::atomic_bool zombie = {false};    ///< owning Sequencer has gone away

        const uint32_t sequencer_id;

        uint32_t get_sequencer_id() const {
            return sequencer_id;
        }

        void queue_new(TransContext *txc) {
            FTRACE
            std::lock_guard<std::mutex> l(qlock);
            txc->seq = ++last_seq;
            q.push_back(*txc);
        }

        void drain() {
            FTRACE
            std::unique_lock<std::mutex> l(qlock);
            while (!q.empty())
                qcond.wait(l);
        }

        void drain_preceding(TransContext *txc) {
            FTRACE
            std::unique_lock<std::mutex> l(qlock);
            while (&q.front() != txc)
                qcond.wait(l);
        }

        bool _is_all_kv_submitted() {
            FTRACE
            // caller must hold qlock & q.empty() must not empty
            ceph_assert(!q.empty());
            TransContext *txc = &q.back();
            if (txc->state >= TransContext::STATE_AIO_SUBMITTED) {
                return true;
            }
            return false;
        }

        void flush() {
            FTRACE
            std::unique_lock<std::mutex> l(qlock);
            while (true) {
                // set flag before the check because the condition
                // may become true outside qlock, and we need to make
                // sure those threads see waiters and signal qcond.
                ++kv_submitted_waiters;
                if (q.empty() || _is_all_kv_submitted()) {
                    --kv_submitted_waiters;
                    return;
                }
                qcond.wait(l);
                --kv_submitted_waiters;
            }
        }

        void flush_all_but_last() {
            FTRACE
            std::unique_lock<std::mutex> l(qlock);
            assert (q.size() >= 1);
            while (true) {
                // set flag before the check because the condition
                // may become true outside qlock, and we need to make
                // sure those threads see waiters and signal qcond.
                ++kv_submitted_waiters;
                if (q.size() <= 1) {
                    --kv_submitted_waiters;
                    return;
                } else {
                    auto it = q.rbegin();
                    it++;
                    if (it->state >= TransContext::STATE_AIO_SUBMITTED) {
                        --kv_submitted_waiters;
                        return;
                    }
                }
                qcond.wait(l);
                --kv_submitted_waiters;
            }
        }


        bool flush_commit(Context *c) {
            FTRACE
            std::lock_guard<std::mutex> l(qlock);
            if (q.empty()) {
                return true;
            }
            TransContext *txc = &q.back();
            if (txc->state >= TransContext::STATE_AIO_DONE) {
                return true;
            }
            txc->oncommits.push_back(c);
            return false;
        }


        FRIEND_MAKE_REF(OpSequencer);
        OpSequencer(KvsStore *store, uint32_t sequencer_id, const coll_t &c);

        ~OpSequencer() {
            FTRACE
            ceph_assert(q.empty());
        }
    };
};

WRITE_CLASS_DENC(kvsstore_sb_t)
WRITE_CLASS_DENC(kvsstore_onode_t)
WRITE_CLASS_DENC(kvsstore_cnode_t)

static inline void intrusive_ptr_add_ref(KvsStoreTypes::Onode *o) {
    o->get();
}

static inline void intrusive_ptr_release(KvsStoreTypes::Onode *o) {
    o->put();
}
static inline void intrusive_ptr_add_ref(KvsStoreTypes::OpSequencer *o) {
    o->get();
    //TR << "op sequencer add nref = " << o->get_nref();
}

static inline void intrusive_ptr_release(KvsStoreTypes::OpSequencer *o) {
    //TR << "op sequencer release - nref = " << o->get_nref() -1;
    o->put();

}




#endif /* SRC_OS_KVSSTORE_KVSSTORE_TYPES_H_ */

