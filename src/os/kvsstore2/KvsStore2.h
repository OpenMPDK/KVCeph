#ifndef CEPH_OSD_KVSSTORE_H
#define CEPH_OSD_KVSSTORE_H

#include "acconfig.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <ratio>
#include <mutex>
#include <condition_variable>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/circular_buffer.hpp>

#include "include/cpp-btree/btree_set.h"

#include "include/ceph_assert.h"
#include "include/unordered_map.h"
#include "include/mempool.h"
#include "include/hash.h"
#include "common/bloom_filter.hpp"
#include "common/Finisher.h"
#include "common/ceph_mutex.h"
#include "common/Throttle.h"
#include "common/perf_counters.h"
#include "common/PriorityCache.h"
#include "compressor/Compressor.h"
#include "os/ObjectStore.h"

#include "kvsstore_types.h"
#include "KVDevice.h"
#include "common/EventTrace.h"

// constants for Buffer::optimize()
#define MAX_BUFFER_SLOP_RATIO_DEN  8  // so actually 1/N

enum {
    l_kvsstore_first = 932430,
    l_kvsstore_onode_misses,
    l_kvsstore_onode_hits,
    l_kvsstore_last
};

#define META_POOL_ID ((uint64_t)-1ull)

class KvsStore : public ObjectStore, public md_config_obs_t {
    // -----------------------------------------------------
    // types

public:
    // md_config_obs_t interface - config observer
    const char** get_tracked_conf_keys() const override;
    void handle_conf_change(const ConfigProxy& conf,
                            const std::set<std::string> &changed) override;

public:

    //handler for discard event
    void handle_discard(interval_set<uint64_t>& to_release);

    class TransContext;

    typedef map<uint64_t, bufferlist> ready_regions_t;

    struct BufferSpace;
    struct Collection;
    typedef boost::intrusive_ptr<Collection> CollectionRef;

    struct AioContext {
        virtual void aio_finish(KvsStore *store) = 0;
        virtual ~AioContext() {}
    };

    /// cached buffer
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

    struct BufferCacheShard;

    /// map logical extent range (object) onto buffers
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
                  KvsStore::ready_regions_t& res,
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



    struct Onode;


    struct OnodeSpace;

    /// an in-memory object
    struct Onode {
        MEMPOOL_CLASS_HELPERS();

        std::atomic_int nref;  ///< reference count
        Collection *c;

        ghobject_t oid;

        /// key under PREFIX_OBJ where we are stored
        mempool::kvsstore_cache_other::string key;

        boost::intrusive::list_member_hook<> lru_item;

        kvsstore_onode_t onode;  ///< metadata stored as value in kv store
        bool exists;              ///< true if object logically exists

        // track txc's that have not been committed to kv store (and whose
        // effects cannot be read via the kvdb read methods)
        std::atomic<int> flushing_count = {0};
        std::atomic<int> waiting_count = {0};
        /// protect flush_txns
        ceph::mutex flush_lock = ceph::make_mutex("KvsStore::Onode::flush_lock");
        ceph::condition_variable flush_cond;   ///< wait here for uncommitted txns

        Onode(Collection *c, const ghobject_t& o,
              const mempool::kvsstore_cache_other::string& k)
                : nref(0),
                  c(c),
                  oid(o),
                  key(k),
                  exists(false) {
        }
        Onode(Collection* c, const ghobject_t& o,
              const string& k)
                : nref(0),
                  c(c),
                  oid(o),
                  key(k),
                  exists(false) {
        }
        Onode(Collection* c, const ghobject_t& o,
              const char* k)
                : nref(0),
                  c(c),
                  oid(o),
                  key(k),
                  exists(false) {
        }

        static Onode* decode(
                CollectionRef c,
                const ghobject_t& oid,
                const string& key,
                const bufferlist& v);

        void dump(Formatter* f) const;

        void flush();
        void get() {
            ++nref;
        }
        void put() {
            if (--nref == 0)
                delete this;
        }

        const string& get_omap_prefix();
        void get_omap_header(string *out);
        void get_omap_key(const string& key, string *out);
        void rewrite_omap_key(const string& old, string *out);
        void get_omap_tail(string *out);
        void decode_omap_key(const string& key, string *user_key);
    };
    typedef boost::intrusive_ptr<Onode> OnodeRef;

    /// A generic Cache Shard
    struct CacheShard {
        CephContext *cct;
        PerfCounters *logger;

        /// protect lru and other structures
        ceph::recursive_mutex lock = {
                ceph::make_recursive_mutex("KvsStore::CacheShard::lock") };

        std::atomic<uint64_t> max = {0};
        std::atomic<uint64_t> num = {0};

        CacheShard(CephContext* cct) : cct(cct), logger(nullptr) {}
        virtual ~CacheShard() {}

        void set_max(uint64_t max_) {
            max = max_;
        }

        uint64_t _get_num() {
            return num;
        }

        virtual void _trim_to(uint64_t max) = 0;
        void _trim() {
            if (cct->_conf->objectstore_blackhole) {
                // do not trim if we are throwing away IOs a layer down
                return;
            }
            _trim_to(max);
        }
        void trim() {
            std::lock_guard l(lock);
            _trim();
        }
        void flush() {
            std::lock_guard l(lock);
            // we should not be shutting down after the blackhole is enabled
            assert(!cct->_conf->objectstore_blackhole);
            _trim_to(0);
        }

#ifdef DEBUG_CACHE
        virtual void _audit(const char *s) = 0;
#else
        void _audit(const char *s) { /* no-op */ }
#endif
    };

    /// A Generic onode Cache Shard
    struct OnodeCacheShard : public CacheShard {
        std::array<std::pair<ghobject_t, mono_clock::time_point>, 64> dumped_onodes;
    public:
        OnodeCacheShard(CephContext* cct) : CacheShard(cct) {}
        static OnodeCacheShard *create(CephContext* cct, string type,
                                       PerfCounters *logger);
        virtual void _add(OnodeRef& o, int level) = 0;
        virtual void _rm(OnodeRef& o) = 0;
        virtual void _touch(OnodeRef& o) = 0;
        virtual void add_stats(uint64_t *onodes) = 0;

        bool empty() {
            return _get_num() == 0;
        }
    };

    /// A Generic buffer Cache Shard
    struct BufferCacheShard : public CacheShard {
        std::atomic<uint64_t> num_extents = {0};
        std::atomic<uint64_t> num_blobs = {0};
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

        void add_extent() {
            ++num_extents;
        }
        void rm_extent() {
            --num_extents;
        }

        void add_blob() {
            ++num_blobs;
        }
        void rm_blob() {
            --num_blobs;
        }

        virtual void add_stats(uint64_t *extents,
                               uint64_t *blobs,
                               uint64_t *buffers,
                               uint64_t *bytes) = 0;

        bool empty() {
            std::lock_guard l(lock);
            return _get_bytes() == 0;
        }
    };

    struct OnodeSpace {
        OnodeCacheShard *cache;

    private:
        /// forward lookups
        mempool::kvsstore_cache_other::unordered_map<ghobject_t,OnodeRef> onode_map;

        friend class Collection; // for split_cache()

    public:
        OnodeSpace(OnodeCacheShard *c) : cache(c) {}
        ~OnodeSpace() {
            clear();
        }

        OnodeRef add(const ghobject_t& oid, OnodeRef o);
        OnodeRef lookup(const ghobject_t& o);
        void remove(const ghobject_t& oid) {
            onode_map.erase(oid);
        }
        void rename(OnodeRef& o, const ghobject_t& old_oid,
                    const ghobject_t& new_oid,
                    const mempool::kvsstore_cache_other::string& new_okey);
        void clear();
        bool empty();

        template <int LogLevelV>
        void dump(CephContext *cct);

        /// return true if f true for any item
        bool map_any(std::function<bool(OnodeRef)> f);
    };

    class OpSequencer;
    using OpSequencerRef = ceph::ref_t<OpSequencer>;

    struct Collection : public CollectionImpl {
        KvsStore *store;
        OpSequencerRef osr;
        BufferCacheShard *cache;       ///< our cache shard
        kvsstore_cnode_t cnode;
        ceph::shared_mutex lock =
                ceph::make_shared_mutex("KvsStore::Collection::lock", true, false);

        bool exists;

        // cache onodes on a per-collection basis to avoid lock
        // contention.
        OnodeSpace onode_map;

        //pool options
        pool_opts_t pool_opts;
        ContextQueue *commit_queue;

        OnodeRef get_onode(const ghobject_t& oid, bool create, bool is_createop=false);

        bool contains(const ghobject_t& oid) {
            if (cid.is_meta())
                return oid.hobj.pool == -1;
            spg_t spgid;
            if (cid.is_pg(&spgid))
                return
                        spgid.pgid.contains(cnode.bits, oid) &&
                        oid.shard_id == spgid.shard;
            return false;
        }

        int64_t pool() const {
            return cid.pool();
        }

        void split_cache(Collection *dest);

        bool flush_commit(Context *c) override;
        void flush() override;
        void flush_all_but_last();

        Collection(KvsStore *ns, OnodeCacheShard *oc, BufferCacheShard *bc, coll_t c);
    };


    class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
        CollectionRef c;
        OnodeRef o;
        map<string,bufferlist> omap;
        map<string,bufferlist>::iterator it;
    public:
        OmapIteratorImpl(KvsStore *store, CollectionRef c, OnodeRef o): c(c), o(o) {
            /*o->onode.fill_omap_values(&omap, [&] (const std::string &key, bufferlist &bl)->int {
                return store->db.read_omap(o->oid, o->onode.lid, key, bl);
            });*/
            it = omap.begin();
        }

        int seek_to_first() override {
            it = omap.begin();
            return 0;
        }
        int upper_bound(const string &after) override {
            it = omap.upper_bound(after);
            return 0;
        }
        int lower_bound(const string &to) override {
            it = omap.lower_bound(to);
            return 0;
        }
        bool valid() override {
            return it != omap.end();
        }
        int next() override {
            ++it;
            return 0;
        }
        string key() override {
            return it->first;
        }
        bufferlist value() override {
            return it->second;
        }
        int status() override {
            return 0;
        }
    };


    struct volatile_statfs{
        enum {
            STATFS_ALLOCATED = 0,
            STATFS_STORED,
            STATFS_COMPRESSED_ORIGINAL,
            STATFS_COMPRESSED,
            STATFS_COMPRESSED_ALLOCATED,
            STATFS_LAST
        };
        int64_t values[STATFS_LAST];
        volatile_statfs() {
            memset(this, 0, sizeof(volatile_statfs));
        }
        void reset() {
            *this = volatile_statfs();
        }
        void publish(store_statfs_t* buf) const {
            buf->allocated = allocated();
            buf->data_stored = stored();
            buf->data_compressed = compressed();
            buf->data_compressed_original = compressed_original();
            buf->data_compressed_allocated = compressed_allocated();
        }

        volatile_statfs& operator+=(const volatile_statfs& other) {
            for (size_t i = 0; i < STATFS_LAST; ++i) {
                values[i] += other.values[i];
            }
            return *this;
        }
        int64_t& allocated() {
            return values[STATFS_ALLOCATED];
        }
        int64_t& stored() {
            return values[STATFS_STORED];
        }
        int64_t& compressed_original() {
            return values[STATFS_COMPRESSED_ORIGINAL];
        }
        int64_t& compressed() {
            return values[STATFS_COMPRESSED];
        }
        int64_t& compressed_allocated() {
            return values[STATFS_COMPRESSED_ALLOCATED];
        }
        int64_t allocated() const {
            return values[STATFS_ALLOCATED];
        }
        int64_t stored() const {
            return values[STATFS_STORED];
        }
        int64_t compressed_original() const {
            return values[STATFS_COMPRESSED_ORIGINAL];
        }
        int64_t compressed() const {
            return values[STATFS_COMPRESSED];
        }
        int64_t compressed_allocated() const {
            return values[STATFS_COMPRESSED_ALLOCATED];
        }
        volatile_statfs& operator=(const store_statfs_t& st) {
            values[STATFS_ALLOCATED] = st.allocated;
            values[STATFS_STORED] = st.data_stored;
            values[STATFS_COMPRESSED_ORIGINAL] = st.data_compressed_original;
            values[STATFS_COMPRESSED] = st.data_compressed;
            values[STATFS_COMPRESSED_ALLOCATED] = st.data_compressed_allocated;
            return *this;
        }
        bool is_empty() {
            return values[STATFS_ALLOCATED] == 0 &&
                   values[STATFS_STORED] == 0 &&
                   values[STATFS_COMPRESSED] == 0 &&
                   values[STATFS_COMPRESSED_ORIGINAL] == 0 &&
                   values[STATFS_COMPRESSED_ALLOCATED] == 0;
        }
        void decode(bufferlist::const_iterator& it) {
            using ceph::decode;
            for (size_t i = 0; i < STATFS_LAST; i++) {
                decode(values[i], it);
            }
        }

        void encode(bufferlist& bl) {
            using ceph::encode;
            for (size_t i = 0; i < STATFS_LAST; i++) {
                encode(values[i], bl);
            }
        }
    };

    struct TransContext final : public AioContext {
        MEMPOOL_CLASS_HELPERS();

        typedef enum {
            STATE_PREPARE,
            STATE_AIO_WAIT,
            STATE_IO_DONE,
            STATE_KV_QUEUED,     // queued for kv_sync_thread submission
            STATE_KV_SUBMITTED,  // submitted to kv; not yet synced
            STATE_KV_DONE,
            STATE_FINISHING,
            STATE_DONE,
        } state_t;

        state_t state = STATE_PREPARE;

        const char *get_state_name() {
            switch (state) {
                case STATE_PREPARE: return "prepare";
                case STATE_AIO_WAIT: return "aio_wait";
                case STATE_IO_DONE: return "io_done";
                case STATE_KV_QUEUED: return "kv_queued";
                case STATE_KV_SUBMITTED: return "kv_submitted";
                case STATE_KV_DONE: return "kv_done";
                case STATE_FINISHING: return "finishing";
                case STATE_DONE: return "done";
            }
            return "???";
        }

        CollectionRef ch;
        OpSequencerRef osr;  // this should be ch->osr
        boost::intrusive::list_member_hook<> sequencer_item;

        uint64_t bytes = 0, ios = 0, cost = 0;

        set<OnodeRef> onodes;     ///< these need to be updated/written
        set<OnodeRef> modified_objects;  ///< objects we modified (and need a ref)

        KeyValueDB::Transaction t; ///< then we will commit this
        list<Context*> oncommits;  ///< more commit completions
        list<CollectionRef> removed_collections; ///< colls we removed

        interval_set<uint64_t> allocated, released;
        volatile_statfs statfs_delta;	   ///< overall store statistics delta
        uint64_t osd_pool_id = META_POOL_ID;    ///< osd pool id we're operating on

        IOContext ioc;
        bool had_ios = false;  ///< true if we submitted IOs before our kv txn

        uint64_t seq = 0;
        utime_t start;
        utime_t last_stamp;

        uint64_t last_nid = 0;     ///< if non-zero, highest new nid we allocated
        uint64_t last_blobid = 0;  ///< if non-zero, highest new blobid we allocated

        explicit TransContext(CephContext* cct, Collection *c, OpSequencer *o,
                              list<Context*> *on_commits)
                : ch(c),
                  osr(o),
                  ioc(cct, this),
                  start(ceph_clock_now()) {
            last_stamp = start;
            if (on_commits) {
                oncommits.swap(*on_commits);
            }
        }
        ~TransContext() {
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

        void aio_finish(KvsStore *store) override {
            store->txc_aio_finish(this);
        }
    };

    class OpSequencer : public RefCountedObject {
    public:
        ceph::mutex qlock = ceph::make_mutex("KvsStore::OpSequencer::qlock");
        ceph::condition_variable qcond;
        typedef boost::intrusive::list<
                TransContext,
                boost::intrusive::member_hook<
                        TransContext,
                        boost::intrusive::list_member_hook<>,
                        &TransContext::sequencer_item> > q_list_t;

        q_list_t q;  ///< transactions

        KvsStore *store;
        coll_t cid;

        uint64_t last_seq = 0;

        std::atomic_int txc_with_unstable_io = {0};  ///< num txcs with unstable io

        std::atomic_int kv_committing_serially = {0};

        std::atomic_int kv_submitted_waiters = {0};

        std::atomic_int kv_drain_preceding_waiters = {0};

        std::atomic_bool zombie = {false};    ///< in zombie_osr set (collection going away)

        const uint32_t sequencer_id;

        uint32_t get_sequencer_id() const {
            return sequencer_id;
        }

        void queue_new(TransContext *txc) {
            std::lock_guard l(qlock);
            txc->seq = ++last_seq;
            q.push_back(*txc);
        }

        void drain() {
            std::unique_lock l(qlock);
            while (!q.empty())
                qcond.wait(l);
        }

        void drain_preceding(TransContext *txc) {
            std::unique_lock l(qlock);
            while (&q.front() != txc)
                qcond.wait(l);
        }

        bool _is_all_kv_submitted() {
            // caller must hold qlock & q.empty() must not empty
            ceph_assert(!q.empty());
            TransContext *txc = &q.back();
            if (txc->state >= TransContext::STATE_KV_SUBMITTED) {
                return true;
            }
            return false;
        }

        void flush() {
            std::unique_lock l(qlock);
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
            std::unique_lock l(qlock);
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
                    if (it->state >= TransContext::STATE_KV_SUBMITTED) {
                        --kv_submitted_waiters;
                        return;
                    }
                }
                qcond.wait(l);
                --kv_submitted_waiters;
            }
        }

        bool flush_commit(Context *c) {
            std::lock_guard l(qlock);
            if (q.empty()) {
                return true;
            }
            TransContext *txc = &q.back();
            if (txc->state >= TransContext::STATE_KV_DONE) {
                return true;
            }
            txc->oncommits.push_back(c);
            return false;
        }
    private:
        FRIEND_MAKE_REF(OpSequencer);
        OpSequencer(KvsStore *store, uint32_t sequencer_id, const coll_t& c)
                : RefCountedObject(store->cct),
                  store(store), cid(c), sequencer_id(sequencer_id) {
        }
        ~OpSequencer() {
            ceph_assert(q.empty());
        }
    };

    struct KVSyncThread : public Thread {
        KvsStore *store;
        explicit KVSyncThread(KvsStore *s) : store(s) {}
        void *entry() override {
            store->_kv_sync_thread();
            return NULL;
        }
    };

    struct KVFinalizeThread : public Thread {
        KvsStore *store;
        explicit KVFinalizeThread(KvsStore *s) : store(s) {}
        void *entry() {
            store->_kv_finalize_thread();
            return NULL;
        }
    };

    // --------------------------------------------------------
    // members
private:

    KeyValueDB *db = nullptr;
    //KVDevice *bdev = nullptr;
    std::string freelist_type;
    uuid_d fsid;
    int path_fd = -1;  ///< open handle to $path
    int fsid_fd = -1;  ///< open handle (locked) to $path/fsid
    bool mounted = false;

    ceph::shared_mutex coll_lock = ceph::make_shared_mutex("KvsStore::coll_lock");  ///< rwlock to protect coll_map
    mempool::kvsstore_cache_other::unordered_map<coll_t, CollectionRef> coll_map;
    bool collections_had_errors = false;
    map<coll_t,CollectionRef> new_coll_map;

    vector<OnodeCacheShard*> onode_cache_shards;
    vector<BufferCacheShard*> buffer_cache_shards;

    /// protect zombie_osr_set
    ceph::mutex zombie_osr_lock = ceph::make_mutex("KvsStore::zombie_osr_lock");
    uint32_t next_sequencer_id = 0;
    std::map<coll_t,OpSequencerRef> zombie_osr_set; ///< set of OpSequencers for deleted collections

    Finisher  finisher;

    KVSyncThread kv_sync_thread;
    ceph::mutex kv_lock = ceph::make_mutex("KvsStore::kv_lock");
    ceph::condition_variable kv_cond;
    bool _kv_only = false;
    bool kv_sync_started = false;
    bool kv_stop = false;
    bool kv_finalize_started = false;
    bool kv_finalize_stop = false;
    deque<TransContext*> kv_queue;             ///< ready, already submitted
    deque<TransContext*> kv_queue_unsubmitted; ///< ready, need submit by kv thread
    deque<TransContext*> kv_committing;        ///< currently syncing
    bool kv_sync_in_progress = false;

    KVFinalizeThread kv_finalize_thread;
    ceph::mutex kv_finalize_lock = ceph::make_mutex("KvsStore::kv_finalize_lock");
    ceph::condition_variable kv_finalize_cond;
    deque<TransContext*> kv_committing_to_finalize;   ///< pending finalization
    bool kv_finalize_in_progress = false;

    PerfCounters *logger = nullptr;

    list<CollectionRef> removed_collections;

    uint64_t block_size = 0;     ///< block size of block device (power of 2)
    uint64_t block_mask = 0;     ///< mask to get just the block offset
    size_t block_size_order = 0; ///< bits to shift to get block size

    bool per_pool_omap = false;

    uint64_t kv_ios = 0;
    uint64_t kv_throttle_costs = 0;

    // cache trim control
    uint64_t cache_size = 0;       ///< total cache size
    double cache_meta_ratio = 0;   ///< cache ratio dedicated to metadata
    double cache_kv_ratio = 0;     ///< cache ratio dedicated to kv (e.g., rocksdb)
    double cache_data_ratio = 0;   ///< cache ratio dedicated to object data
    bool cache_autotune = false;   ///< cache autotune setting
    double cache_autotune_interval = 0; ///< time to wait between cache rebalancing
    uint64_t osd_memory_target = 0;   ///< OSD memory target when autotuning cache
    uint64_t osd_memory_base = 0;     ///< OSD base memory when autotuning cache
    double osd_memory_expected_fragmentation = 0; ///< expected memory fragmentation
    uint64_t osd_memory_cache_min = 0; ///< Min memory to assign when autotuning cache
    double osd_memory_cache_resize_interval = 0; ///< Time to wait between cache resizing
    double max_defer_interval = 0; ///< Time to wait between last deferred submit
    std::atomic<uint32_t> config_changed = {0}; ///< Counter to determine if there is a configuration change.

    typedef map<uint64_t, volatile_statfs> osd_pools_map;

    ceph::mutex vstatfs_lock = ceph::make_mutex("KvsStore::vstatfs_lock");
    volatile_statfs vstatfs;
    osd_pools_map osd_pools; // protected by vstatfs_lock as well

    bool per_pool_stat_collection = true;

    struct MempoolThread : public Thread {
    public:
        KvsStore *store;

        ceph::condition_variable cond;
        ceph::mutex lock = ceph::make_mutex("KvsStore::MempoolThread::lock");
        bool stop = false;
        std::shared_ptr<PriorityCache::PriCache> binned_kv_cache = nullptr;
        std::shared_ptr<PriorityCache::Manager> pcm = nullptr;

        struct MempoolCache : public PriorityCache::PriCache {
            KvsStore *store;
            int64_t cache_bytes[PriorityCache::Priority::LAST+1] = {0};
            int64_t committed_bytes = 0;
            double cache_ratio = 0;

            MempoolCache(KvsStore *s) : store(s) {};

            virtual uint64_t _get_used_bytes() const = 0;

            virtual int64_t request_cache_bytes(
                    PriorityCache::Priority pri, uint64_t total_cache) const {
                int64_t assigned = get_cache_bytes(pri);

                switch (pri) {
                    // All cache items are currently shoved into the PRI1 priority
                    case PriorityCache::Priority::PRI1:
                    {
                        int64_t request = _get_used_bytes();
                        return(request > assigned) ? request - assigned : 0;
                    }
                    default:
                        break;
                }
                return -EOPNOTSUPP;
            }

            virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const {
                return cache_bytes[pri];
            }
            virtual int64_t get_cache_bytes() const {
                int64_t total = 0;

                for (int i = 0; i < PriorityCache::Priority::LAST + 1; i++) {
                    PriorityCache::Priority pri = static_cast<PriorityCache::Priority>(i);
                    total += get_cache_bytes(pri);
                }
                return total;
            }
            virtual void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
                cache_bytes[pri] = bytes;
            }
            virtual void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
                cache_bytes[pri] += bytes;
            }
            virtual int64_t commit_cache_size(uint64_t total_cache) {
                committed_bytes = PriorityCache::get_chunk(
                        get_cache_bytes(), total_cache);
                return committed_bytes;
            }
            virtual int64_t get_committed_size() const {
                return committed_bytes;
            }
            virtual double get_cache_ratio() const {
                return cache_ratio;
            }
            virtual void set_cache_ratio(double ratio) {
                cache_ratio = ratio;
            }
            virtual string get_cache_name() const = 0;
        };

        struct MetaCache : public MempoolCache {
            MetaCache(KvsStore *s) : MempoolCache(s) {};

            virtual uint64_t _get_used_bytes() const {
                return mempool::kvsstore_cache_other::allocated_bytes() +
                       mempool::kvsstore_cache_onode::allocated_bytes();
            }

            virtual string get_cache_name() const {
                return "KvsStore Meta Cache";
            }

            uint64_t _get_num_onodes() const {
                uint64_t onode_num =
                        mempool::kvsstore_cache_onode::allocated_items();
                return (2 > onode_num) ? 2 : onode_num;
            }

            double get_bytes_per_onode() const {
                return (double)_get_used_bytes() / (double)_get_num_onodes();
            }
        };
        std::shared_ptr<MetaCache> meta_cache;

        struct DataCache : public MempoolCache {
            DataCache(KvsStore *s) : MempoolCache(s) {};

            virtual uint64_t _get_used_bytes() const {
                uint64_t bytes = 0;
                for (auto i : store->buffer_cache_shards) {
                    bytes += i->_get_bytes();
                }
                return bytes;
            }
            virtual string get_cache_name() const {
                return "KvsStore Data Cache";
            }
        };
        std::shared_ptr<DataCache> data_cache;

    public:
        explicit MempoolThread(KvsStore *s)
                : store(s),
                  meta_cache(new MetaCache(s)),
                  data_cache(new DataCache(s)) {}

        void *entry() override;
        void init() {
            ceph_assert(stop == false);
            create("bstore_mempool");
        }
        void shutdown() {
            lock.lock();
            stop = true;
            cond.notify_all();
            lock.unlock();
            join();
        }

    private:
        void _adjust_cache_settings();
        void _update_cache_settings();
        void _resize_shards(bool interval_stats);
    } mempool_thread;

    // --------------------------------------------------------
    // private methods

    void _init_logger();
    void _shutdown_logger();
    int _reload_logger();

    int _open_path();
    void _close_path();
    int _open_fsid(bool create);
    int _lock_fsid();
    int _read_fsid(uuid_d *f);
    int _write_fsid();
    void _close_fsid();
    void _set_alloc_sizes();
    void _set_blob_size();
    void _set_finisher_num();
    void _update_osd_memory_options();

    /*
    * opens both DB and dependant super_meta, FreelistManager and allocator
    * in the proper order
    */
    int _open_db_and_around(bool read_only);
    void _close_db_and_around();

    /*
     * @warning to_repair_db means that we open this db to repair it, will not
     * hold the rocksdb's file lock.
     */
    int _open_db(bool create,
                 bool to_repair_db=false,
                 bool read_only = false);
    void _close_db();
    int _open_fm(KeyValueDB::Transaction t);
    void _close_fm();
    int _open_alloc();
    void _close_alloc();
    int _open_collections();
    void _fsck_collections(int64_t* errors);
    void _close_collections();

    int _setup_block_symlink_or_file(string name, string path, uint64_t size,
                                     bool create);


private:
    int _check_or_set_bdev_label(string path, uint64_t size, string desc,
                                 bool create);

    int _open_super_meta();

    void _open_statfs();
    void _get_statfs_overall(struct store_statfs_t *buf);

    void _dump_alloc_on_failure();

    int64_t _get_bluefs_size_delta(uint64_t bluefs_free, uint64_t bluefs_total);
    int _balance_bluefs_freespace();

    CollectionRef _get_collection(const coll_t& cid);
    void _queue_reap_collection(CollectionRef& c);
    void _reap_collections();
    void _update_cache_logger();

    void _assign_nid(TransContext *txc, OnodeRef o);
    uint64_t _assign_blobid(TransContext *txc);

    template <int LogLevelV>
    friend void _dump_onode(CephContext *cct, const Onode& o);
    template <int LogLevelV>
    friend void _dump_transaction(CephContext *cct, Transaction *t);

    TransContext *_txc_create(Collection *c, OpSequencer *osr,
                              list<Context*> *on_commits);
    void _txc_update_store_statfs(TransContext *txc);
    void _txc_add_transaction(TransContext *txc, Transaction *t);
    void _txc_calc_cost(TransContext *txc);
    void _txc_write_nodes(TransContext *txc, KeyValueDB::Transaction t);
    void _txc_state_proc(TransContext *txc);
    void _txc_aio_submit(TransContext *txc);
public:
    void txc_aio_finish(void *p) {
        _txc_state_proc(static_cast<TransContext*>(p));
    }
private:
    void _txc_finish_io(TransContext *txc);
    void _txc_finalize_kv(TransContext *txc, KeyValueDB::Transaction t);
    void _txc_apply_kv(TransContext *txc, bool sync_submit_transaction);
    void _txc_committed_kv(TransContext *txc);
    void _txc_finish(TransContext *txc);
    void _txc_release_alloc(TransContext *txc);

    void _osr_attach(Collection *c);
    void _osr_register_zombie(OpSequencer *osr);
    void _osr_drain(OpSequencer *osr);
    void _osr_drain_preceding(TransContext *txc);
    void _osr_drain_all();

    void _kv_start();
    void _kv_stop();
    void _kv_sync_thread();
    void _kv_finalize_thread();

public:
    using mempool_dynamic_bitset =
    boost::dynamic_bitset<uint64_t,
            mempool::kvsstore_fsck::pool_allocator<uint64_t>>;
    using  per_pool_statfs =
    mempool::kvsstore_fsck::map<uint64_t, store_statfs_t>;

    enum FSCKDepth {
        FSCK_REGULAR,
        FSCK_DEEP,
        FSCK_SHALLOW
    };

private:

    int _fsck(FSCKDepth depth, bool repair);
    int _fsck_on_open(KvsStore::FSCKDepth depth, bool repair);

    int _collection_list(
            Collection *c, const ghobject_t& start, const ghobject_t& end,
            int max, vector<ghobject_t> *ls, ghobject_t *next);

    template <typename T, typename F>
    T select_option(const std::string& opt_name, T val1, F f) {
        //NB: opt_name reserved for future use
        boost::optional<T> val2 = f();
        if (val2) {
            return *val2;
        }
        return val1;
    }

    void _apply_padding(uint64_t head_pad,
                        uint64_t tail_pad,
                        bufferlist& padded);

    void _record_onode(OnodeRef &o, KeyValueDB::Transaction &txn);

    // --- public interface ---
public:
    KvsStore(CephContext *cct, const string& path);
    KvsStore(CephContext *cct, const string& path, uint64_t min_alloc_size); // Ctor for UT only
    ~KvsStore() override;

    string get_type() override {
        return "kvsstore";
    }

    bool needs_journal() override { return false; };
    bool wants_journal() override { return false; };
    bool allows_journal() override { return false; };

    uint64_t get_min_alloc_size() const override {
        return 0;
    }

    int get_devices(set<string> *ls) override;

    bool is_rotational() override;
    bool is_journal_rotational() override;

    string get_default_device_class() override {
        return "ssd";
    }

    static int get_block_device_fsid(CephContext* cct, const string& path,
                                     uuid_d *fsid);

    bool test_mount_in_use() override;

private:
    int _mount(bool kv_only, bool open_db=true);
public:
    int mount() override {
        return _mount(false);
    }
    int umount() override;

    int start_kv_only(KeyValueDB **pdb, bool open_db=true) {
        int r = _mount(true, open_db);
        if (r < 0)
            return r;
        *pdb = db;
        return 0;
    }

    int write_meta(const std::string& key, const std::string& value) override;
    int read_meta(const std::string& key, std::string *value) override;

    int cold_open();
    int cold_close();

    int fsck(bool deep) override {
        return _fsck(deep ? FSCK_DEEP : FSCK_REGULAR, false);
    }
    int repair(bool deep) override {
        return _fsck(deep ? FSCK_DEEP : FSCK_REGULAR, true);
    }
    int quick_fix() override {
        return _fsck(FSCK_SHALLOW, true);
    }

    void set_cache_shards(unsigned num) override;
    void dump_cache_stats(Formatter *f) override {
        int onode_count = 0, buffers_bytes = 0;
        for (auto i: onode_cache_shards) {
            onode_count += i->_get_num();
        }
        for (auto i: buffer_cache_shards) {
            buffers_bytes += i->_get_bytes();
        }
        f->dump_int("kvsstore_onode", onode_count);
        f->dump_int("kvsstore_buffers", buffers_bytes);
    }
    void dump_cache_stats(ostream& ss) override {
        int onode_count = 0, buffers_bytes = 0;
        for (auto i: onode_cache_shards) {
            onode_count += i->_get_num();
        }
        for (auto i: buffer_cache_shards) {
            buffers_bytes += i->_get_bytes();
        }
        ss << "kvsstore_onode: " << onode_count;
        ss << "kvsstore_buffers: " << buffers_bytes;
    }

    int validate_hobject_key(const hobject_t &obj) const override {
        return 0;
    }
    unsigned get_max_attr_name_length() override {
        return 256;  // arbitrary; there is no real limit internally
    }

    int mkfs() override;
    int mkjournal() override {
        return 0;
    }

    void get_db_statistics(Formatter *f) override;
    void generate_db_histogram(Formatter *f) override;
    void _flush_cache();
    int flush_cache(ostream *os = NULL) override;
    void dump_perf_counters(Formatter *f) override {
        f->open_object_section("perf_counters");
        logger->dump_formatted(f, false);
        f->close_section();
    }

    int add_new_bluefs_device(int id, const string& path);
    int migrate_to_existing_bluefs_device(const set<int>& devs_source,
                                          int id);
    int migrate_to_new_bluefs_device(const set<int>& devs_source,
                                     int id,
                                     const string& path);
    int expand_devices(ostream& out);
    string get_device_path(unsigned id);

public:
    int statfs(struct store_statfs_t *buf,
               osd_alert_list_t* alerts = nullptr) override;
    int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
                    bool *per_pool_omap) override;

    void collect_metadata(map<string,string> *pm) override;

    bool exists(CollectionHandle &c, const ghobject_t& oid) override;
    int set_collection_opts(
            CollectionHandle& c,
            const pool_opts_t& opts) override;

    int stat(
            CollectionHandle &c,
            const ghobject_t& oid,
            struct stat *st,
            bool allow_eio = false) override;

    int read(
            CollectionHandle &c,
            const ghobject_t& oid,
            uint64_t offset,
            size_t len,
            bufferlist& bl,
            uint32_t op_flags = 0) override;
private:

    int _do_read(
            Collection *c,
            OnodeRef o,
            uint64_t offset,
            size_t len,
            bufferlist& bl,
            uint32_t op_flags = 0,
            uint64_t retry_count = 0);

    int _do_readv(
            Collection *c,
            OnodeRef o,
            const interval_set<uint64_t>& m,
            bufferlist& bl,
            uint32_t op_flags = 0,
            uint64_t retry_count = 0);

    int _fiemap(CollectionHandle &c_, const ghobject_t& oid,
                uint64_t offset, size_t len, interval_set<uint64_t>& destset);

public:

    int fiemap(CollectionHandle &c, const ghobject_t& oid,
               uint64_t offset, size_t len, bufferlist& bl) override;

    int fiemap(CollectionHandle &c, const ghobject_t& oid,
               uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap) override;

    int readv(
            CollectionHandle &c_,
            const ghobject_t& oid,
            interval_set<uint64_t>& m,
            bufferlist& bl,
            uint32_t op_flags) override;

    int dump_onode(CollectionHandle &c, const ghobject_t& oid,
                   const string& section_name, Formatter *f) override;

    int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
                bufferptr& value) override;

    int getattrs(CollectionHandle &c, const ghobject_t& oid,
                 map<string,bufferptr>& aset) override;

    int list_collections(vector<coll_t>& ls) override;

    CollectionHandle open_collection(const coll_t &c) override;
    CollectionHandle create_new_collection(const coll_t& cid) override;
    void set_collection_commit_queue(const coll_t& cid,
                                     ContextQueue *commit_queue) override;

    bool collection_exists(const coll_t& c) override;
    int collection_empty(CollectionHandle& c, bool *empty) override;
    int collection_bits(CollectionHandle& c) override;

    int collection_list(CollectionHandle &c,
                        const ghobject_t& start,
                        const ghobject_t& end,
                        int max,
                        vector<ghobject_t> *ls, ghobject_t *next) override;

    int omap_get(
            CollectionHandle &c,     ///< [in] Collection containing oid
            const ghobject_t &oid,   ///< [in] Object containing omap
            bufferlist *header,      ///< [out] omap header
            map<string, bufferlist> *out /// < [out] Key to value map
    ) override;
    int _omap_get(
            Collection *c,     ///< [in] Collection containing oid
            const ghobject_t &oid,   ///< [in] Object containing omap
            bufferlist *header,      ///< [out] omap header
            map<string, bufferlist> *out /// < [out] Key to value map
    );

    /// Get omap header
    int omap_get_header(
            CollectionHandle &c,                ///< [in] Collection containing oid
            const ghobject_t &oid,   ///< [in] Object containing omap
            bufferlist *header,      ///< [out] omap header
            bool allow_eio = false ///< [in] don't assert on eio
    ) override;

    /// Get keys defined on oid
    int omap_get_keys(
            CollectionHandle &c,              ///< [in] Collection containing oid
            const ghobject_t &oid, ///< [in] Object containing omap
            set<string> *keys      ///< [out] Keys defined on oid
    ) override;

    /// Get key values
    int omap_get_values(
            CollectionHandle &c,         ///< [in] Collection containing oid
            const ghobject_t &oid,       ///< [in] Object containing omap
            const set<string> &keys,     ///< [in] Keys to get
            map<string, bufferlist> *out ///< [out] Returned keys and values
    ) override;

    /// Filters keys into out which are defined on oid
    int omap_check_keys(
            CollectionHandle &c,                ///< [in] Collection containing oid
            const ghobject_t &oid,   ///< [in] Object containing omap
            const set<string> &keys, ///< [in] Keys to check
            set<string> *out         ///< [out] Subset of keys defined on oid
    ) override;

    ObjectMap::ObjectMapIterator get_omap_iterator(
            CollectionHandle &c,   ///< [in] collection
            const ghobject_t &oid  ///< [in] object
    ) override;

    void set_fsid(uuid_d u) override {
        fsid = u;
    }
    uuid_d get_fsid() override {
        return fsid;
    }

    uint64_t estimate_objects_overhead(uint64_t num_objects) override {
        return num_objects * 300; //assuming per-object overhead is 300 bytes
    }

    struct BSPerfTracker {
        PerfCounters::avg_tracker<uint64_t> os_commit_latency_ns;
        PerfCounters::avg_tracker<uint64_t> os_apply_latency_ns;

        objectstore_perf_stat_t get_cur_stats() const {
            objectstore_perf_stat_t ret;
            ret.os_commit_latency_ns = os_commit_latency_ns.current_avg();
            ret.os_apply_latency_ns = os_apply_latency_ns.current_avg();
            return ret;
        }

        void update_from_perfcounters(PerfCounters &logger);
    } perf_tracker;

    objectstore_perf_stat_t get_cur_stats() override {
        perf_tracker.update_from_perfcounters(*logger);
        return perf_tracker.get_cur_stats();
    }
    const PerfCounters* get_perf_counters() const override {
        return logger;
    }

    int queue_transactions(
            CollectionHandle& ch,
            vector<Transaction>& tls,
            TrackedOpRef op = TrackedOpRef(),
            ThreadPool::TPHandle *handle = NULL) override;

#if 0
    // error injection
    void inject_data_error(const ghobject_t& o) override {
        std::unique_lock l(debug_read_error_lock);
        debug_data_error_objects.insert(o);
    }
    void inject_mdata_error(const ghobject_t& o) override {
        std::unique_lock l(debug_read_error_lock);
        debug_mdata_error_objects.insert(o);
    }
#endif

    /// methods to inject various errors fsck can repair
    void inject_broken_shared_blob_key(const string& key,
                                       const bufferlist& bl);
    void inject_leaked(uint64_t len);
    void inject_false_free(coll_t cid, ghobject_t oid);
    void inject_statfs(const string& key, const store_statfs_t& new_statfs);
    void inject_global_statfs(const store_statfs_t& new_statfs);
    void inject_misreference(coll_t cid1, ghobject_t oid1,
                             coll_t cid2, ghobject_t oid2,
                             uint64_t offset);

    void compact() override {
        ceph_assert(db);
        db->compact();
    }
    bool has_builtin_csum() const override {
        return true;
    }

    inline void log_latency(const char* name,
                            int idx,
                            const ceph::timespan& lat,
                            double lat_threshold,
                            const char* info = "") const;

    inline void log_latency_fn(const char* name,
                               int idx,
                               const ceph::timespan& lat,
                               double lat_threshold,
                               std::function<string (const ceph::timespan& lat)> fn) const;

private:
#if 0
    bool _debug_data_eio(const ghobject_t& o) {
        if (!cct->_conf->kvsstore_debug_inject_read_err) {
            return false;
        }
        std::shared_lock l(debug_read_error_lock);
        return debug_data_error_objects.count(o);
    }
    bool _debug_mdata_eio(const ghobject_t& o) {
        if (!cct->_conf->kvsstore_debug_inject_read_err) {
            return false;
        }
        std::shared_lock l(debug_read_error_lock);
        return debug_mdata_error_objects.count(o);
    }
    void _debug_obj_on_delete(const ghobject_t& o) {
        if (cct->_conf->kvsstore_debug_inject_read_err) {
            std::unique_lock l(debug_read_error_lock);
            debug_data_error_objects.erase(o);
            debug_mdata_error_objects.erase(o);
        }
    }
#endif
private:
    ceph::mutex qlock = ceph::make_mutex("KvsStore::Alerts::qlock");
    string failed_cmode;
    set<string> failed_compressors;
    string spillover_alert;
    string legacy_statfs_alert;
    string no_per_pool_omap_alert;
    string disk_size_mismatch_alert;

    void _log_alerts(osd_alert_list_t& alerts);
    bool _set_compression_alert(bool cmode, const char* s) {
        std::lock_guard l(qlock);
        if (cmode) {
            bool ret = failed_cmode.empty();
            failed_cmode = s;
            return ret;
        }
        return failed_compressors.emplace(s).second;
    }
    void _clear_compression_alert() {
        std::lock_guard l(qlock);
        failed_compressors.clear();
        failed_cmode.clear();
    }

    void _set_spillover_alert(const string& s) {
        std::lock_guard l(qlock);
        spillover_alert = s;
    }
    void _clear_spillover_alert() {
        std::lock_guard l(qlock);
        spillover_alert.clear();
    }

    void _check_legacy_statfs_alert();
    void _check_no_per_pool_omap_alert();
    void _set_disk_size_mismatch_alert(const string& s) {
        std::lock_guard l(qlock);
        disk_size_mismatch_alert = s;
    }

private:

    // --------------------------------------------------------
    // write ops

    int _write(TransContext *txc,
               CollectionRef& c,
               OnodeRef& o,
               uint64_t offset, size_t len,
               bufferlist& bl,
               uint32_t fadvise_flags);
    void _pad_zeros(bufferlist *bl, uint64_t *offset,
                    uint64_t chunk_size);

    int _do_write(TransContext *txc,
                  CollectionRef &c,
                  OnodeRef o,
                  uint64_t offset, uint64_t length,
                  bufferlist& bl,
                  uint32_t fadvise_flags);

    int _touch(TransContext *txc,
               CollectionRef& c,
               OnodeRef& o);
    int _do_zero(TransContext *txc,
                 CollectionRef& c,
                 OnodeRef& o,
                 uint64_t offset, size_t len);
    int _zero(TransContext *txc,
              CollectionRef& c,
              OnodeRef& o,
              uint64_t offset, size_t len);

    int _truncate(TransContext *txc,
                  CollectionRef& c,
                  OnodeRef& o,
                  uint64_t offset);
    int _remove(TransContext *txc,
                CollectionRef& c,
                OnodeRef& o);
    int _do_remove(TransContext *txc,
                   CollectionRef& c,
                   OnodeRef o);
    int _setattr(TransContext *txc,
                 CollectionRef& c,
                 OnodeRef& o,
                 const string& name,
                 bufferptr& val);
    int _setattrs(TransContext *txc,
                  CollectionRef& c,
                  OnodeRef& o,
                  const map<string,bufferptr>& aset);
    int _rmattr(TransContext *txc,
                CollectionRef& c,
                OnodeRef& o,
                const string& name);
    int _rmattrs(TransContext *txc,
                 CollectionRef& c,
                 OnodeRef& o);
    void _do_omap_clear(TransContext *txc, OnodeRef &o);
    int _omap_clear(TransContext *txc,
                    CollectionRef& c,
                    OnodeRef& o);
    int _omap_setkeys(TransContext *txc,
                      CollectionRef& c,
                      OnodeRef& o,
                      bufferlist& bl);
    int _omap_setheader(TransContext *txc,
                        CollectionRef& c,
                        OnodeRef& o,
                        bufferlist& header);
    int _omap_rmkeys(TransContext *txc,
                     CollectionRef& c,
                     OnodeRef& o,
                     bufferlist& bl);
    int _omap_rmkey_range(TransContext *txc,
                          CollectionRef& c,
                          OnodeRef& o,
                          const string& first, const string& last);
    int _set_alloc_hint(
            TransContext *txc,
            CollectionRef& c,
            OnodeRef& o,
            uint64_t expected_object_size,
            uint64_t expected_write_size,
            uint32_t flags);
    int _do_clone_range(TransContext *txc,
                        CollectionRef& c,
                        OnodeRef& oldo,
                        OnodeRef& newo,
                        uint64_t srcoff, uint64_t length, uint64_t dstoff);
    int _clone(TransContext *txc,
               CollectionRef& c,
               OnodeRef& oldo,
               OnodeRef& newo);
    int _clone_range(TransContext *txc,
                     CollectionRef& c,
                     OnodeRef& oldo,
                     OnodeRef& newo,
                     uint64_t srcoff, uint64_t length, uint64_t dstoff);
    int _rename(TransContext *txc,
                CollectionRef& c,
                OnodeRef& oldo,
                OnodeRef& newo,
                const ghobject_t& new_oid);
    int _create_collection(TransContext *txc, const coll_t &cid,
                           unsigned bits, CollectionRef *c);
    int _remove_collection(TransContext *txc, const coll_t &cid,
                           CollectionRef *c);
    void _do_remove_collection(TransContext *txc, CollectionRef *c);
    int _split_collection(TransContext *txc,
                          CollectionRef& c,
                          CollectionRef& d,
                          unsigned bits, int rem);
    int _merge_collection(TransContext *txc,
                          CollectionRef *c,
                          CollectionRef& d,
                          unsigned bits);

public:

};

inline ostream& operator<<(ostream& out, const KvsStore::volatile_statfs& s) {
    return out
            << " allocated:"
            << s.values[KvsStore::volatile_statfs::STATFS_ALLOCATED]
            << " stored:"
            << s.values[KvsStore::volatile_statfs::STATFS_STORED]
            << " compressed:"
            << s.values[KvsStore::volatile_statfs::STATFS_COMPRESSED]
            << " compressed_orig:"
            << s.values[KvsStore::volatile_statfs::STATFS_COMPRESSED_ORIGINAL]
            << " compressed_alloc:"
            << s.values[KvsStore::volatile_statfs::STATFS_COMPRESSED_ALLOCATED];
}

static inline void intrusive_ptr_add_ref(KvsStore::Onode *o) {
    o->get();
}
static inline void intrusive_ptr_release(KvsStore::Onode *o) {
    o->put();
}

static inline void intrusive_ptr_add_ref(KvsStore::OpSequencer *o) {
    o->get();
}
static inline void intrusive_ptr_release(KvsStore::OpSequencer *o) {
    o->put();
}


#endif
