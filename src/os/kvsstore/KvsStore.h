#ifndef CEPH_KVSSTORE_H
#define CEPH_KVSSTORE_H

#include "os_adapter.h"

enum {
    l_kvsstore_first = 932430,
    l_kvsstore_last
};

class KvsStore: public ObjectStoreAdapter {

public:
	KvsStoreDB db;
	KvsStoreCache *cache;

public:
    KvsStore(CephContext *cct, const string& path);
    ~KvsStore() override;

    void _init_perf_logger(CephContext *cct);

public: // inherited interfaces

    /// =========================================================
	/// ObjectStoreAdapter Implementation
	/// =========================================================

    // for mount & umount
    int read_sb() override;
    int write_sb() override;
    int open_db(bool create) override;
    void close_db() override;

    int mkfs_kvsstore() override;
    int mount_kvsstore() override;
    int umount_kvsstore() override;
    int flush_cache_impl(bool collmap_clear = false) override;
    void osr_drain_all() override;

    int fsck_impl() override;
    int fiemap_impl(CollectionHandle &c_, const ghobject_t &oid, uint64_t offset, size_t len, map<uint64_t, uint64_t> &destmap) override;

    int open_collections() override;
    void reap_collections() override;
    CollectionRef get_collection(const coll_t& cid) override;

    int omap_get_impl(Collection *c,const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out) override;

    /// =========================================================
    /// ObjectStore Implementation
    /// =========================================================

    // cache
    void set_cache_shards(unsigned num) override;

    // collections
    int list_collections(vector<coll_t>& ls) override;
    bool collection_exists(const coll_t& c) override;
    CollectionHandle create_new_collection(const coll_t &c) override;
    int set_collection_opts(CollectionHandle &c, const pool_opts_t &opts) override;
    int collection_list(CollectionHandle &c, const ghobject_t& start, const ghobject_t& end, int max, vector<ghobject_t> *ls, ghobject_t *next) override;

    //stat
    int stat(CollectionHandle &c, const ghobject_t &oid, struct stat *st, bool allow_eio= false) override;
    int statfs(struct store_statfs_t *buf, osd_alert_list_t* alerts = nullptr) override;

    // read & write
    bool exists(CollectionHandle &c_, const ghobject_t& oid) override;
    int read(CollectionHandle &c,const ghobject_t& oid, uint64_t offset,size_t len,bufferlist& bl,uint32_t op_flags = 0) override;
    int queue_transactions(CollectionHandle& ch, vector<Transaction>& tls, TrackedOpRef op = TrackedOpRef(), ThreadPool::TPHandle *handle = NULL) override;

    // attributes
    int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name, bufferptr& value) override;
    int getattrs(CollectionHandle &c, const ghobject_t& oid, map<string,bufferptr>& aset) override;

    // omap
    int omap_get_header(    CollectionHandle &c,    const ghobject_t &oid,    bufferlist *header,    bool allow_eio = false) override;
    int omap_get_keys(    CollectionHandle &c,    const ghobject_t &oid,    set<string> *keys    ) override;
    int omap_get_values(    CollectionHandle &c,    const ghobject_t &oid,    const set<string> &keys,    map<string, bufferlist> *out    ) override;
    int omap_check_keys(CollectionHandle &c,const ghobject_t &oid,   const set<string> &keys,     set<string> *out) override;
    ObjectMap::ObjectMapIterator get_omap_iterator( CollectionHandle &c, const ghobject_t &oid ) override;


public:

    /// =========================================================
    /// Read Functions
    /// =========================================================

    int _do_read(Collection *c,OnodeRef o,uint64_t offset,size_t length,bufferlist& bl,uint32_t op_flags = 0, uint64_t retry_count = 0);
    int _do_read_chunks_async(OnodeRef &o, ready_regions_t &ready_regions, chunk2read_t &chunk2read, BufferCacheShard *cache);
    int _prepare_read_chunk_ioc(const ghobject_t &oid, ready_regions_t& ready_regions,chunk2read_t& chunk2read, IoContext *ioc);
    int _generate_read_result_bl(OnodeRef o,uint64_t offset,size_t length, ready_regions_t& ready_regions, bufferlist& bl);
    void _read_cache(BufferCacheShard *cache, OnodeRef o, uint64_t offset , size_t length, int read_cache_policy,ready_regions_t& ready_regions,chunk2read_t& blobs2read);

public:

    /// =========================================================
    /// Onode Support
    /// =========================================================

    void _assign_nid(TransContext *txc, OnodeRef o);

public:

    /// =========================================================
    /// Transaction Support
    /// =========================================================

    TransContext *_txc_create(Collection *c, OpSequencer *osr, list<Context*> *on_commits);
    void _txc_add_transaction(TransContext *txc, Transaction *t);

    /// Write Functions

    int _touch(TransContext *txc,CollectionRef& c,OnodeRef &o);
    int _write(TransContext *txc,CollectionRef& c,OnodeRef& o,uint64_t offset, size_t len, bufferlist& bl,uint32_t fadvise_flags);

    int _do_write(TransContext *txc, CollectionRef& c,OnodeRef &o,uint64_t offset, uint64_t length,bufferlist::iterator* blp);
    int _do_write_read_chunks_if_needed(CollectionRef& c, OnodeRef &o, const uint64_t object_length,
                                        uint64_t offset, uint64_t length, ready_regions_t &readyregions, zero_regions_t &zeroregions, const uint64_t chunksize);
    void _do_write_pad_zeros(ready_regions_t &readyregions, zero_regions_t &zeroregions, const uint64_t chunksize);

    int _zero(TransContext *txc,  CollectionRef& c, OnodeRef& o, uint64_t offset, size_t length);

    /// Truncate Functions

    int _truncate(TransContext *txc, CollectionRef& c, OnodeRef& o, uint64_t offset);
    void _do_truncate(TransContext *txc, CollectionRef &c, OnodeRef o, uint64_t offset);

    /// Remove Functions

    int _remove(TransContext *txc, CollectionRef& c, OnodeRef &o);
    int _do_remove(TransContext *txc, CollectionRef& c, OnodeRef o);

    /// Clone I/O Functions

    int _clone(TransContext *txc,CollectionRef& c, OnodeRef& oldo,OnodeRef& newo);
    int _clone_range(TransContext *txc,CollectionRef& c,OnodeRef& oldo,OnodeRef& newo, uint64_t srcoff, uint64_t length, uint64_t dstoff);
    int _rename(TransContext *txc, CollectionRef& c, OnodeRef& oldo, OnodeRef& newo, const ghobject_t& new_oid);

    /// Attribute I/O Functions

    int _setattrs(TransContext *txc,CollectionRef& c, OnodeRef& o,const map<string,bufferptr>& aset);
    int _setattr(TransContext *txc, CollectionRef& c, OnodeRef& o, const string& name, bufferptr& val);
    int _rmattr(TransContext *txc,CollectionRef& c,OnodeRef& o,const string& name);
    int _rmattrs(TransContext *txc,CollectionRef& c,OnodeRef& o);

    /// OMAP I/O Functions

    class OmapIteratorImpl;
    void _do_omap_clear(TransContext *txc, OnodeRef &o);
    int _omap_clear(TransContext *txc, CollectionRef &c, OnodeRef &o);
    int _omap_setkeys(TransContext *txc, CollectionRef &c, OnodeRef &o, bufferlist &bl);
    int _omap_setheader(TransContext *txc, CollectionRef &c, OnodeRef &o, bufferlist &header);
    int _omap_rmkeys(TransContext *txc, CollectionRef &c, OnodeRef &o, bufferlist &bl);
    int _omap_rmkey_range(TransContext *txc, CollectionRef &c, OnodeRef &o, const string &first, const string &last);

    /// Collection I/O Functions
    void _queue_reap_collection(CollectionRef& c);
    int _create_collection( TransContext *txc, const coll_t &cid, unsigned bits, CollectionRef *c);
    int _remove_collection(TransContext *txc, const coll_t &cid, CollectionRef *c);
    int _do_remove_collection(TransContext *txc, CollectionRef *c);
    int _split_collection(TransContext *txc, CollectionRef& c, CollectionRef& d, unsigned bits, int rem);
    int _merge_collection(TransContext *txc, CollectionRef *c, CollectionRef& d, unsigned bits);
    int _collection_list(Collection *c, const ghobject_t& start, const ghobject_t& end, int max, vector<ghobject_t> *ls, ghobject_t *pnext);

    /// Transaction Support Functions
    void txc_aio_finish(TransContext *txc);
    void _txc_state_proc(TransContext *txc);
    void _txc_committed_kv(TransContext *txc);
    void _txc_finish_io(TransContext *txc);
    void _txc_finish_writes(TransContext *txc);
    void _txc_finish(TransContext *txc);
    int _txc_write_nodes(TransContext *txc);

public:
    /// =========================================================
    /// OP Sequencer
    /// =========================================================

    void _osr_attach(Collection *c);
    void _osr_register_zombie(OpSequencer *osr);
    void _osr_drain(OpSequencer *osr);
    void _osr_drain_preceding(TransContext *txc);



public:

    /// =========================================================
    /// Internal Threads for KvsStore
    /// =========================================================

    //bool _check_db();
    bool _check_onode_validity(kvsstore_onode_t &ori_onode, bufferlist&bl);
    void _kv_callback_thread();
    void _kv_finalize_thread();
    void _kv_index_thread();

    struct KVCallbackThread : public Thread {
        KvsStore *store;
        explicit KVCallbackThread(KvsStore *s) : store(s) {}
        void *entry() override { store->_kv_callback_thread(); return NULL; }
    };

    struct KVFinalizeThread : public Thread {
        KvsStore *store;
        explicit KVFinalizeThread(KvsStore *s) : store(s) {}
        void *entry() { store->_kv_finalize_thread(); return NULL; }
    };

    struct KVIndexThread : public Thread {
        KvsStore *store;
        explicit KVIndexThread(KvsStore *s) : store(s) {}
        void *entry() { store->_kv_index_thread(); return NULL; }
    };


public:

    /// =========================================================
	/// Member Variables
	/// =========================================================

    std::atomic<uint64_t> nid_last  = {0};			//# Onode ID

    //# Collections -----------------------

    ceph::shared_mutex coll_lock = ceph::make_shared_mutex("KvsStore::coll_lock");  ///< rwlock to protect coll_map
    mempool::kvsstore_cache_other::unordered_map<coll_t, CollectionRef> coll_map;
    map<coll_t, CollectionRef> new_coll_map;
    list<CollectionRef> removed_collections;

    //# Protect zombie_osr_set -------------------------------------

    std::mutex zombie_osr_lock; // = ceph::make_mutex("KvsStore::zombie_osr_lock");
    uint32_t next_sequencer_id = 0;
    std::map<coll_t,OpSequencerRef> zombie_osr_set;

    //# Onode & Data cache  ----------------------------------------

    vector<OnodeCacheShard*>  onode_cache_shards;
    vector<BufferCacheShard*> buffer_cache_shards;

    std::mutex compact_lock;

    Finisher finisher;

    std::mutex kv_lock;
    std::condition_variable kv_cond;

    std::mutex kv_finalize_lock;
    std::condition_variable kv_finalize_cond;

    std::atomic_bool kv_stop = { false };

    bool kv_index_stop = false;
    bool kv_callback_started = false;
    bool kv_finalize_started = false;
    bool kv_finalize_stop = false;

    KVCallbackThread kv_callback_thread;
    KVFinalizeThread kv_finalize_thread;
    KVIndexThread    kv_index_thread;

    deque<TransContext*> kv_finalize_queue;   ///< pending finalization

    kvsstore_sb_t kvsb;
    CompressorRef cp;
};



#endif /* SRC_OS_KVSSTORE_KVSSTORE_H_ */
