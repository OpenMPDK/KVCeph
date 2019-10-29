//
// Created by root on 10/12/18.
//

#ifndef CEPH_KVSSTORE_H
#define CEPH_KVSSTORE_H


#include <unistd.h>
#include <memory.h>

#include <atomic>
#include <mutex>
#include <vector>
#include <map>
#include <condition_variable>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/range.hpp>
#include <boost/range/join.hpp>

#include "kvsstore_kadi.h"
#include "kvsstore_types.h"
#include "kvsstore_cmds.h"

#include "include/ceph_assert.h"
#include "include/unordered_map.h"
#include "include/mempool.h"
//#include "include/memory.h"

#include "common/Finisher.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "os/ObjectStore.h"
#include "common/perf_counters.h"
#include "os/fs/FS.h"


// TODO: do not read onode

enum {
    l_kvsstore_first = 932430,
    l_prefetch_onode_cache_hit,
    l_prefetch_onode_cache_slow,
    l_prefetch_onode_cache_miss, 
    l_kvsstore_read_lat,
    l_kvsstore_queue_to_submit_lat,
    l_kvsstore_submit_to_complete_lat,
    l_kvsstore_onode_read_miss_lat,
    //l_kvsstore_onode_hit,
    //l_kvsstore_onode_miss,
    l_kvsstore_read_latency,
    l_kvsstore_tr_latency,
    l_kvsstore_write_latency,
    l_kvsstore_delete_latency,
    l_kvsstore_iterate_latency,
    l_kvsstore_pending_trx_ios,

	l_kvsstore_1_add_tr_latency,  /* t1 - t0 */
	l_kvsstore_2_add_onode_latency, /* t2- t1 */
	l_kvsstore_3_add_journal_write_latency, /* t3-t2 */
	l_kvsstore_4_aio_submit_latency,   /* t4-t3 */
	l_kvsstore_5_device_io_latency,   /* t5-t4 */
	l_kvsstore_6_tr_ordering_latency,   /* t6-t5 */
	l_kvsstore_7_add_finisher_latency, /*t7-t6*/
	l_kvsstore_8_finisher_latency, /*t8-t7*/
	l_kvsstore_9_release_latency , /*t9-t8*/
	l_kvsstore_10_full_tr_latency, /*t9-t0*/
    l_kvsstore_last
};

enum {
    kvsstore_csum_none = 0,
    kvsstore_csum_crc32c = 1
};

class KvsStore : public ObjectStore {

private:
    /// Types
    ///     - Three background threads: callback, finalize, and mempool

    struct KVCallbackThread : public Thread {
        KvsStore *store;
        explicit KVCallbackThread(KvsStore *s) : store(s) {}
        void *entry() override {
            store->_kv_callback_thread();
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
 #if 0
    struct KVPrefetchThread: public Thread{
        KvsStore *store;
        Cond cond;
        Mutex lock;
        bool stop = false;
    public:
        explicit KVPrefetchThread(KvsStore *s)
                : store(s),
                  lock("KvsStore::KVPrefetchThread::lock"){}
        void *entry(){
            store->_kv_prefetch_thread();
            return NULL;
        }
    };
#endif
    struct MempoolThread : public Thread {
        KvsStore *store;
        std::condition_variable cond;
        ceph::mutex lock = ceph::make_mutex("KvsStore::MempoolThread::lock");
        bool stop = false;
    public:
        explicit MempoolThread(KvsStore *s)
                : store(s) {}

        void *entry() override;

        void init() {
            assert(stop == false);
            create("kvsmempool");
        }

        void shutdown() {
            lock.lock();
            stop = true;
            cond.notify_all();
            lock.unlock();
            join();
        }
    } ;
public:
    KvsStoreKADIAdapter db;
    KvsStoreKVCommands  kvcmds;
    std::atomic<uint64_t> lid_last  = {0};
private:
    ///
    /// Member variables


    uuid_d fsid;
    int path_fd = -1;  ///< open handle to $path
    int fsid_fd = -1;  ///< open handle (locked) to $path/fsid
    int csum_type = 0;
    bool mounted = false;
    
    LsCache<ghobject_t, KvsStore> lscache;
    
    RWLock coll_lock = {"KvsStore::coll_lock"};  ///< rwlock to protect coll_map
    mempool::kvsstore_cache_other::unordered_map<coll_t, CollectionRef> coll_map;
    map<coll_t, CollectionRef> new_coll_map;
    vector<KvsCollection *> cached_collections;
    
    vector<KvsCache*> cache_shards;

    // protect zombie_osr_set
    ceph::mutex zombie_osr_lock = ceph::make_mutex("KvsStore::zombie_osr_lock");
    std::map<coll_t,OpSequencerRef> zombie_osr_set;

    std::atomic<uint64_t> nid_last = {0};
    std::atomic<uint64_t> nid_max = {0};

    int m_finisher_num = 1;
    vector<Finisher*> finishers;
   // Finisher finisher;

    std::atomic_bool kv_stop = { false };
    bool kv_callback_started = false;
    bool kv_finalize_started = false;
    bool kv_finalize_stop = false;

    std::mutex osr_lock;              ///< protect osd_set
    std::set<OpSequencerRef> osr_set; ///< set of all OpSequencers

    std::mutex kv_lock;               ///< control kv threads
    std::condition_variable kv_cond;

    KVCallbackThread kv_callback_thread;
    KVCallbackThread kv_callback_thread2;

    KVFinalizeThread kv_finalize_thread;
    std::mutex kv_finalize_lock;
    std::condition_variable kv_finalize_cond;
    deque<KvsTransContext*> kv_committing_to_finalize;   ///< pending finalization

    MempoolThread    mempool_thread;

    PerfCounters *logger = nullptr;

    std::mutex reap_lock;
    list<CollectionRef> removed_collections;
    kvsstore_sb_t kvsb;


private:

    ///
    /// Utility functions

    // for mount & umount
    int _open_fsid(bool create);
    int _read_fsid(uuid_d *uuid);
    int _write_fsid();
    void _close_fsid();
    int _lock_fsid();
    int _open_path();
    void _close_path();
    void _flush_cache();
    int _read_sb();
    int _write_sb();

    int get_predefinedID(const std::string& key);

    void _osr_attach(KvsCollection *c);
    void _osr_register_zombie(KvsOpSequencer *osr);
    void _osr_drain(KvsOpSequencer *osr);
    //void _osr_drain_preceding(KvsTransContext *txc);
    void _osr_drain_all();
    void _osr_unregister_all();

public:
    ///
    /// Constructor
    KvsStore(CephContext *cct, const string& path);
    ~KvsStore() override;

    string get_type() override {
        return "kvsstore";
    }


public:
    ///
    /// Interface

    bool needs_journal() override { return false; }
    bool wants_journal() override { return false; }
    bool allows_journal() override { return false;}

    bool is_rotational() override { return false; }
    bool is_journal_rotational() override { return false; }

    string get_default_device_class() override { return "ssd"; }
    bool test_mount_in_use() override;

    int mount() override;
    int umount() override;

    int fsck(bool deep) override { return _fsck_with_mount();    }
    int repair(bool deep) override { return _fsck_with_mount();    }

    void set_cache_shards(unsigned num) override;

    int validate_hobject_key(const hobject_t &obj) const override { return 0; }
    unsigned get_max_attr_name_length() override { return 256; }

    int mkfs() override;
    int mkjournal() override { return 0;   }

    int flush_cache(ostream *os = NULL) override;
    void _txc_committed_kv(KvsTransContext *txc);
    void _txc_finish(KvsTransContext *txc);
    void _txc_release_alloc(KvsTransContext *txc);
    void dump_perf_counters(Formatter *f) override {
        f->open_object_section("perf_counters");
        logger->dump_formatted(f, false);
        f->close_section();
    }

public:
    ///
    /// Interface - continued

    int statfs(struct store_statfs_t *buf,
                osd_alert_list_t* alerts = nullptr) override;
    int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
          bool *per_pool_omap) override;

    bool exists(const coll_t& cid, const ghobject_t& oid);
    bool exists(CollectionHandle &c_, const ghobject_t& oid) override;
   
    //void prefetch_onode(const coll_t& cid, const ghobject_t* oid) override;
    void prefetch_onode(const coll_t &cid, const ghobject_t *oid);

    int set_collection_opts( const coll_t& cid, const pool_opts_t& opts);
    int set_collection_opts(CollectionHandle &c, const pool_opts_t &opts) override;

    int stat(const coll_t& cid, const ghobject_t& oid, struct stat *st, bool allow_eio = false);
    int stat(CollectionHandle &c, const ghobject_t& oid, struct stat *st, bool allow_eio = false) override;

    int read(const coll_t& cid,const ghobject_t& oid,uint64_t offset, size_t len,bufferlist& bl, uint32_t op_flags = 0);
    int read(CollectionHandle &c,const ghobject_t& oid,uint64_t offset,size_t len,bufferlist& bl,uint32_t op_flags = 0) override;

    int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
    int fiemap(CollectionHandle &c, const ghobject_t& oid,uint64_t offset, size_t len, bufferlist& bl) override;
 
    int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap);
    int fiemap(CollectionHandle &c, const ghobject_t& oid, uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap) override;

    int getattr(const coll_t& cid, const ghobject_t& oid, const char *name, bufferptr& value);
    int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name, bufferptr& value) override;

    int getattrs(const coll_t& cid, const ghobject_t& oid, map<string,bufferptr>& aset);
    int getattrs(CollectionHandle &c, const ghobject_t& oid, map<string,bufferptr>& aset) override;

    int list_collections(vector<coll_t>& ls) override;

    CollectionHandle open_collection(const coll_t &c) override;
    // added
    CollectionHandle create_new_collection(const coll_t &c) override;
    void set_collection_commit_queue(const coll_t &cid,
                                     ContextQueue *commit_queue) override;

    bool collection_exists(const coll_t& c) override;
    int collection_empty(CollectionHandle &c, bool *empty) override;
    int collection_bits(CollectionHandle &c) override;

    int collection_list(const coll_t& cid, const ghobject_t& start, const ghobject_t& end, int max, vector<ghobject_t> *ls, ghobject_t *next);
    int collection_list(CollectionHandle &c, const ghobject_t& start, const ghobject_t& end, int max, vector<ghobject_t> *ls, ghobject_t *next) override;


    int omap_get( const coll_t& cid, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out);
    int omap_get(CollectionHandle &c,const ghobject_t &oid,bufferlist *header,  map<string, bufferlist> *out) override;

    /// Get omap header
    int omap_get_header(
            const coll_t& cid,                ///< [in] Collection containing oid
            const ghobject_t &oid,   ///< [in] Object containing omap
            bufferlist *header,      ///< [out] omap header
            bool allow_eio = false ///< [in] don't assert on eio
    );
    int omap_get_header(
            CollectionHandle &c,                ///< [in] Collection containing oid
            const ghobject_t &oid,   ///< [in] Object containing omap
            bufferlist *header,      ///< [out] omap header
            bool allow_eio = false ///< [in] don't assert on eio
    ) override;

    /// Get keys defined on oid
    int omap_get_keys(
            const coll_t& cid,              ///< [in] Collection containing oid
            const ghobject_t &oid, ///< [in] Object containing omap
            set<string> *keys      ///< [out] Keys defined on oid
    );
    int omap_get_keys(
            CollectionHandle &c,              ///< [in] Collection containing oid
            const ghobject_t &oid, ///< [in] Object containing omap
            set<string> *keys      ///< [out] Keys defined on oid
    ) override;

    /// Get key values
    int omap_get_values(
            const coll_t& cid,                    ///< [in] Collection containing oid
            const ghobject_t &oid,       ///< [in] Object containing omap
            const set<string> &keys,     ///< [in] Keys to get
            map<string, bufferlist> *out ///< [out] Returned keys and values
    );
    int omap_get_values(
            CollectionHandle &c,         ///< [in] Collection containing oid
            const ghobject_t &oid,       ///< [in] Object containing omap
            const set<string> &keys,     ///< [in] Keys to get
            map<string, bufferlist> *out ///< [out] Returned keys and values
    ) override;

    /// Filters keys into out which are defined on oid
    int omap_check_keys(
            const coll_t& cid,                ///< [in] Collection containing oid
            const ghobject_t &oid,   ///< [in] Object containing omap
            const set<string> &keys, ///< [in] Keys to check
            set<string> *out         ///< [out] Subset of keys defined on oid
    );
    int omap_check_keys(
            CollectionHandle &c,                ///< [in] Collection containing oid
            const ghobject_t &oid,   ///< [in] Object containing omap
            const set<string> &keys, ///< [in] Keys to check
            set<string> *out         ///< [out] Subset of keys defined on oid
    ) override;

    ObjectMap::ObjectMapIterator get_omap_iterator(
            const coll_t& cid,              ///< [in] collection
            const ghobject_t &oid  ///< [in] object
    );

    ObjectMap::ObjectMapIterator get_omap_iterator(
            CollectionHandle &c,   ///< [in] collection
            const ghobject_t &oid  ///< [in] object
    ) override;

    ObjectMap::ObjectMapIterator _get_omap_iterator(
            KvsCollection *c,   ///< [in] collection
            OnodeRef &o  ///< [in] object
    );


    void set_fsid(uuid_d u) override {
        fsid = u;
    }
    uuid_d get_fsid() override {
        return fsid;
    }

    uint64_t estimate_objects_overhead(uint64_t num_objects) override {
        return num_objects * 300; //assuming per-object overhead is 300 bytes
    }

    objectstore_perf_stat_t get_cur_stats() override { return objectstore_perf_stat_t(); }
    const PerfCounters* get_perf_counters() const override { return logger; }
    PerfCounters* get_counters() { return logger;}

    int queue_transactions(CollectionHandle& ch, vector<Transaction>& tls, TrackedOpRef op = TrackedOpRef(), ThreadPool::TPHandle *handle = NULL) override;
    void update_latency(int op, uint64_t latency);
private:
    ///
    /// Interface implementation
    int _do_read(KvsCollection *c, OnodeRef o,uint64_t offset,size_t len,bufferlist& bl,uint32_t op_flags = 0);
    int _fiemap(CollectionHandle &c_, const ghobject_t& oid, uint64_t offset, size_t len, interval_set<uint64_t>& destset);

    int _open_super();
    int _open_db(bool create);
    void _close_db();
    CollectionRef _get_collection(const coll_t& cid);
    int _open_collections();
    void _reap_collections();
    void _txc_add_transaction(KvsTransContext *txc, Transaction *t);
    void _txc_journal_meta(KvsTransContext *txc, uint64_t index);
    int _touch(KvsTransContext *txc,CollectionRef& c,OnodeRef &o);
    int _write(KvsTransContext *txc,CollectionRef& c,OnodeRef& o,uint64_t offset, size_t len,bufferlist* bl,uint32_t fadvise_flags, bool truncate = false);
    int _update_write_buffer(OnodeRef &o, uint64_t offset, size_t length, bufferlist *towrite, bufferlist &out, bool truncate);
    int _do_write(KvsTransContext *txc, CollectionRef& c,OnodeRef o,uint64_t offset, uint64_t length,bufferlist& bl, uint32_t fadvise_flags);
    void _txc_write_onodes(KvsTransContext *txc);
    int journal_replay(int prefix, const std::function<int (kv_key*)> &keyconverter);
    void _txc_state_proc(KvsTransContext *txc);
    void _txc_aio_submit(KvsTransContext *txc);
    void _txc_finish_io(KvsTransContext *txc);
    void _txc_finish(KvsTransContext *txc, KvsOpSequencer::q_list_t &releasing_txc);
    void _reap_transactions(KvsOpSequencer::q_list_t  &releasing_txc);
    void _queue_reap_collection(CollectionRef& c);
    int _omap_setkeys(KvsTransContext *txc,CollectionRef& c, OnodeRef& o,bufferlist &bl);
    int _omap_rmkeys(KvsTransContext *txc,CollectionRef& c, OnodeRef& o, bufferlist& bl);
    int _omap_rmkey_range(KvsTransContext *txc,CollectionRef& c,OnodeRef& o, const string& first, const string& last);

    int _setattrs(KvsTransContext *txc,CollectionRef& c, OnodeRef& o,const map<string,bufferptr>& aset);
    int _fsck();
    int _fsck_with_mount();
    int _fiemap(CollectionHandle &c_,const ghobject_t& oid, uint64_t offset,
            size_t len, map<uint64_t, uint64_t>& destmap);

    int _rename(KvsTransContext *txc, CollectionRef& c,
                           OnodeRef& oldo, OnodeRef& newo,
                           const ghobject_t& new_oid);

    KvsTransContext *_txc_create(KvsCollection *c, KvsOpSequencer *osr,
                                 list<Context *> *on_commits);

    int _collection_list(
            KvsCollection *c, const ghobject_t& start, const ghobject_t& end, int max,
            vector<ghobject_t> *ls, ghobject_t *pnext, bool destructive = false);

    int _prep_collection_list(KvsCollection *c, const ghobject_t& start, struct iter_param &temp, struct iter_param &other);
    int _load_and_search_collection_list(const ghobject_t& start, const ghobject_t& end, struct iter_param &temp, struct iter_param &other, int max,
            vector<ghobject_t> *ls, ghobject_t *pnext, bool destructive = false);

    void _txc_write_nodes(KvsTransContext *txc);
    int _remove_collection(KvsTransContext *txc, const coll_t &cid,
                           CollectionRef *c);

    int _zero(KvsTransContext *txc,  CollectionRef& c, OnodeRef& o, uint64_t offset, size_t length);
    int _do_zero(KvsTransContext *txc, CollectionRef& c, OnodeRef& o, uint64_t offset, size_t length);
    int _truncate(KvsTransContext *txc, CollectionRef& c, OnodeRef& o, uint64_t offset);
    int _do_remove(KvsTransContext *txc, CollectionRef& c, OnodeRef o);
    int _do_truncate(KvsTransContext *txc, CollectionRef& c, OnodeRef o, uint64_t offset);
    int _remove(KvsTransContext *txc, CollectionRef& c, OnodeRef &o);
    int _setattr(KvsTransContext *txc, CollectionRef& c, OnodeRef& o, const string& name, bufferptr& val);
    int _rmattr(KvsTransContext *txc,CollectionRef& c,OnodeRef& o,const string& name);
    int _rmattrs(KvsTransContext *txc,CollectionRef& c,OnodeRef& o);
    void _do_omap_clear(KvsTransContext *txc, OnodeRef &o);
    int _clone(KvsTransContext *txc,CollectionRef& c, OnodeRef& oldo,OnodeRef& newo);
    int _clone_range(KvsTransContext *txc,CollectionRef& c,OnodeRef& oldo,OnodeRef& newo,
                                uint64_t srcoff, uint64_t length, uint64_t dstoff);

    int _omap_clear(KvsTransContext *txc,CollectionRef& c,OnodeRef& o);
    int _omap_setheader(KvsTransContext *txc, CollectionRef& c, OnodeRef &o, bufferlist& bl);

    int _set_alloc_hint( KvsTransContext *txc, CollectionRef& c, OnodeRef& o, uint64_t expected_object_size,
                         uint64_t expected_write_size, uint32_t flags);
    int _create_collection( KvsTransContext *txc, const coll_t &cid,
                            unsigned bits, CollectionRef *c);

    int _split_collection(KvsTransContext *txc, CollectionRef& c, CollectionRef& d, unsigned bits, int rem);
    int _kvs_replay_journal(kvs_journal_key *j);
    KvsOmapIterator* _get_kvsomapiterator(KvsCollection *c, OnodeRef &o);
    int _read_omap_keys(uint64_t lid,const ghobject_t &oid,std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, std::set<string> &keylist, bool excludeheader);
public:
    ///
    /// Background threads

    void _kv_callback_thread();
    void _kv_finalize_thread();
    void _mempool_thread();

public:

    void add_pending_write_ios(int num) {
        if (logger)
        logger->inc(l_kvsstore_pending_trx_ios, num);
    }

    void txc_aio_finish(kv_io_context *op, KvsTransContext *txc);   // called per each I/O completion

    int iterate_objects_in_device(uint64_t poolid, int8_t shardid, std::set<ghobject_t> &data, uint8_t space_id);
    ///
    /// OSR SET

    void register_osr(KvsOpSequencer *osr) {
        std::lock_guard<std::mutex> l(osr_lock);
        osr_set.insert(osr);
    }
    void unregister_osr(KvsOpSequencer *osr) {
        std::lock_guard<std::mutex> l(osr_lock);
        osr_set.erase(osr);
    }


};

/**inline ostream& operator<<(ostream& out, const KvsOpSequencer& s) {
    return out <<*s.;
}**/


static inline void intrusive_ptr_add_ref(KvsOpSequencer *o) {
    o->get();
}
static inline void intrusive_ptr_release(KvsOpSequencer *o) {
    o->put();
}


#endif //CEPH_KVSSTORE_H
