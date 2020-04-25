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

#include "kvio/kadi/kadi_bptree.h"
#include "kvsstore_types.h"
#include "kvsstore_db.h"

#include "include/ceph_assert.h"
#include "include/unordered_map.h"
#include "include/mempool.h"

#include "common/Finisher.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "os/ObjectStore.h"
#include "common/perf_counters.h"
#include "os/fs/FS.h"

// performance counters
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

class KvsTransContext;
class KvsOpSequencer;

class KvsStore: public ObjectStore {


public:
	KvsStoreDB db;
    std::atomic<uint64_t> lid_last  = {0};			//# Onode ID

    /// =========================================================
	/// Constructor
	/// =========================================================

    KvsStore(CephContext *cct, const string& path);
    ~KvsStore() override;

public:
    /// =========================================================
	/// ObjectStore Interface
	/// =========================================================

    string get_type() override { return "kvsstore"; }

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

    int statfs(struct store_statfs_t *buf,
                osd_alert_list_t* alerts = nullptr) override;
    int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
          bool *per_pool_omap) override;

    bool exists(CollectionHandle &c_, const ghobject_t& oid) override;

    //void prefetch_onode(const coll_t& cid, const ghobject_t* oid) override;
    void prefetch_onode(const coll_t &cid, const ghobject_t *oid);

    int set_collection_opts( const coll_t& cid, const pool_opts_t& opts);
    int set_collection_opts(CollectionHandle &c, const pool_opts_t &opts) override;

    int stat(CollectionHandle &c, const ghobject_t &oid, struct stat *st, bool allow_eio= false) override;

    int read(CollectionHandle &c,const ghobject_t& oid,
    		uint64_t offset,size_t len,bufferlist& bl,uint32_t op_flags = 0) override;

    int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
    int fiemap(CollectionHandle &c, const ghobject_t& oid,uint64_t offset, size_t len, bufferlist& bl) override;

    int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap);
    int fiemap(CollectionHandle &c, const ghobject_t& oid, uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap) override;

    int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
  	      bufferptr& value) override;

    int getattrs(CollectionHandle &c, const ghobject_t& oid,
  	       map<string,bufferptr>& aset) override;

    int list_collections(vector<coll_t>& ls) override;

    CollectionHandle open_collection(const coll_t &c) override;
    // added
    CollectionHandle create_new_collection(const coll_t &c) override;
    void set_collection_commit_queue(const coll_t &cid,
                                     ContextQueue *commit_queue) override;

    bool collection_exists(const coll_t& c) override;
    int collection_empty(CollectionHandle &c, bool *empty) override;
    int collection_bits(CollectionHandle &c) override;

    //int collection_list(const coll_t& cid, const ghobject_t& start, const ghobject_t& end, int max, vector<ghobject_t> *ls, ghobject_t *next);
    int collection_list(CollectionHandle &c, const ghobject_t& start, const ghobject_t& end, int max, vector<ghobject_t> *ls, ghobject_t *next) override;

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
public:
    /// OMAP functions
    class OmapIteratorImpl;

    int omap_get(
      CollectionHandle &c,     ///< [in] Collection containing oid
      const ghobject_t &oid,   ///< [in] Object containing omap
      bufferlist *header,      ///< [out] omap header
      map<string, bufferlist> *out /// < [out] Key to value map
      ) override;

    int _omap_get(
      KvsCollection *c,     ///< [in] Collection containing oid
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

    void _do_omap_clear(KvsTransContext *txc, OnodeRef &o);
	int _omap_clear(KvsTransContext *txc, CollectionRef &c, OnodeRef &o);
	int _omap_setkeys(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
			bufferlist &bl);
	int _omap_setheader(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
			bufferlist &header);
	int _omap_rmkeys(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
			bufferlist &bl);
	int _omap_rmkey_range(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
			const string &first, const string &last);


public:

    /// =========================================================
	/// Utility Functions
	/// =========================================================

    // for mount & umount
    int _open_fsid(bool create);
    int _read_fsid(uuid_d *uuid);
    int _write_fsid();
    void _close_fsid();
    int _lock_fsid();
    int _open_path();
    void _close_path();
    int _read_sb();
    int _write_sb();

    int get_predefinedID(const std::string& key);

public:
    // OP Sequencer
    void _osr_attach(KvsCollection *c);
    void _osr_register_zombie(KvsOpSequencer *osr);
    void _osr_drain(KvsOpSequencer *osr);
    void _osr_drain_all();
    void _make_ordered_transaction_list(KvsTransContext *txc, deque<KvsTransContext*> &list);
public:
    /// =========================================================
	/// Internal Threads for KvsStore
	/// =========================================================

    void _kv_callback_thread();
    void _kv_finalize_thread();
    void _kv_index_thread();


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

    struct KVIndexThread : public Thread {
		KvsStore *store;
		explicit KVIndexThread(KvsStore *s) : store(s) {}
		void *entry() {
			store->_kv_index_thread();
			return NULL;
		}
    };



public:
    /// KvsStore public interface

    void txc_aio_finish(kv_io_context *op, KvsTransContext *txc);

public:
    /// =========================================================
	/// Implementation of ObjectStore Interface
	/// =========================================================
    void _do_write_stripe(KvsTransContext *txc, OnodeRef o, kvs_stripe *stripe);
    kvs_stripe* _write_stripe(OnodeRef o, bufferlist& orig_bl, uint64_t dataoff, const uint64_t stripeoff, const int64_t off, const uint64_t use, const bool need_read);

    kvs_stripe* get_stripe_for_write(OnodeRef o,int stripe_off);
    kvs_stripe* get_stripe_for_rmw(OnodeRef o, int stripe_off);
    kvs_stripe* get_stripe_for_read(OnodeRef o, int stripe_off, bool &cachehit);

    void _do_remove_stripe(KvsTransContext *txc, OnodeRef o, uint64_t offset);

    int _do_write(KvsTransContext *txc, OnodeRef o, uint64_t offset, uint64_t length,
                  bufferlist& bl, uint32_t fadvise_flags);

    int _do_read( OnodeRef o,uint64_t offset,size_t len,bufferlist& bl,uint32_t op_flags = 0);
	int _fiemap(CollectionHandle &c_, const ghobject_t& oid, uint64_t offset, size_t len, interval_set<uint64_t>& destset);
	int _flush_cache();
	int _open_super();
	int _open_db(bool create);
	void _close_db();
	CollectionRef _get_collection(const coll_t& cid);
	int _open_collections();
	void _reap_collections();
	void _txc_add_transaction(KvsTransContext *txc, Transaction *t);

	int _touch(KvsTransContext *txc,CollectionRef& c,OnodeRef &o);

	int _write(KvsTransContext *txc,CollectionRef& c,OnodeRef& o,uint64_t offset, size_t len, bufferlist& bl,uint32_t fadvise_flags);

	int _update_write_buffer(OnodeRef &o, uint64_t offset, size_t length, bufferlist *towrite, bufferlist &out, bool truncate);
	int _do_write(KvsTransContext *txc, CollectionRef& c,OnodeRef o,uint64_t offset, uint64_t length,bufferlist& bl, uint32_t fadvise_flags);
	void _txc_write_onodes(KvsTransContext *txc);

	void _txc_state_proc(KvsTransContext *txc);
	void _txc_aio_submit(KvsTransContext *txc);
	void _txc_finish_io(KvsTransContext *txc);

	void _txc_finish(KvsTransContext *txc, KvsOpSequencer::q_list_t &releasing_txc);
	void _reap_transactions(KvsOpSequencer::q_list_t  &releasing_txc);
	void _queue_reap_collection(CollectionRef& c);

	int _setattrs(KvsTransContext *txc,CollectionRef& c, OnodeRef& o,const map<string,bufferptr>& aset);

	int _journal_replay();
	int _fsck();
	int _fsck_with_mount();
	int _fiemap(CollectionHandle &c_,const ghobject_t& oid, uint64_t offset,
			size_t len, map<uint64_t, uint64_t>& destmap);

	int _rename(KvsTransContext *txc, CollectionRef& c,
						   OnodeRef& oldo, OnodeRef& newo,
						   const ghobject_t& new_oid);
	int _read_data(KvsTransContext *txc, const ghobject_t &oid, uint64_t offset, size_t length, bufferlist &bl);
	KvsTransContext *_txc_create(KvsCollection *c, KvsOpSequencer *osr);

	int _collection_list(
			KvsCollection *c, const ghobject_t& start, const ghobject_t& end, int max,
			vector<ghobject_t> *ls, ghobject_t *pnext, bool destructive = false);
	int _do_remove_collection(KvsTransContext *txc, CollectionRef *c);
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
	int _do_truncate(KvsTransContext *txc,OnodeRef o, uint64_t offset);
	int _remove(KvsTransContext *txc, CollectionRef& c, OnodeRef &o);
	int _setattr(KvsTransContext *txc, CollectionRef& c, OnodeRef& o, const string& name, bufferptr& val);
	int _rmattr(KvsTransContext *txc,CollectionRef& c,OnodeRef& o,const string& name);
	int _rmattrs(KvsTransContext *txc,CollectionRef& c,OnodeRef& o);
	int _clone(KvsTransContext *txc,CollectionRef& c, OnodeRef& oldo,OnodeRef& newo);
	int _clone_range(KvsTransContext *txc,CollectionRef& c,OnodeRef& oldo,OnodeRef& newo,
								uint64_t srcoff, uint64_t length, uint64_t dstoff);

	int _set_alloc_hint( KvsTransContext *txc, CollectionRef& c, OnodeRef& o, uint64_t expected_object_size,
						 uint64_t expected_write_size, uint32_t flags) { return 0; }
	int _create_collection( KvsTransContext *txc, const coll_t &cid,
							unsigned bits, CollectionRef *c);

	int _split_collection(KvsTransContext *txc, CollectionRef& c, CollectionRef& d, unsigned bits, int rem);
	int _merge_collection(KvsTransContext *txc, CollectionRef *c, CollectionRef& d, unsigned bits);
	int _kvs_replay_journal(kvs_journal_key *j);
	void _delete_journal();
	void _init_perf_logger(CephContext *cct);


public:
    /// =========================================================
	/// Member Variables
	/// =========================================================

    uuid_d fsid;
    int path_fd = -1;  ///< open handle to $path
    int fsid_fd = -1;  ///< open handle (locked) to $path/fsid
    int csum_type = 0;
    bool mounted = false;

    //# Collections -----------------------

    // TODO: change RWLock to shared_mutex: e.g. ceph::shared_mutex coll_lock = ceph::make_shared_mutex("KvsStore::coll_lock");  ///< rwlock to protect coll_map

    ceph::shared_mutex coll_lock = ceph::make_shared_mutex("KvsStore::coll_lock");  ///< rwlock to protect coll_map
    mempool::kvsstore_cache_other::unordered_map<coll_t, CollectionRef> coll_map;
    map<coll_t, CollectionRef> new_coll_map;
    list<CollectionRef> removed_collections;

    //# Protect zombie_osr_set -------------------------------------

    std::mutex zombie_osr_lock; // = ceph::make_mutex("KvsStore::zombie_osr_lock");
    uint32_t next_sequencer_id = 0;
    std::map<coll_t,OpSequencerRef> zombie_osr_set;

    //# Onode & Data cache  ----------------------------------------

    vector<KvsOnodeCacheShard*>  onode_cache_shards;
    vector<KvsBufferCacheShard*> buffer_cache_shards;



    //# Iterators --------------------------------------------------
    std::mutex compact_lock;
    //LsCache<ghobject_t, KvsStore> lscache;
    //vector<KvsCollection *> cached_collections;

    //# Finishers --------------------------------------------------

    Finisher finisher;


    //# Internal Thread --------------------------------------------

    std::mutex kv_lock; //  = ceph::make_mutex("KvsStore::kv_lock");               ///< control kv threads
    std::condition_variable kv_cond;

    std::mutex kv_finalize_lock; //  = ceph::make_mutex("KvsStore::kv_finalize_lock");
    std::condition_variable kv_finalize_cond;

    std::atomic_bool kv_stop = { false };
    bool kv_index_stop = false;
    bool kv_callback_started = false;
    bool kv_finalize_started = false;
    bool kv_finalize_stop = false;

    KVCallbackThread kv_callback_thread;
    KVFinalizeThread kv_finalize_thread;
    KVIndexThread    kv_index_thread;

    deque<KvsTransContext*> kv_committing_to_finalize;   ///< pending finalization

    std::mutex writing_lock;
    //map<const ghobject_t, KvsStoreDataObject* > pending_datao;

    //# Perfcounters ------------------------------------------------

    PerfCounters *logger = nullptr;

    //# Superblock  --------------------------------------------------

    kvsstore_sb_t kvsb;

};



#endif /* SRC_OS_KVSSTORE_KVSSTORE_H_ */
