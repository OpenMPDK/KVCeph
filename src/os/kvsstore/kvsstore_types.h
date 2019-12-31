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

#include "include/ceph_assert.h"
#include "include/unordered_map.h"
#include "include/mempool.h"
#include "include/hash.h"
#include "common/ref.h"
#include "common/bloom_filter.hpp"
#include "common/Finisher.h"
#include "common/ceph_mutex.h"
#include "common/Throttle.h"
#include "common/perf_counters.h"
#include "common/PriorityCache.h"
#include "common/RefCountedObj.h"
#include "os/ObjectStore.h"
#include "include/types.h"
#include "include/utime.h"

#include "kvio/kvio_options.h"
#include "kvio/ondisk_types.h"
#include "kvio/kadi/kadi_types.h"
#include "kvio/keyencoder.h"
#include "kvsstore_pages.h"

class KvsStore;
class KvsTransContext;
class KvsOpSequencer;
using OpSequencerRef = ceph::ref_t<KvsOpSequencer>;

struct KvsOnodeSpace;
struct KvsCollection;
typedef boost::intrusive_ptr<KvsCollection> CollectionRef;

using namespace ceph;

enum {
    KVS_ONODE_CREATED        = -1,
    KVS_ONODE_FETCHING       =  0,
    KVS_ONODE_DOES_NOT_EXIST =  2,
    KVS_ONODE_PREFETCHED     =  3,
    KVS_ONODE_VALID          =  4
};

enum KVS_JOURNAL_ENTRY {
	KVS_JOURNAL_ENTRY_ONODE = 0,
	KVS_JOURNAL_ENTRY_COLL  = 1
};

#define KVS_ITERATOR_TYPE_SORTED    0
#define KVS_ITERATOR_TYPE_INTORDER  1

#ifndef KVSSTORE_ITERATOR_TYPE
#define KVSSTORE_ITERATOR_TYPE KVS_INORDER_ITERATOR
#endif


inline int align_4B(uint32_t length) { return ((length - 1) / 4 + 1)*4;   }

class kvs_stripe {
public:
    uint32_t len;
    uint32_t pos;
    char *buffer;
    int pgid;

    kvs_stripe(int pgid_): len(KVS_OBJECT_SPLIT_SIZE), pos(0), pgid(pgid_) {
        allocate();
    }
    ~kvs_stripe() {
        release();
    }



    void allocate() {
        buffer = (char*)malloc(len);
    }

    void set_moved() {
        buffer = 0;
    }

    void release() {
        free (buffer);
        buffer = 0;
    }

    inline uint32_t length() { return len; }

    inline void clear() {
        pos = 0;
    }

    inline void inc_pos(uint32_t newpos) {
        pos += newpos;
    }
    inline void set_pos(uint32_t newpos) {
        pos = newpos;
    }

    inline uint32_t get_pos() { return pos; }


    inline void append_zero(uint32_t l) {
        if (l + pos > KVS_OBJECT_SPLIT_SIZE) throw "end of buffer";
        if (l == 0) return;
        memset(buffer + pos, 0, l);
        inc_pos(l);
    }

    inline void append(const bufferlist& other, unsigned off, unsigned l) {
        if (l + pos > KVS_OBJECT_SPLIT_SIZE) throw "end of buffer";
        if (l == 0) return;
        other.copy(off, l, buffer + pos);  inc_pos(l);
    }

    inline void substr_of(const bufferlist& other, unsigned off, unsigned l)
    {
        clear();
        append(other, off, l);
    }

};

///  ====================================================
///  ONODE
///  ====================================================

struct KvsOnode {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref;  ///< reference count
    KvsCollection *c;
    ghobject_t oid;

    boost::intrusive::list_member_hook<> lru_item;

    kvsstore_onode_t onode;  ///< metadata stored as value in kv store
    bool exists;              ///< true if object logically exists

    int status;

    // track txc's that have not been committed to kv store (and whose
    // effects cannot be read via the kvdb read methods)
    std::atomic<int> flushing_count = {0};
    std::atomic<int> waiting_count = {0};

    std::mutex flush_lock; // = ceph::make_mutex("KvsStore::flush_lock");  ///< protect flush_txns
    std::condition_variable flush_cond;   ///< wait here for uncommitted txns
    set<KvsTransContext*> flush_txns;

    map<uint64_t,kvs_stripe*> pending_stripes;
    void clear_pending_stripes() {
       for(const auto &stripe : pending_stripes) {
            delete stripe.second;
       };
       pending_stripes.clear();
    }
    KvsOnode(KvsCollection *c, const ghobject_t& o)
            : nref(0),
              c(c),
              oid(o),
              exists(false),
              status(KVS_ONODE_CREATED) {
    }

    void flush();
    void get() {
        ++nref;
    }
    void put() {
        if (--nref == 0)
            delete this;
    }

    static KvsOnode* decode(
          CollectionRef c,
          const ghobject_t& oid,
          const bufferlist& v);

};

typedef boost::intrusive_ptr<KvsOnode> OnodeRef;

static inline void intrusive_ptr_add_ref(KvsOnode *o) {
    o->get();
}
static inline void intrusive_ptr_release(KvsOnode *o) {
    o->put();
}

///  ====================================================
///  Cache
///  ====================================================

#include "kvsstore_cache_impl.h"

///  ====================================================
///  Collections
///  ====================================================

struct KvsCollection : public ObjectStore::CollectionImpl {
    KvsStore *store;
    OpSequencerRef osr;
    KvsBufferCacheShard *cache;       ///< our cache shard
    kvsstore_cnode_t cnode;
    ceph::shared_mutex lock = ceph::make_shared_mutex("KvsCollection::lock", true, false);

    bool exists;

    // cache onodes on a per-collection basis to avoid lock
    // contention.
    KvsOnodeSpace onode_map;
    KvsBufferSpace data_map;

    ContextQueue *commit_queue;


    OnodeRef get_onode(const ghobject_t& oid, bool create, bool is_createop=false);


    void split_cache(KvsCollection *dest);

    //int get_data(KvsTransContext *txc, const ghobject_t& oid, uint64_t offset, size_t length, bufferlist &bl);

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

    KvsCollection(KvsStore *ns, KvsOnodeCacheShard *oc, KvsBufferCacheShard *bc, coll_t c);
    virtual ~KvsCollection() {}
};

///  ====================================================
///  Iterators
///  ====================================================

class KvsIterator {
public:
    KvsIterator() {}
    virtual ~KvsIterator() {}
    virtual int begin() = 0;
    virtual int end() = 0;
    virtual int upper_bound(const kv_key &key) = 0;
    virtual int lower_bound(const kv_key &key) = 0;

    virtual bool valid() = 0;

    virtual int next() = 0;
    virtual int prev() = 0;

    virtual kv_key key() = 0;
};


///  ====================================================
///  Journal
///  ====================================================


struct KvsJournal {
	static std::atomic<uint64_t> journal_index;
	static const size_t MAX_JOURNAL_ENTRY_SIZE = 2*1024*1024UL;

	// journal data
	// <num_io (n)> <journal entry 0> .... <journal entry n>

	uint32_t* num_io_pos;
	char *journal_buffer;
	unsigned journal_buffer_pos;
	bool created;

	KvsJournal() {
	    FTRACE
		journal_buffer = (char*)malloc(MAX_JOURNAL_ENTRY_SIZE);
		if (journal_buffer == 0) throw "failed to allocate a journal";
		num_io_pos = (uint32_t*)journal_buffer;
		*num_io_pos = 0;
		journal_buffer_pos = 4;
        created = true;
	}

	KvsJournal(char *buffer) {
		journal_buffer = buffer;
		num_io_pos = (uint32_t*)journal_buffer;
		journal_buffer_pos = 4;
        created = false;
	}

	~KvsJournal() {
		if (created) free(journal_buffer);
	}

	inline bool is_full(int length) {
		return journal_buffer_pos + length >= MAX_JOURNAL_ENTRY_SIZE;
	}

	//const std::function<int (char *)>
	template<typename Functor>
	void add_journal_entry(Functor &&filler) {
	    FTRACE
		journal_buffer_pos += filler(journal_buffer + journal_buffer_pos);
		*num_io_pos = *num_io_pos + 1;
	}

	template<typename Functor>
	void read_journal_entry(Functor &&reader) {
		const int ne = *num_io_pos;
		for (int i =0 ;i < ne ; i++) {
			kvs_journal_entry* entry = (kvs_journal_entry*)(journal_buffer + journal_buffer_pos);
			int datapos = journal_buffer_pos + sizeof(kvs_journal_entry);
			int keypos  = datapos + align_4B(entry->length);
			reader(entry, journal_buffer + keypos, (entry->length == 0)? 0:journal_buffer + datapos);
			journal_buffer_pos = keypos + entry->key_length;
		}
	}
};


///  ====================================================
///  IO Context
///  ====================================================

struct KvsIoContext {
private:
    std::mutex lock; // = ceph::make_mutex("KvsStore::iocontext_lock");
    ceph::condition_variable cond;

public:
    std::mutex running_aio_lock; //  = ceph::make_mutex("KvsStore::running_aio_lock");
    atomic_bool submitted = { false };
    CephContext* cct;
    void *priv;
    utime_t start;
    std::atomic<int> num_running = {0};
    std::atomic<int> num_running_journals = {0};

public:
    // commands
    kv_batch_context batchctx;
    KvsJournal *cur_journal;
    std::list<KvsJournal *> journal; 		   										///< not yet submitted

    struct IoRequest {
    	uint8_t spaceid;
    	kv_key *key;
    	bufferlist* data;
    	char *raw_data;
    	int   raw_data_length;

    	IoRequest(uint8_t spaceid_, kv_key *key_, bufferlist* data_):
    		spaceid(spaceid_), key(key_), data(data_), raw_data(0), raw_data_length(0)
    	{}

    	IoRequest(uint8_t spaceid_, kv_key *key_, char *raw_data_, int raw_data_length_):
    		spaceid(spaceid_), key(key_), data(0), raw_data(raw_data_), raw_data_length(raw_data_length_)
    	{}
    };

    std::list<IoRequest*> pending_ios;    		    ///< not yet submitted
    std::list<IoRequest*> running_ios;    		    ///< not yet submitted

    explicit KvsIoContext(CephContext* _cct): cct(_cct), priv(0) {
        FTRACE
    	create_new_journal();
    }

    ~KvsIoContext() {
        FTRACE
    }

    inline void create_new_journal() {
        FTRACE
    	cur_journal = new KvsJournal;
    	journal.push_back(cur_journal);
    }

    KvsIoContext(const KvsIoContext& other) = delete;
    KvsIoContext &operator=(const KvsIoContext& other) = delete;

    template<typename Keygen>
	inline void add_to_journal(const int spaceid, const int object_type, bufferlist *bl, const Keygen &keygen) {
        FTRACE
		const int bl_length = (bl == 0)? 0:align_4B(bl->length());
		const int journal_entry_size = sizeof(kvs_journal_entry) + KVKEY_MAX_SIZE + bl_length;
		if (cur_journal->is_full(journal_entry_size)) {
			create_new_journal();
		}

		// entry + value + key  (start address of each item is 4B aligned)
		cur_journal->add_journal_entry([&] (char *buffer)-> int {
			kvs_journal_entry* entry = (kvs_journal_entry*)buffer;
			entry->spaceid 	   = spaceid;
			entry->object_type = object_type;
			entry->op_type     = (bl == 0); //1 /* delete */: 0 /* write */;
			entry->key_length  = keygen(buffer + sizeof(kvs_journal_entry) + bl_length);

			if (bl) {
				bl->copy(0, bl->length(), buffer + sizeof(kvs_journal_entry));
				entry->length = bl->length();
			} else {
				entry->length =0;
			}
			return journal_entry_size - KVKEY_MAX_SIZE + entry->key_length;
		});
	}



    bool has_pending_aios() {
        return batchctx.size() > 0 || journal.size() > 0 || pending_ios.size() > 0;
    }

    /// Add to I/O and Journal queues
    /// ----------------------------------

    inline void add_pending_bl(uint8_t space_id, bufferlist &bl, const std::function< uint8_t (void*)> &keygen) {

    	if (DISABLE_BATCH || bl.length() > MAX_BATCH_VALUE_SIZE) {
    		// add key-value pair to the pending queue
    		bufferlist* list = new bufferlist(std::move(bl));
    		pending_ios.push_back(new IoRequest(space_id, set_kv_key(keygen), list));
    	}
    	else {
    		// use batch cmd: add key-value pair to the batch buffer
    		batchctx.batch_store(space_id, 0, keygen, [&] (char* buffer)->uint32_t {
    			bl.copy(0, bl.length(), (char*)buffer);
    			return bl.length();
    		});
    	}
    }
    inline void add_pending_data(uint8_t space_id, char*data, uint32_t length, const std::function< uint8_t (void*)> &keygen) {
       	if (DISABLE_BATCH || length > MAX_BATCH_VALUE_SIZE) {
       		// add key-value pair to the pending queue
       		pending_ios.push_back(new IoRequest(space_id, set_kv_key(keygen), data, length));
       	}
       	else {
       		// use batch cmd: add key-value pair to the batch buffer
       		batchctx.batch_store(space_id, 0, keygen, [&] (char* buffer)->uint32_t {
       			memcpy((char*)buffer, data, length);
       			return length;
       		});
       	}
    }
    inline void add_pending_remove(uint8_t space_id, const std::function< uint8_t (void*)> &keygen) {
    	pending_ios.push_back(new IoRequest(space_id, set_kv_key(keygen), nullptr));
    }

    /// Transaction key & value pair
    /// ----------------------------

    // should be created on a heap so the journal can be flushed first
    inline kv_key* set_kv_key(const std::function< uint8_t (void*)> &keygen) {
    	kv_key* key = new kv_key();
    	key->key    = malloc(256);
    	key->length = keygen(key->key);
    	return key;
    }
};

struct KvsDataBlock {
	bufferlist data;
	uint32_t pos;
	KvsDataBlock(): pos(0) {}

	inline void write_data(unsigned b_off, unsigned b_len, char *src) {
		unsigned padding_len = b_off - pos;
		if (padding_len > 0) {
			data.append_zero(padding_len);
		}

		if (src) {
			data.copy_in(b_off, b_len, src, false);
		} else {
			data.zero(b_off, b_len);
		}

		// new length
		if (b_off + b_len > pos)
			pos = b_off + b_len;
	}
};

struct KvsTransContext  {
    MEMPOOL_CLASS_HELPERS();

    typedef enum {
        STATE_PREPARE,
		//STATE_JOURNAL_WAIT, // submitted journal entries. not yet synced
        STATE_AIO_WAIT,		// submitted data. not yet synced
        STATE_IO_DONE,
        STATE_FINISHING,
        STATE_DONE,
    } state_t;
    state_t state = STATE_PREPARE;

    const char *get_state_name() {
        switch (state) {
            case STATE_PREPARE: return "STATE_PREPARE";
            //case STATE_JOURNAL_WAIT: return "STATE_JOURNAL_WAIT - submitted journal IO";
            case STATE_AIO_WAIT: return "STATE_AIO_WAIT - submitted IO are done(called by c)";
            case STATE_IO_DONE: return "STATE_IO_DONE - processing IO done events (called by cb)";
            case STATE_FINISHING: return "STATE_FINISHING - releasing resources for IO (called by cb)";
            case STATE_DONE: return "done";
        }
        return "???";
    }

    CephContext *cct;
    CollectionRef ch;
    KvsStore *store;
    OpSequencerRef osr; // this should be ch->osr
    boost::intrusive::list_member_hook<> sequencer_item;

    uint64_t bytes = 0, cost = 0;

    set<OnodeRef> onodes;     ///< these need to be updated/written
    set<OnodeRef> modified_objects;  ///< objects we modified (and need a ref)

    Context *oncommit;         ///< signal on commit
    Context *onreadable;         ///< signal on readable
    Context *onreadable_sync;         ///< signal on readable
    list<Context*> 		oncommits;  		 ///< more commit completions
    list<CollectionRef> removed_collections; ///< colls we removed

    /*inline map<const ghobject_t, KvsStoreDataObject*>* get_databuffers() {
        return &databuffers;
    }

    inline KvsStoreDataObject* get_databuffer(const ghobject_t& oid) {
        auto it = databuffers.find(oid);
        if (it != databuffers.end()) {
            return it->second;
        } else {
            auto obj = new KvsStoreDataObject;
            databuffers[oid] = obj;
            return obj;
        }
    }
    */
    KvsIoContext ioc;	// I/O operations

    uint64_t seq = 0;
    utime_t t0,t1,t2,t3,t4,t5,t6,t7,t8,t9;
    bool submitted = false;
    explicit KvsTransContext(CephContext *_cct, KvsCollection *c, KvsStore *_store, KvsOpSequencer *o)
        : cct(_cct), ch(c), store(_store), osr(o), oncommit(0),onreadable(0),onreadable_sync(0), ioc(_cct)
    {
    }

    ~KvsTransContext() {
    }

    void write_onode(OnodeRef &o) {
        //o->status = KVS_ONODE_VALID;
        o->exists = true;
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
    void aio_finish(kv_io_context *op);
    void journal_finish(kv_io_context *op);
private:

    //map<const ghobject_t, KvsStoreDataObject*> databuffers; /// data to write

};


///  ====================================================
///  OpSequencer
///  ====================================================

class KvsOpSequencer : public RefCountedObject {
public:
    std::mutex qlock; // = ceph::make_mutex("KvspSequencer::qlock");
    std::condition_variable qcond;

    typedef boost::intrusive::list<KvsTransContext, boost::intrusive::member_hook<KvsTransContext,boost::intrusive::list_member_hook<>,&KvsTransContext::sequencer_item> > q_list_t;

    q_list_t q;  ///< transactions

    boost::intrusive::list_member_hook<> deferred_osr_queue_item;

    //ObjectStore::Sequencer *parent;
    KvsStore *store;
    coll_t cid;

    uint64_t last_seq = 0;

    std::atomic_int txc_with_unstable_io = {0};  ///< num txcs with unstable io
    std::atomic_int kv_committing_serially = {0};
    std::atomic_int kv_submitted_waiters = {0};

    std::atomic_bool zombie = {false};    ///< owning Sequencer has gone away

    const uint32_t sequencer_id;

    uint32_t get_sequencer_id() const {
      return sequencer_id;
    }

    void queue_new(KvsTransContext *txc) {
		std::lock_guard l(qlock);
		txc->seq = ++last_seq;
		q.push_back(*txc);
	    //TR << "queue new " <<  (void*)txc << " - queue size = " << q.size()<< "\n";
	}

	void pop_front_nolock() {
        //void *txc = &q.front();
        q.pop_front();
        //TR << "queue pop " <<  (void*)txc << " - queue size = " << q.size()<< "\n";
    }

    void drain() {
      std::unique_lock<std::mutex> l(qlock);
      //TR << "queue drain before " << q.size() << "\n";
      while (!q.empty()) {
          qcond.wait(l);
      }
      //TR << "queue drain end " << q.size() << "\n";
    }

    void drain_preceding(KvsTransContext *txc) {
      std::unique_lock<std::mutex> l(qlock);
      //TR << "queue drain proceding before " << q.size() << "\n";
      while (&q.front() != txc)
    	  qcond.wait(l);
      //TR << "queue drain proceding end" << q.size() << "\n";
    }


    bool _is_all_kv_submitted() {
      // caller must hold qlock & q.empty() must not empty
      ceph_assert(!q.empty());
      KvsTransContext *txc = &q.back();
      if (txc->state >= KvsTransContext::STATE_AIO_WAIT) {
    	  return true;
      }
      return false;
    }

    void flush() {
      std::unique_lock<std::mutex> l(qlock);
      TR << "queue flush " << q.size() << "\n";
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
      TR << "queue flush after" << q.size()<< "\n";
    }


    bool flush_commit(Context *c) {
        std::lock_guard l(qlock);
        if (q.empty()) {
            return true;
        }
        KvsTransContext *txc = &q.back();
        if (txc->state >= KvsTransContext::STATE_IO_DONE) {
            return true;
        }
        txc->oncommits.push_back(c);
        return false;
    }


    void flush_all_but_last() {
      std::unique_lock<std::mutex> l(qlock);
      if (q.size() == 0) return;
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
		  if (it->state >= KvsTransContext::STATE_AIO_WAIT) {
			--kv_submitted_waiters;
			return;
		  }
		}
		qcond.wait(l);
		--kv_submitted_waiters;
      }
    }

    KvsOpSequencer(KvsStore *store, uint32_t sequencer_id, const coll_t &c);

    ~KvsOpSequencer() {
        FTRACE
    	ceph_assert(q.empty());
    }
};

static inline void intrusive_ptr_add_ref(KvsOpSequencer *o) {
    o->get();
}
static inline void intrusive_ptr_release(KvsOpSequencer *o) {
    o->put();
}


#endif /* SRC_OS_KVSSTORE_KVSSTORE_TYPES_H_ */

