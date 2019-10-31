//
// Created by root on 10/12/18.
//

#ifndef CEPH_KVSSTORE_TYPES_H
#define CEPH_KVSSTORE_TYPES_H

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

#include "include/ceph_assert.h"
#include "include/unordered_map.h"
#include "include/mempool.h"
//#include "include/memory.h"

#include "common/Finisher.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "common/Clock.h"
#include "os/ObjectStore.h"
#include "os/fs/FS.h"
#include "kvsstore_ondisk.h"
#include "kadi/kadi_types.h"
//#include "kvsstore_kadi.h"


#define KVS_OBJECT_MAX_SIZE 2*1024*1024
#define KVSSTORE_POOL_VALUE_SIZE 8192

enum constants {
	OMAP_KEY_MAX_SIZE = 241,
	COLL_NAME_MAX_SIZE = 250,
	MAX_BATCH_VALUE_SIZE = 8192
};
class KvsStore;
struct KvsOnodeSpace;

class KvsOpSequencer;
using OpSequencerRef = ceph::ref_t<KvsOpSequencer>;
//typedef boost::intrusive_ptr<KvsOpSequencer> OpSequencerRef;

struct KvsCollection;
typedef boost::intrusive_ptr<KvsCollection> CollectionRef;

enum {
    KVS_ONODE_CREATED        = -1,
    KVS_ONODE_FETCHING       =  0,
    KVS_ONODE_DOES_NOT_EXIST =  2,
    KVS_ONODE_PREFETCHED     =  3,
    KVS_ONODE_VALID          =  4
};

static const std::map<std::string, int> builtin_omapkeys = {
        {"snapper",  2},
        {"_info", 3},
        {"_", 4},
        {"snapset", 5},
        {"0000000007.00000000000000000001",6},
        {"_epoch", 7},
        {"_infover", 8},
        {"_biginfo", 9},
        {"may_include_deletes_in_missing", 10},
        {"_fastinfo", 11}
};

struct __attribute__((__packed__)) kvs_var_object_key
{
    uint32_t         grouphash;                    //4B
    uint8_t          group;                        //1B
    uint64_t         poolid;                       //8B 13
    int8_t           shardid;                      //1B 14
    uint8_t          spaceid;                     // KSID
    uint32_t         bitwisekey;                   //4B 18
    uint64_t         snapid;                       //8B 26
    uint64_t         genid;                        //8B 34
    char             name[1024];                   //221B - MAX SIZE is 217. set to 1024 to avoid possible buffer overflow during encoding
};

struct __attribute__((__packed__)) kvs_journal_key
{
    uint32_t         hash;
    uint8_t          group;
    uint64_t         index;                        // 8B
    uint8_t          reserved1;
    uint8_t          reserved2;
    uint8_t          reserved3;
};

struct __attribute__((__packed__)) kvs_coll_key
{
    uint32_t         hash;
    uint8_t          group;
    char             name[250];                      //15B
};

struct __attribute__((__packed__)) kvs_omap_key_header
{
    uint8_t  hdr;
    uint64_t lid;
};

struct __attribute__((__packed__)) kvs_omap_key
{
   uint32_t	     hash;
   uint8_t       group;
   uint64_t       lid;  // unique key
   uint8_t       isheader;  // unique key
   uint8_t       spaceid; //KSID
   char		     name[OMAP_KEY_MAX_SIZE];// 255-13B
};

struct __attribute__((__packed__)) kvs_omapheader_key
{
    uint32_t	  hash;     // 4
    uint8_t       group;    // 1
    uint64_t      lid;      // unique key       8  13 bytes
    uint8_t       reserved1;
    uint8_t       reserved2;
    uint8_t       reserved3;
};


struct __attribute__((__packed__)) kvs_sb_key
{
    uint32_t         prefix;                        //4B
    uint8_t          group;                         //1B
    char             name[11];                      //11B
};


class KvsMemPool {

public:
    static kv_key *Alloc_key(int keybuf_size = 256) {
        kv_key *key = (kv_key *)calloc(1, sizeof(kv_key));
        if (key ==0) ceph_assert("failed to allocate memory");
        key->key = alloc_memory(get_aligned_size(keybuf_size, 256));
        key->length = keybuf_size;
        return key;
    }

    static kv_value *Alloc_value(int valuesize = KVSSTORE_POOL_VALUE_SIZE, bool needfree = true)  {
        kv_value *value = (kv_value *)calloc(1, sizeof(kv_value));
        if (value ==0) ceph_assert("failed to allocate memory");
        if (needfree) {
            value->value  = alloc_memory(get_aligned_size(valuesize, 4096));
        }
        value->length = valuesize;
        value->needfree = (needfree)? 1:0;

        return value;
    }

    static inline int get_aligned_size(const int valuesize, const int align) {
        return ((valuesize + align -1 ) / align) * align;
    }

    static inline void* alloc_memory(const int valuesize) {
        return calloc(1, valuesize);
    }

     static inline void free_memory(void *ptr) {
        free(ptr);
    }

    static void Release_key(kv_key *key) {
        assert(key != 0);
        free((void*)key->key);
        free(key);
    }

    static void Release_value (kv_value *value) {
        assert(value != 0);
        if (value->needfree && value->length > 0)
            free((void*)value->value);
        free(value);
    }

};

/// an in-memory object
struct KvsOnode {
    MEMPOOL_CLASS_HELPERS();

    std::atomic_int nref;  ///< reference count
    KvsCollection *c;
    ghobject_t oid;
    boost::intrusive::list_member_hook<> lru_item;
    kvsstore_onode_t onode;  ///< metadata stored as value in kv store
    bool exists;              ///< true if object logically exists
    bool prefetched;

    int status;

    // track txc's that have not been committed to kv store (and whose
    // effects cannot be read via the kvdb read methods)
    std::atomic<int> flushing_count = {0};
    ceph::mutex flush_lock = ceph::make_mutex("KvsStore::flush_lock");  ///< protect flush_txns
    ceph::condition_variable flush_cond;   ///< wait here for uncommitted txns
    ceph::mutex prefetch_lock = ceph::make_mutex("KvsStore::prefetched_lock");  ///< protect flush_txns
    ceph::condition_variable prefetch_cond;   ///< wait here for uncommitted txns

    KvsOnode(KvsCollection *c, const ghobject_t& o)
            : nref(0),
              c(c),
              oid(o),
              exists(false),
			  prefetched(false),
              status(KVS_ONODE_CREATED){
    }

    void flush();
    void get() {
        ++nref;
    }
    void put() {
        if (--nref == 0)
            delete this;
    }

    uint8_t get_omap_index(std::string &key) {
        static std::atomic<uint8_t> index = {0};
        auto it = builtin_omapkeys.find(key);
        if (it != builtin_omapkeys.end()) {
            return it->second;
        }

        // TODO: add omap entry to the map
        return 12 + (++index);
    }
};

typedef boost::intrusive_ptr<KvsOnode> OnodeRef;

static inline void intrusive_ptr_add_ref(KvsOnode *o) {
    o->get();
}
static inline void intrusive_ptr_release(KvsOnode *o) {
    o->put();
}



struct ReadCacheBuffer
{
    KvsOnodeSpace *space;
    bufferlist buffer;
    ghobject_t oid;
    std::atomic_int nref;  ///< reference count
    boost::intrusive::list_member_hook<> lru_item;

    ReadCacheBuffer(KvsOnodeSpace *space_, const ghobject_t& o, bufferlist* buffer_):
            space(space_), oid (o), nref(0) {
        // deep copy
        buffer = *buffer_;
    }

    ~ReadCacheBuffer() {}

    void get() {
        ++nref;
    }

    void put() {
        if (--nref == 0)
            delete this;
    }

    inline unsigned int length() {
        return buffer.length();
    }
};

typedef boost::intrusive_ptr<ReadCacheBuffer> ReadCacheBufferRef;

static inline void intrusive_ptr_add_ref(ReadCacheBuffer *o) {
    o->get();
}
static inline void intrusive_ptr_release(ReadCacheBuffer *o) {
    o->put();
}


/// a cache (shard) of onodes and buffers
struct KvsCache {
    CephContext* cct;
    PerfCounters *logger;
    std::recursive_mutex lock;          ///< protect lru and other structures
    std::recursive_mutex datalock;          ///< protect lru and other structures
    uint64_t max_readcache;
    uint64_t max_onodes;
    static KvsCache *create(CephContext* cct, PerfCounters *logger);

    KvsCache(CephContext* _cct) : cct(_cct), logger(nullptr) {}
    virtual ~KvsCache() {}

    virtual void _add_onode(OnodeRef& o, int level) = 0;
    virtual void _rm_onode(OnodeRef& o) = 0;
    virtual void _touch_onode(OnodeRef& o) = 0;
    virtual void _add_data(ReadCacheBufferRef&, int level) {}
    virtual void _rm_data(ReadCacheBufferRef&) {};
    virtual void _touch_data(ReadCacheBufferRef&) {};

    virtual uint64_t _get_num_onodes() = 0;
    virtual uint64_t _get_buffer_bytes() = 0;

    void trim();

    void trim_all();

    virtual void _trim(uint64_t onode_max, uint64_t buffer_max) = 0;

    bool empty() {
        std::lock_guard<std::recursive_mutex> l(lock);
        return _get_num_onodes() == 0 && _get_buffer_bytes() == 0;
    }
};

/// simple LRU cache for onodes and buffers
struct KvsLRUCache : public KvsCache  {
private:
    CephContext* cct;
    typedef boost::intrusive::list<
            KvsOnode,
            boost::intrusive::member_hook<
                    KvsOnode,
                    boost::intrusive::list_member_hook<>,
                    &KvsOnode::lru_item> > onode_lru_list_t;
    typedef boost::intrusive::list<
            ReadCacheBuffer,
            boost::intrusive::member_hook<
                    ReadCacheBuffer,
                    boost::intrusive::list_member_hook<>,
                    &ReadCacheBuffer::lru_item> > buffer_lru_list_t;
    onode_lru_list_t onode_lru;
    buffer_lru_list_t buffer_lru;
    uint64_t buffer_size;

public:
    KvsLRUCache(CephContext* _cct) : KvsCache(_cct), cct(_cct), buffer_size(0) {}
    uint64_t _get_num_onodes()  {
        return onode_lru.size();
    }
    void _add_onode(OnodeRef& o, int level) override {
        if (level > 0)
            onode_lru.push_front(*o);
        else
            onode_lru.push_back(*o);
    }
    void _rm_onode(OnodeRef& o) override {
        auto q = onode_lru.iterator_to(*o);
        onode_lru.erase(q);
    }

    void _touch_onode(OnodeRef& o) override;

    uint64_t _get_num_data()  {
        return buffer_lru.size();
    }



    void _add_data(ReadCacheBufferRef& o, int level) override {
        if (level > 0)
            buffer_lru.push_front(*o);
        else
            buffer_lru.push_back(*o);

        buffer_size += o->length();
    }

    void _rm_data(ReadCacheBufferRef& o) override;


    uint64_t _get_buffer_bytes() override {
        return buffer_size;
    }

    void _touch_data(ReadCacheBufferRef &o) override;

    void _trim(uint64_t onode_max, uint64_t buffer_max) override;

};


struct KvsOnodeSpace {
public:
    KvsCache *cache;

    /// forward lookups
    mempool::kvsstore_cache_other::unordered_map<ghobject_t,OnodeRef> onode_map;
    mempool::kvsstore_cache_other::unordered_map<ghobject_t,ReadCacheBufferRef> data_map;

    friend class Collection; // for split_cache()

public:
    KvsOnodeSpace(KvsCache *c, KvsCache *d) : cache(c){}
    ~KvsOnodeSpace() {
        clear();
    }

    //OnodeRef test();


    OnodeRef add(const ghobject_t& oid, OnodeRef o);
    OnodeRef lookup(const ghobject_t& o);
    void remove(const ghobject_t& oid); 
    bool invalidate_data(const ghobject_t &oid);
    bool invalidate_onode(const ghobject_t &oid);
    void add_data(const ghobject_t &oid, ReadCacheBufferRef p);
    ReadCacheBufferRef lookup_data(const ghobject_t &oid);

    void remove_data(const ghobject_t& oid) { data_map.erase(oid); }


    void clear();
    bool empty();

    void dump(CephContext *cct, int lvl);

    /// return true if f true for any item
    bool map_any(std::function<bool(OnodeRef)> f);
};


struct iter_param
{
    bool valid;
    int8_t shardid;
    uint64_t poolid;
    uint32_t starthash = 0;
    uint32_t endhash = 0;
};

#define dout_context cct
#define dout_subsys ceph_subsys_kvs

template<class T, class DB>
class LsCache
{
    class LsData
    {
        ceph::mutex d_lock = ceph::make_mutex("KvsStore::d_lock");
        ceph::mutex i_lock = ceph::make_mutex("KvsStore::i_lock");
        bool loaded;
        bool buffering;
    public:    
        std::set<T> data;
        std::vector<bool> incoming_op_w;
        std::vector<T> incoming_data;
        
        uint64_t poolid;
        int8_t shardid;
        utime_t ttl;
        CephContext *cct;

        LsData(CephContext *cct_, uint64_t &pid, int8_t sid):  loaded(false),buffering(false), poolid(pid), shardid(sid),cct(cct_) {}

        utime_t fill(DB *db, uint8_t space_id) {
            std::unique_lock lk{d_lock};
            if (loaded) {
                return utime_t();
            }

            utime_t st = ceph_clock_now();

            buffering = true;
            db->iterate_objects_in_device(poolid, shardid, data, space_id);
            buffering = false;

            utime_t et = ceph_clock_now();

            ttl = et;
            ttl += 60*2;

            apply_outstanding_requests();

            loaded = true;
            
            return et-st;
        }

        int apply_outstanding_requests()
        {
            std::unique_lock lk{i_lock};
            int size = incoming_op_w.size();
            for (int i = 0 ; i < size; i++) {
                bool write = incoming_op_w[i];
                const T& d = incoming_data[i];
                if (write)
                    data.insert(d);
                else
                    data.erase(d);
            }
            incoming_op_w.clear();
            incoming_data.clear();
            return size;
        }

        bool insert(const T &item) {
            std::unique_lock lck(d_lock,std::defer_lock);
            bool inserted = false;
            if (!lck.try_lock()) {
                std::unique_lock lk{i_lock};
                incoming_op_w.push_back(true);
                incoming_data.push_back(item);
                inserted = true;
            } else {
                if (loaded) {
                    data.insert(item);
                    inserted = true;
                } else if (buffering) {
                    std::unique_lock lk{i_lock};
                    incoming_op_w.push_back(true);
                    incoming_data.push_back(item);
                    inserted = true;
        
                }
            }
            return inserted;
        }

        bool remove(const T &item) {
            std::unique_lock lck(d_lock,std::defer_lock);
            bool removed = false;
            if (!lck.try_lock()) {
                std::unique_lock lk(i_lock);
                incoming_op_w.push_back(false);
                incoming_data.push_back(item);                 
                removed = true;
            } else {
                if (loaded) {
                    data.erase(item);
                    removed = true;
                } else if (buffering) {
                    std::unique_lock lk(i_lock);
                    incoming_op_w.push_back(false);
                    incoming_data.push_back(item);                
                    removed = true;
                }
            } 
            return removed;
        }

        
        inline void lock() {
            d_lock.lock();
        }
        inline void unlock() {
            d_lock.unlock();
        }

        bool trim(bool force = false) {
            std::unique_lock lck(d_lock,std::defer_lock);
            if (!loaded) {
                return false;
            }
            if (lck.try_lock()) {
                if (force || ceph_clock_now() > ttl) {
                    loaded = false; 
                    ttl = ceph_clock_now();
                    data.clear();
                    return true;
                }
            }
            
            return false;
        }
    }; 
 
    // poolid <-> pool data
    ceph::mutex pool_lock  = ceph::make_mutex("KvsStore::pool_lock");
    std::unordered_map<uint64_t, std::unordered_map<int8_t, LsData*> > cache;
    std::vector<LsData *> dirtylist;

    LsData *get_lsdata(int8_t shardid, uint64_t poolid, bool create) {
        std::unique_lock lock{pool_lock};
        auto &lsdata_list = cache[poolid];
        
        LsData *d = lsdata_list[shardid];
        if (d == 0 && create) {
            d = new LsData(cct, poolid, shardid); 
            lsdata_list[shardid] = d;
            dirtylist.push_back(d);
        }

        return d;
    }

    inline bool param_check(struct iter_param &param, const T& t) 
    {
        const uint32_t value = t.hobj.get_bitwise_key_u32();
        return value < param.endhash && value >= param.starthash;
    }

    inline bool range_check(const T& start, const T& end, const T& t)
    {
        return (t >= start && t < end);
    }
    CephContext *cct;

public:
    
    LsCache(CephContext *cct_) : cct(cct_) {

    }
  
    void trim(int i) 
    {
        int deleted = 0;
        int max = (i == -1)? INT_MAX:i;
        std::unique_lock lock{pool_lock};

        auto it = dirtylist.begin();
        while (it != dirtylist.end()) {
            LsData *t = (*it);
            if (t->trim()) {
                dirtylist.erase(it);
                deleted++;
                if (deleted == max) break;
            }
            else {
                it++;
            }
        }
    }

    void clear() 
    {
        std::unique_lock lock{pool_lock};   
        for (const auto &t : cache) {
            for (const auto &p : t.second) {
                if (p.second) delete p.second;
            }
        }
        dirtylist.clear();
        cache.clear();
    }


    void load_and_search(const T& start, const T& end, int max, struct iter_param &temp, struct iter_param &other, DB *db, std::vector<T> *ls, T *pnext, bool destructive) 
    {
        LsData *odatapage = (other.valid)? get_lsdata(other.shardid, other.poolid, true):0;
        LsData *tdatapage = (temp.valid)? get_lsdata(temp.shardid, temp.poolid, true):0;

        if (odatapage) {
            odatapage->fill(db, KEYSPACE_ONODE);
        }
        if (tdatapage) {
            tdatapage->fill(db, KEYSPACE_ONODE_TEMP);
        }

        if (odatapage) { odatapage->lock(); odatapage->apply_outstanding_requests();  }
        if (tdatapage) { tdatapage->lock(); tdatapage->apply_outstanding_requests();  }

        _search(odatapage, tdatapage, start, end, temp, other, max, ls, pnext);

        if (odatapage) { odatapage->unlock();  }
        if (tdatapage) { tdatapage->unlock();  }

        if (destructive) {
            if (odatapage) { odatapage->trim(true);  }
            if (tdatapage) { tdatapage->trim(true);  }
        }
    }

    

    int add(const T& t) {
        auto *page = get_lsdata(t.shard_id.id, t.hobj.pool + 0x8000000000000000ull, false);
        if (page) {
            if (page->insert(t)) {
                return 0;
            } else {
                return 1;
            }
        }
        return 2;
    }

    void remove(const T& t) {
        auto *page = get_lsdata(t.shard_id.id, t.hobj.pool + 0x8000000000000000ull, false);
        if (page) {
            
            page->remove(t);
        }
    }
    
    void _search(LsData *odatapage, LsData *tdatapage, const T& start, const T& end, struct iter_param &temp, struct iter_param &other, int max, std::vector<T> *ls, T *pnext) {
        //derr << "search " << odatapage << "," << tdatapage << " start " << start << ", end " << end  << ", max = " << max << dendl;

        std::set<ghobject_t>::iterator ofirst, olast, tfirst, tlast;
        if (odatapage == 0 && tdatapage == 0) {
            if (pnext) *pnext = ghobject_t::get_max();
            return;
        }

        
        ofirst = (odatapage)? odatapage->data.begin():tdatapage->data.end(); 
        olast  = (odatapage)? odatapage->data.end():tdatapage->data.end();
        tfirst = (tdatapage)? tdatapage->data.begin(): odatapage->data.end(); 
        tlast  = (tdatapage)? tdatapage->data.end(): odatapage->data.end(); 

        // merged iterator
        while (true) {
            const bool tfirst_done = (tfirst == tlast);
            const bool ofirst_done = (ofirst == olast);
            bool takeofirst;

            if (tfirst_done && ofirst_done) { //both are done 
                break;
            } 
            
            if (!tfirst_done && !ofirst_done) {
                takeofirst = (*ofirst < *tfirst);
            } else 
                takeofirst = tfirst_done;

            if (takeofirst) {
                const T &t = *ofirst;
                if (range_check(start, end, t) && param_check(other ,t )) {
                    ls->push_back(t);
                    if (ls->size() == (unsigned)max) {
                        if (pnext) *pnext  = t;
                        return;
                    }
                }
                ++ofirst;
            } else {
                const T &t = *tfirst;
                if (range_check(start, end, t) && param_check(temp , t )) {
                    ls->push_back(t);
                    if (ls->size() == (unsigned)max) {
                        if (pnext) *pnext  = t;
                        return;
                    }
                }
                ++tfirst;
            }
        }

        if (pnext) *pnext = ghobject_t::get_max();
    
    }

};
#undef dout_context
#undef dout_subsys


class KvsTransContext;
struct KvsCollection : public ObjectStore::CollectionImpl {
    KvsStore *store;
    OpSequencerRef osr;
    KvsCache *cache;       ///< our cache shard
    coll_t cid;

    kvsstore_cnode_t cnode;
    RWLock lock;
    ceph::mutex l_prefetch  = ceph::make_mutex("KvsStore::l_prefetch_lock");
    bool exists;

    // cache onodes on a per-collection basis to avoid lock
    // contention.
    KvsOnodeSpace onode_map;
    std::unordered_map<ghobject_t, KvsOnode *> onode_prefetch_map;

    ContextQueue *commit_queue;

    KvsOnode *lookup_prefetch_map(const ghobject_t &oid, bool erase) {
        KvsOnode *ret = 0;
        std::unique_lock lock{l_prefetch};
        auto it = onode_prefetch_map.find(oid);
        if (it != onode_prefetch_map.end()) {
                ret = it->second;
                if (erase) onode_prefetch_map.erase(it);
                return ret;
        }
        return ret;
    }

    void add_to_prefetch_map(const ghobject_t &oid, KvsOnode *on) {
        std::unique_lock lock{l_prefetch};
        onode_prefetch_map[oid] = on;
    }

    void split_cache(KvsCollection *dest);
    OnodeRef get_onode(const ghobject_t& oid, bool create);
    int get_data(KvsTransContext *txc, const ghobject_t& oid, uint64_t offset, size_t length, bufferlist &bl);

//    const coll_t &get_cid() override {
//        return cid;
//    }

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

    bool flush_commit(Context *c) override;
    void flush() override;
    void flush_all_but_last();

    KvsCollection(KvsStore *ns, KvsCache *ca, KvsCache *dc, coll_t c);
};

class KvsOmapIterator : public ObjectMap::ObjectMapIteratorImpl {
    CollectionRef c;
    OnodeRef o;
    string head, tail;
    KvsStore *store;
    std::set<string>:: iterator it;
public:
    std::set<string> keylist;
    std::list<std::pair<malloc_unique_ptr<char>, int> > buflist;

    KvsOmapIterator(CollectionRef c, OnodeRef o, KvsStore *store);
    virtual ~KvsOmapIterator() {}
    void makeready();
    bool header(bufferlist  &hdr);
    int seek_to_first() override;
    int upper_bound(const string &after) override;
    int lower_bound(const string &to) override;
    bool valid() override;
    int next() override;
    string key() override;
    bufferlist value() override;
    int status() override {
        return 0;
    }
};

class KvsOmapIteratorImpl: public ObjectMap::ObjectMapIteratorImpl {
    CollectionRef c;
    KvsOmapIterator *it;
public:

    KvsOmapIteratorImpl(CollectionRef c, KvsOmapIterator *it);
    virtual ~KvsOmapIteratorImpl() {
        if (it) delete it;
    }

    int seek_to_first() override;
    int upper_bound(const string &after) override;
    int lower_bound(const string &to) override;
    bool valid() override;
    int next() override;
    string key() override;
    bufferlist value() override;
    int status() override { return 0; }
};

struct KvsIoContext {
private:
    ceph::mutex lock = ceph::make_mutex("KvsStore::iocontext_lock");
    ceph::condition_variable cond;

public:
    ceph::mutex running_aio_lock  = ceph::make_mutex("KvsStore::running_aio_lock");
    atomic_bool submitted = { false };
    CephContext* cct;
    void *priv;
    utime_t start;

public:
    // commands
    std::atomic_int num_pending = {0};
    std::atomic_int num_running = {0};
    kv_batch_context batchctx;
    kv_batch_context journal;
    std::list<std::tuple<uint8_t, kv_key *, kv_value *> > pending_aios;    ///< not yet submitted
    std::list<std::tuple<uint8_t, kv_key *, kv_value *> > running_aios;    ///< submitting or submitted

    explicit KvsIoContext(CephContext* _cct) : cct(_cct), priv(0) {}

    KvsIoContext(const KvsIoContext& other) = delete;
    KvsIoContext &operator=(const KvsIoContext& other) = delete;

    bool has_pending_aios() {
        return num_pending.load() || batchctx.size() > 0 || journal.size() > 0;
    }

    inline void add(uint8_t space_id, kv_key *kvkey, kv_value *kvvalue) {
        std::unique_lock l{lock};
        pending_aios.push_back(std::make_tuple(space_id, kvkey, kvvalue));
        num_pending++;
    }

    inline void del(uint8_t space_id, kv_key *kvkey) {
        std::unique_lock l{lock};
        pending_aios.push_back(std::make_tuple(space_id, kvkey, (kv_value*)0));
        num_pending++;
    }

};

struct KvsPrefetchContext {
private:
    ceph::mutex lock = ceph::make_mutex("KvsStore::KvsPrefetch_lock");
    ceph::condition_variable cond;
public:
    CephContext* cct;
    KvsOnode *onode;
    KvsStore *store;

    std::tuple<uint8_t, kv_key *, kv_value *> command;


    explicit KvsPrefetchContext(CephContext* _cct, KvsStore* _store)
    : cct(_cct), onode(0), store(_store)
    {}
    
    ~KvsPrefetchContext() {
    	kv_key *key = std::get<1>(command);
		if (key) {
			KvsMemPool::Release_key(key);
		}

		kv_value *value = std::get<2>(command);
		if (value) {
			KvsMemPool::Release_value(value);
		}
    }

};




struct KvsTransContext  {
    MEMPOOL_CLASS_HELPERS();

    typedef enum {
        STATE_PREPARE,
        STATE_AIO_WAIT,// submitted to kv; not yet synced
        STATE_IO_DONE,
        STATE_FINISHING,
        STATE_DONE,
    } state_t;

    state_t state = STATE_PREPARE;

    const char *get_state_name() {
        switch (state) {
            case STATE_PREPARE: return "STATE_PREPARE";
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

    list<Context*> oncommits;  ///< more commit completions
    list<CollectionRef> removed_collections; ///< colls we removed
    map<const ghobject_t, bufferlist> tempbuffers;
    KvsIoContext ioc;

    bool had_ios = false;  ///< true if we submitted IOs before our kv txn
    uint64_t seq = 0;


    utime_t t0,t1,t2,t3,t4,t5,t6,t7,t8, t9;
    explicit KvsTransContext(CephContext *_cct, KvsCollection *c, KvsStore *_store, KvsOpSequencer *o,
                             list<Context *> *on_commits)
        : cct(_cct), ch(c), store(_store), osr(o), ioc(_cct)
    {
        if (on_commits)
        {
            oncommits.swap(*on_commits);
        }
    }

    ~KvsTransContext() {

    }


    void write_onode(OnodeRef &o) {
        o->status = KVS_ONODE_VALID;
        onodes.insert(o);
    }

    void removed(OnodeRef& o) {
        onodes.erase(o);
    }

    void aio_finish(kv_io_context *op);

    /*inline void update_latency(int op, uint64_t latency) {
    	//store->update_latency(op, latency);
    }*/

};


class KvsOpSequencer : public RefCountedObject{
public:
    ceph::mutex qlock = ceph::make_mutex("KvsStoreStore::OpSequencer::qlock");
    ceph::condition_variable qcond;
    
    typedef boost::intrusive::list<
            KvsTransContext,
            boost::intrusive::member_hook<
                    KvsTransContext,
                    boost::intrusive::list_member_hook<>,
                    &KvsTransContext::sequencer_item> > q_list_t;
    q_list_t q;  ///< transactions

    boost::intrusive::list_member_hook<> deferred_osr_queue_item;

    //ObjectStore::Sequencer *parent;
    KvsStore *store;
    coll_t cid;

    uint64_t last_seq = 0;

    std::atomic_int txc_with_unstable_io = {0};  ///< num txcs with unstable io
    std::atomic_int kv_committing_serially = {0};
    std::atomic_int kv_submitted_waiters = {0};

    std::atomic_bool registered = {true}; ///< registered in BlueStore's osr_set
    std::atomic_bool zombie = {false};    ///< owning Sequencer has gone away

    KvsOpSequencer(KvsStore *store, const coll_t &c);
    ~KvsOpSequencer() override;

  //  void discard() override;
    void drain();
    void flush();
    bool _is_all_kv_submitted();
    void flush_all_but_last();
    
    void drain_preceding(KvsTransContext *txc);
    bool flush_commit(Context *c);

 
    void _unregister();

    void queue_new(KvsTransContext *txc) {
       // std::lock_guard<std::mutex> l(qlock);
        std::lock_guard l(qlock);
        txc->seq = ++last_seq;
        q.push_back(*txc);
    }
};


#endif //CEPH_KVSSTORE_TYPES_H
