#ifndef SRC_OS_KVSSTORE_KVSSTORE_KVCMDS_H_
#define SRC_OS_KVSSTORE_KVSSTORE_KVCMDS_H_

#include <map>
#include <atomic>
#include "kvsstore_types.h"
#include "kadi/kadi_cmds.h"
#include "kadi/kadi_bptree.h"

enum KVS_JOURNAL_ENTRY {
    KVS_JOURNAL_ENTRY_ONODE = 0,
    KVS_JOURNAL_ENTRY_COLL  = 1
};

#define KVS_ITERATOR_TYPE_SORTED    0
#define KVS_ITERATOR_TYPE_INTORDER  1

#ifndef KVSSTORE_ITERATOR_TYPE
#define KVSSTORE_ITERATOR_TYPE KVS_INORDER_ITERATOR
#endif


//# SIZE
//--------------------------

#define KVS_OBJECT_MAX_SIZE 		2*1024*1024UL
#define KVSSTORE_POOL_VALUE_SIZE 	8192

static const uint32_t DEFAULT_READBUF_SIZE = 8192U;
static const uint32_t KVS_OBJECT_SPLIT_SIZE = DEFAULT_READBUF_SIZE;

#define KVS_OBJECT_SPLIT_SHIFT      13
#define KVKEY_MAX_SIZE				255
#define OMAP_KEY_MAX_SIZE 			241
#define MAX_BATCH_VALUE_SIZE 		8192

//# CACHE
//-------------------------
#define ONODE_LRUCACHE_TRIM_MAX_SKIP_PINNED 128
#define KVS_CACHE_MAX_ONODES (3*1024*1024*1024UL / 4096)
#define KVS_CACHE_MAX_DATA_SIZE 1*1024*1024*1024
#define KVS_CACHE_RESIZE_INTERVAL 1
#define KVS_CACHE_BUFFERED_READ 1


inline int align_4B(uint32_t length) { return ((length - 1) / 4 + 1)*4;   }


inline uint16_t get_chunk_index(uint64_t offset) {
    return (uint16_t) (offset >> KVS_OBJECT_SPLIT_SHIFT);
}

///  ====================================================
///  Iterator Interface
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

class KvsBptreeIterator: public KvsIterator {
    bptree tree;
    bptree_iterator *iter;

public:
    KvsBptreeIterator(KADI *adi, int ksid_skp, uint32_t prefix);

    int begin() override;
    int end() override;
    int upper_bound(const kv_key &key) override;
    int lower_bound(const kv_key &key) override;

    bool valid() override;

    int next() override;
    int prev() override;

    kv_key key() override;
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


class KvsStoreDB
{
public:
	CephContext *cct;
    KADI kadi;
	bool compaction_started;
	//std::atomic_bool is_compaction_started;
	std::mutex compact_lock;
	std::condition_variable compact_cond;

    int keyspace_sorted = 0;
    int keyspace_notsorted = 1;

    std::function< bool (uint64_t,int, int, std::vector<bufferlist*>&)>       omap_readfunc;
    std::function< void (uint64_t, bufferlist*, int, IoContext &ctx)>         omap_writefunc;
    std::function< bool (uint64_t,int start_id, int end_id, IoContext &ctx)>  omap_removefunc;

public:
    KvsStoreDB(CephContext *cct_);
    //kvaio_t* _syncio_write(int keyspaceid, bufferlist &bl, IoContext *ioc);
    //kvaio_t* _syncio_write(int keyspaceid, void *addr, uint32_t len, IoContext *ioc);
    //kvaio_t* _syncio_remove(int keyspaceid, IoContext *ioc);

    kvaio_t* _aio_write(int keyspaceid, bufferlist &bl, IoContext *ioc);
    kvaio_t* _aio_write(int keyspaceid, void *addr, uint32_t len, IoContext *ioc);
    kvaio_t* _aio_remove(int keyspaceid, IoContext *ioc);
    kvaio_t* _aio_read(int keyspaceid, uint32_t len, bufferlist *pbl, IoContext *ioc);

    void aio_read_chunk(const ghobject_t &oid, uint16_t chunkid, uint32_t len, bufferlist &bl, IoContext *ioc);
    void aio_write_chunk(const ghobject_t &oid, uint16_t chunkid, void *addr, uint32_t len, IoContext *ioc);
    void aio_remove_chunk(const ghobject_t &oid, uint16_t chunkid, IoContext *ioc);

    int  read_onode(const ghobject_t &oid, bufferlist &bl);
    void aio_read_onode(const ghobject_t &oid, bufferlist &bl, IoContext *ioc);
    void aio_write_onode(const ghobject_t &oid, bufferlist &bl, IoContext *ioc);
    void aio_remove_onode(const ghobject_t &oid, IoContext *ioc);

    int aio_write_omap_keyblock(uint64_t nid, bufferlist * iolist, int start_id, IoContext &ioc);
    int aio_read_omap_keyblock(uint64_t nid, uint32_t start_id, uint32_t end_id, std::vector<bufferlist*> &bls);
    int aio_delete_omap_keyblock(uint64_t nid, uint32_t startid, uint32_t endid, IoContext &ioc);

    void aio_read_omap( const uint64_t index, const std::string &strkey, bufferlist &bl, IoContext *ioc);
    void aio_write_omap( uint64_t index, const std::string &strkey, bufferlist &bl, IoContext *ioc);
    void aio_remove_omap( uint64_t index, const std::string &strkey, IoContext *ioc);

    void aio_read_journal(int index, bufferlist &bl, IoContext *ioc);
    void aio_write_journal(int index, void *addr, uint32_t len, IoContext *ioc);
    void aio_remove_journal(int index, IoContext *ioc);

    void aio_read_coll(const coll_t &cid, bufferlist &bl, IoContext *ioc);
    void aio_write_coll(const coll_t &cid, bufferlist &bl, IoContext *ioc);
    void aio_remove_coll(const coll_t &cid, IoContext *ioc);

    int read_sb(bufferlist &bl);
    int write_sb(bufferlist &bl);
    int aio_write_sb(bufferlist &bl, IoContext *ioc);

    int read_kvkey(kv_key *key, bufferlist &bl, bool sorted);

    //int aio_submit(IoContext *ioc);
    //int aio_submit_and_wait(IoContext *ioc);
    //int syncio_submit(IoContext *ioc);
    //int syncio_submit_and_wait(IoContext *ioc);



    inline int poll_completion(uint32_t &num_events, uint32_t timeout_us) {	return kadi.poll_completion(num_events, timeout_us); }


    inline int open(const std::string &devpath) {return kadi.open(devpath, keyspace_sorted);	}
    inline int close() { return kadi.close(); }
    inline bool is_opened() { return kadi.is_opened(); }

    uint64_t compact();
    KvsIterator *get_iterator(uint32_t prefix);



	inline int get_freespace(uint64_t &bytesused, uint64_t &capacity, double &utilization) {
		return kadi.get_freespace(bytesused, capacity, utilization);
	}

	// >=
	inline bool is_key_ge(const kv_key &key1, const kv_key &key2) {
		unsigned char *k1 = (unsigned char *)key1.key;
        unsigned char *k2 = (unsigned char *)key2.key;
		const int k1_length = key1.length;
		const int k2_length = key2.length;
		return (kvkey_lex_compare(k1, k1+k1_length, k2, k2+k2_length) >= 0);
	}

private:
	inline int kvkey_lex_compare(unsigned char* first1, unsigned char* last1, unsigned char* first2, unsigned char* last2)
	{
		for ( ; (first1 != last1) && (first2 != last2); ++first1, (void) ++first2 ) {
			if (*first1 < *first2) {
                return -1;
			}
			if (*first2 < *first1) {
			    return +1;
			}
		}

		if (first1 == last1) {
			if (first2 == last2) return 0;
			return -1;
		}

		return +1;
	}

};


#endif /* SRC_OS_KVSSTORE_KVSSTORE_KVCMDS_H_ */
