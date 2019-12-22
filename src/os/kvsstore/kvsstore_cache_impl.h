/*
 * kvsstore_cache.h
 *
 *  Created on: Nov 19, 2019
 *      Author: root
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_CACHE_IMPL_H_
#define SRC_OS_KVSSTORE_KVSSTORE_CACHE_IMPL_H_

#include <boost/intrusive/list.hpp>
#include "kvsstore_debug.h"

struct KvsOnodeSpace;

struct KvsCacheShard {
	CephContext *cct;
	PerfCounters *logger;

	/// protect lru and other structures
	ceph::recursive_mutex lock = { ceph::make_recursive_mutex("KvsCacheShard::lock") };

	std::atomic<uint64_t> max = { 0 };
	std::atomic<uint64_t> num = { 0 };

	KvsCacheShard(CephContext *cct) :
			cct(cct), logger(nullptr) {
	}
	virtual ~KvsCacheShard() {
	}

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


//------------------------------------------------------------
// LRU cache for onodes, @borrowed from Bluestore
//------------------------------------------------------------

/// A Generic onode Cache Shard
struct KvsOnodeCacheShard: public KvsCacheShard {
	std::array<std::pair<ghobject_t, mono_clock::time_point>, 64> dumped_onodes;
public:
	KvsOnodeCacheShard(CephContext *cct) :
			KvsCacheShard(cct) {
	}
	static KvsOnodeCacheShard* create(CephContext *cct, string type,
			PerfCounters *logger);
	virtual void _add(OnodeRef &o, int level) = 0;
	virtual void _rm(OnodeRef &o) = 0;
	virtual void _touch(OnodeRef &o) = 0;
	virtual void add_stats(uint64_t *onodes) = 0;

	bool empty() {
		return _get_num() == 0;
	}
};


// LruOnodeCacheShard
struct KvsLruOnodeCacheShard: public KvsOnodeCacheShard {
	typedef boost::intrusive::list<
			KvsOnode,
			boost::intrusive::member_hook<KvsOnode,
			boost::intrusive::list_member_hook<>, &KvsOnode::lru_item> > list_t;
	list_t lru;

	explicit KvsLruOnodeCacheShard(CephContext *cct) :
			KvsOnodeCacheShard(cct) {
	}

	void _add(OnodeRef &o, int level) override
	{
		(level > 0) ? lru.push_front(*o) : lru.push_back(*o);
		num = lru.size();
	}
	void _rm(OnodeRef &o) override
	{
		lru.erase(lru.iterator_to(*o));
		num = lru.size();
	}
	void _touch(OnodeRef &o) override
	{
		lru.erase(lru.iterator_to(*o));
		lru.push_front(*o);
		num = lru.size();
	}
	void _trim_to(uint64_t max) override;

	void add_stats(uint64_t *onodes) override
	{
		*onodes += num;
	}
};

//------------------------------------------------------------
// Read Cache - LRU cache is borrowed from Bluestore
//------------------------------------------------------------
struct KvsBufferSpace;

struct KvsBuffer
{
    KvsBufferSpace *space;
    bufferlist data;
    ghobject_t oid;
    std::atomic_int nref;  ///< reference count
    boost::intrusive::list_member_hook<> lru_item;

    KvsBuffer(KvsBufferSpace *space_, const ghobject_t& o, bufferlist* buffer_):
            space(space_), oid (o), nref(0) {
    	// TODO: make caller guarantee bufferlist is allocated in heap to avoid memory copy
        data = *buffer_;
    }

    ~KvsBuffer() {}

    void get() {
        ++nref;
    }

    void put() {
        if (--nref == 0)
            delete this;
    }

    inline unsigned int length() {
        return data.length();
    }
};

typedef boost::intrusive_ptr<KvsBuffer> KvsBufferRef;

static inline void intrusive_ptr_add_ref(KvsBuffer *o) {
    o->get();
}
static inline void intrusive_ptr_release(KvsBuffer *o) {
    o->put();
}


/// A Generic buffer Cache Shard
struct KvsBufferCacheShard : public KvsCacheShard {
  std::atomic<uint64_t> num_extents = {0};
  std::atomic<uint64_t> num_blobs = {0};
  uint64_t buffer_bytes = 0;

public:
  KvsBufferCacheShard(CephContext* cct) : KvsCacheShard(cct) {}
  static KvsBufferCacheShard *create(CephContext* cct, string type,
                                  PerfCounters *logger);
  virtual void _add(KvsBuffer *b, int level, KvsBuffer *near) = 0;
  virtual void _rm(KvsBuffer *b) = 0;
  virtual void _move(KvsBufferCacheShard *src, KvsBuffer *b) = 0;
  virtual void _touch(KvsBuffer *b) = 0;
  virtual void _adjust_size(KvsBuffer *b, int64_t delta) = 0;

  uint64_t _get_bytes() {
    return buffer_bytes;
  }

  virtual void add_stats(uint64_t *buffers, uint64_t *bytes) = 0;

  bool empty() {
    std::lock_guard l(lock);
    return _get_bytes() == 0;
  }
};

// LruBufferCacheShard
struct KvsLruBufferCacheShard : public KvsBufferCacheShard {
  typedef boost::intrusive::list<
    KvsBuffer,
    boost::intrusive::member_hook<
	KvsBuffer,
      boost::intrusive::list_member_hook<>,
      &KvsBuffer::lru_item> > list_t;
  list_t lru;

  explicit KvsLruBufferCacheShard(CephContext *cct) : KvsBufferCacheShard(cct) {}

  void _add(KvsBuffer *b, int level, KvsBuffer *near) override {
      FTRACE
    if (near) {
      auto q = lru.iterator_to(*near);
      lru.insert(q, *b);
    } else if (level > 0) {
      lru.push_front(*b);
    } else {
      lru.push_back(*b);
    }
    buffer_bytes += b->length();
    num = lru.size();
  }
  void _rm(KvsBuffer *b) override {
      FTRACE
    ceph_assert(buffer_bytes >= b->length());
    buffer_bytes -= b->length();
    auto q = lru.iterator_to(*b);
    lru.erase(q);
    num = lru.size();
  }
  void _move(KvsBufferCacheShard *src, KvsBuffer *b) override {
      FTRACE
    src->_rm(b);
    _add(b, 0, nullptr);
  }
  void _adjust_size(KvsBuffer *b, int64_t delta) override {
      FTRACE
    ceph_assert((int64_t)buffer_bytes + delta >= 0);
    buffer_bytes += delta;
  }
  void _touch(KvsBuffer *b) override {
      FTRACE
    auto p = lru.iterator_to(*b);
    lru.erase(p);
    lru.push_front(*b);
    num = lru.size();
    _audit("_touch_buffer end");
  }

  void _trim_to(uint64_t max) override;

  void add_stats(uint64_t *buffers, uint64_t *bytes) override {
      FTRACE
    *buffers += num;
    *bytes += buffer_bytes;
  }
};

//------------------------------------------------------------
// OnodeSpace
//------------------------------------------------------------

struct KvsOnodeSpace {
	KvsOnodeCacheShard *cache;

public:
	/// forward lookups
	mempool::kvsstore_cache_other::unordered_map<ghobject_t, OnodeRef> onode_map;

	friend class Collection; // for split_cache()

public:

	KvsOnodeSpace(KvsOnodeCacheShard *c) : cache(c) {}

	~KvsOnodeSpace() {
	    TR << "clearing cache " << (void*)cache << "\n";
		clear();
	}

	OnodeRef add(const ghobject_t &oid, OnodeRef o);
	OnodeRef lookup(const ghobject_t &o);
	void remove(const ghobject_t &oid) {
		onode_map.erase(oid);
	}
	/*
	void rename(OnodeRef &o, const ghobject_t &old_oid,
			const ghobject_t &new_oid,
			const mempool::kvsstore_cache_other::string &new_okey);
	*/
	void clear();
	bool empty();

	void dump(CephContext *cct);

	/// return true if f true for any item
	bool map_any(std::function<bool(OnodeRef)> f);
};


struct KvsBufferSpace {
	KvsBufferCacheShard *cache;

private:
	/// forward lookups
	mempool::kvsstore_cache_other::unordered_map<ghobject_t, KvsBuffer*> data_map;
	friend class Collection; // for split_cache()

public:

	KvsBufferSpace(KvsBufferCacheShard *c) : cache(c) {}

	~KvsBufferSpace() {
		clear();
	}

    void add_data(const ghobject_t &oid, KvsBuffer* b) {
        auto p = data_map.find(oid);
        if (p != data_map.end()) {
            return;
        }

    	b->data.reassign_to_mempool(mempool::mempool_kvsstore_cache_data);
    	cache->_add(b, 1, 0);	// update LRU
    }

    void rm_data(const ghobject_t &oid) {
    	auto p = data_map.find(oid);
		if (p != data_map.end()) {
			cache->_rm(p->second);
			data_map.erase(p);
			return;
		}
    }

    KvsBuffer* lookup_data(const ghobject_t &oid);


	void clear() {
	  for (const auto &p :data_map) {
		  rm_data(p.first);
	  }
	}

	bool empty() {
		return data_map.size() == 0;
	}

};


#endif /* SRC_OS_KVSSTORE_KVSSTORE_CACHE_IMPL_H_ */
