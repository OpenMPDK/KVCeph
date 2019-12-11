/*
 * kvsstore_types.cc
 *
 *  Created on: Nov 17, 2019
 *      Author: root
 */
#include "indexer_hint.h"
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <bitset>
#include "kvsstore_debug.h"

#include "KvsStore.h"
#include "kvsstore_types.h"

// set up dout_context and dout_prefix here
// -----------------------------------------
#define dout_context cct
#define dout_subsys ceph_subsys_kvs
#undef dout_prefix
#define dout_prefix *_dout << "[kvsstore] "

FtraceFile kvs_ff;
std::atomic<uint64_t> KvsJournal::journal_index = {0};


/// --------------------------------------------------------------------
/// Onode
/// --------------------------------------------------------------------

KvsOnode* KvsOnode::decode(
  CollectionRef c,
  const ghobject_t& oid,
  const bufferlist& v)
{
	KvsOnode* on = new KvsOnode(c.get(), oid);
	on->exists = true;
	auto p = v.front().begin_deep();
	on->onode.decode(p);
	for (auto& i : on->onode.attrs) {
		i.second.reassign_to_mempool(mempool::mempool_bluestore_cache_other);
	}

	return on;
}


void KvsOnode::flush()
{
    if (flushing_count.load()) {
    	//TODO: waiting count - txc apply kv
    	waiting_count++;
        std::unique_lock l(flush_lock);
        while (flushing_count.load()) {
            flush_cond.wait(l);
        }
        waiting_count--;
    }
}

//
// #CACHE
//

void KvsLruOnodeCacheShard::_trim_to(uint64_t max)
{
	if (max >= lru.size()) {
		return; // don't even try
	}
	uint64_t n = lru.size() - max;

	auto p = lru.end();
	ceph_assert(p != lru.begin());
	--p;
	int skipped = 0;
	int max_skipped = ONODE_LRUCACHE_TRIM_MAX_SKIP_PINNED;
	while (n > 0) {
		KvsOnode *o = &*p;
		int refs = o->nref.load();
		if (refs > 1) {
			if (++skipped >= max_skipped) {
				break;
			}

			if (p == lru.begin()) {
				break;
			} else {
				p--;
				n--;
				continue;
			}
		}
		if (p != lru.begin()) {
			lru.erase(p--);
		} else {
			lru.erase(p);
			ceph_assert(n == 1);
		}
		o->get();  // paranoia
		o->c->onode_map.remove(o->oid);
		o->put();
		--n;
	}
	num = lru.size();
}

void KvsLruBufferCacheShard::_trim_to(uint64_t max)
{
    FTRACE
  while (buffer_bytes > max) {
    auto i = lru.rbegin();
    if (i == lru.rend()) {
      // stop if lru is now empty
      break;
    }

    KvsBuffer *b = &*i;
    b->space->rm_data(b->oid);
  }
  num = lru.size();
}

///--------------------------------------------------------
/// KvsBufferSpace
///--------------------------------------------------------


KvsBuffer* KvsBufferSpace::lookup_data(const ghobject_t &oid)
{
	KvsBuffer* o;
	{
		std::lock_guard l(cache->lock);
		const auto &p = data_map.find(oid);
		if (p != data_map.end()) {
			cache->_touch(p->second);
			o = p->second;
		}
	}

	return o;
}

//--------------------------------
//# COLLECTION
//--------------------------------

KvsCollection::KvsCollection(KvsStore *store_, KvsOnodeCacheShard *oc, KvsBufferCacheShard *bc, coll_t cid)
  : ObjectStore::CollectionImpl(store_->cct, cid),
    store(store_),
	cache(bc),
    exists(true),
    onode_map(oc),
	data_map(bc),
    commit_queue(nullptr)
{
}

bool KvsCollection::flush_commit(Context *c) {
	return osr->flush_commit(c);
}

void KvsCollection::flush() {
	osr->flush();
}

void KvsCollection::flush_all_but_last()
{
	osr->flush_all_but_last();
}



OnodeRef KvsCollection::get_onode(const ghobject_t &oid, bool create,
		bool is_createop) {

	ceph_assert(create ? ceph_mutex_is_wlocked(lock) : ceph_mutex_is_locked(lock));
    TR << __func__ << " oid = " << oid << TREND;
	spg_t pgid;
	if (cid.is_pg(&pgid)) {
		if (!oid.match(cnode.bits, pgid.ps())) {
			lderr(store->cct) << __func__ << " oid " << oid << " not part of " << pgid
							  << " bits " << cnode.bits << dendl;
			ceph_abort();
		}
	}

	OnodeRef o = onode_map.lookup(oid);
	if (o)
		return o;

	bufferlist v;
	int ret = KV_ERR_KEY_NOT_EXIST;
	KvsOnode *on;

	if (!is_createop) {
		ret = store->db.read_onode(oid, v);
	}

	if (ret == KV_ERR_KEY_NOT_EXIST) {
		if (!create)
			return OnodeRef();
		// new object, new onode
		on = new KvsOnode(this, oid);
		uint64_t lid = ++store->lid_last;
		on->onode.lid = lid;
		on->onode.size = 0;
	} else if (ret == KV_SUCCESS) {
		on = KvsOnode::decode(this, oid, v);
	} else {
		lderr(store->cct) << __func__ << "I/O Error: ret = " << ret << dendl;
		ceph_abort_msg("Failed to read an onode due to an I/O error");
	}
    TR << __func__ << "oid is loaded " << on->oid << TREND;
	o.reset(on);
	return onode_map.add(oid, o);
}

/// --------------------------------------------------------------------
/// Cache
/// --------------------------------------------------------------------

KvsOnodeCacheShard *KvsOnodeCacheShard::create(
    CephContext* cct,
    string type,
    PerfCounters *logger)
{
	KvsOnodeCacheShard *c = nullptr;
   // Currently we only implement an LRU cache for onodes
	c = new KvsLruOnodeCacheShard(cct);
	c->logger = logger;
	return c;
}

KvsBufferCacheShard *KvsBufferCacheShard::create(CephContext* cct, string type, PerfCounters *logger) {
	KvsBufferCacheShard *c = nullptr;
   // Currently we only implement an LRU cache for onodes
	c = new KvsLruBufferCacheShard(cct);
	c->logger = logger;
	return c;

}


// OnodeSpace

OnodeRef KvsOnodeSpace::add(const ghobject_t& oid, OnodeRef o)
{
  std::lock_guard l(cache->lock);
  auto p = onode_map.find(oid);
  if (p != onode_map.end()) {
    return p->second;
  }
  onode_map[oid] = o;
  cache->_add(o, 1);
  cache->_trim();
  return o;
}


OnodeRef KvsOnodeSpace::lookup(const ghobject_t& oid)
{
  ldout(cache->cct, 30) << __func__ << dendl;
  OnodeRef o;

  {
    std::lock_guard l(cache->lock);
    ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
    if (p == onode_map.end()) {
      ldout(cache->cct, 30) << __func__ << " " << oid << " miss" << dendl;
    } else {
      ldout(cache->cct, 30) << __func__ << " " << oid << " hit " << p->second
			    << dendl;
      cache->_touch(p->second);
      o = p->second;
    }
  }
  return o;
}

void KvsOnodeSpace::clear()
{
  std::lock_guard l(cache->lock);
  ldout(cache->cct, 10) << __func__ << dendl;
  for (auto &p : onode_map) {
    cache->_rm(p.second);
  }
  onode_map.clear();
}

bool KvsOnodeSpace::empty()
{
  std::lock_guard l(cache->lock);
  return onode_map.empty();
}

bool KvsOnodeSpace::map_any(std::function<bool(OnodeRef)> f)
{
  std::lock_guard l(cache->lock);
  ldout(cache->cct, 20) << __func__ << dendl;
  for (auto& i : onode_map) {
    if (f(i.second)) {
      return true;
    }
  }
  return false;
}

void KvsOnodeSpace::dump(CephContext *cct)
{
    for (auto& i : onode_map) {
       // ldout(cct, lvl) << i.first << " : " << i.second << dendl;
       ldout(cct, 20) << i.first << " : " << i.second << dendl;
    }
}

///--------------------------------------------------------
/// KvsTransContext
///--------------------------------------------------------

void KvsTransContext::aio_finish(kv_io_context *op) {
	store->txc_aio_finish(op, this);
}

void KvsTransContext::journal_finish(kv_io_context *op) {

}

///--------------------------------------------------------
/// KvsOpSequencer
///--------------------------------------------------------

KvsOpSequencer::KvsOpSequencer(KvsStore *store, uint32_t sequencer_id, const coll_t &c)
	: RefCountedObject(store->cct),
	store(store), cid(c), sequencer_id(sequencer_id) {
}





