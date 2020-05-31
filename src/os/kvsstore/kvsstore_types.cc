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

// from kvsstore_debug.h
FtraceFile FLOG;
FtraceTerm TLOG;


MEMPOOL_DEFINE_OBJECT_FACTORY(KvsStoreTypes::Onode, kvsstore_onode, kvsstore_cache_onode);
//MEMPOOL_DEFINE_OBJECT_FACTORY(KvsStoreTypes::TransContext, kvsstore_transcontext, kvsstore_txc);

std::atomic<uint64_t> KvsJournal::journal_index = {0};


/// --------------------------------------------------------------------
/// KvsStoreTypes::Onode
/// --------------------------------------------------------------------

KvsStoreTypes::Onode* KvsStoreTypes::Onode::decode(
  CollectionRef c, const ghobject_t& oid, const bufferlist& v)
{
    FTRACE
	KvsStoreTypes::Onode* on = new KvsStoreTypes::Onode(c.get(), oid);
	on->exists = true;
	auto p = v.front().begin_deep();
	on->onode.decode(p);
	for (auto& i : on->onode.attrs) {
		i.second.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
	}
	return on;
}


void KvsStoreTypes::Onode::flush()
{
    FTRACE
    /*
    if (flushing_count.load()) {
    	waiting_count++;
        TR << "flush lock -1 flushing count = " << flushing_count.load() << "waiting count = " <<  waiting_count ;
        std::unique_lock l(flush_lock);
        while (flushing_count.load()) {
            flush_cond.wait(l);
        }
        waiting_count--;
    }*/
}

/// --------------------------------------------------------------------
/// KvsStoreTypes::Collection
/// --------------------------------------------------------------------


KvsStoreTypes::Collection::Collection(KvsStore *store_, OnodeCacheShard *oc, BufferCacheShard *bc, coll_t cid)
  : ObjectStore::CollectionImpl(store_->cct, cid),
    store(store_),
	cache(bc),
    exists(true),
    onode_map(oc),
    commit_queue(nullptr)
{
    FTRACE
}

bool KvsStoreTypes::Collection::flush_commit(Context *c) {
    FTRACE
	return osr->flush_commit(c);
}

void KvsStoreTypes::Collection::flush() {
    FTRACE
	osr->flush();
}

void KvsStoreTypes::Collection::flush_all_but_last()
{
    FTRACE
	osr->flush_all_but_last();
}

KvsStoreTypes::OnodeRef KvsStoreTypes::Collection::get_onode(const ghobject_t &oid, bool create,
		bool is_createop) {
    //ceph_assert(create ? ceph_mutex_is_wlocked(lock) : ceph_mutex_is_locked(lock));
    FTRACE
	spg_t pgid;
	if (cid.is_pg(&pgid)) {
		if (!oid.match(cnode.bits, pgid.ps())) {
			lderr(store->cct) << __func__ << " oid " << oid << " not part of " << pgid
							  << " bits " << cnode.bits << dendl;
			ceph_abort();
		}
	}

	KvsStoreTypes::OnodeRef o = onode_map.lookup(oid);
	if (o) {
        return o;
    }

	bufferlist v;
    int r = KV_ERR_KEY_NOT_EXIST;
	KvsStoreTypes::Onode *on;

	if (!is_createop) {
	    r = store->db.read_onode(oid, v);
	}

	if (r == KV_ERR_KEY_NOT_EXIST) {
		if (!create)
			return KvsStoreTypes::OnodeRef();
		// new object, new onode
		on = new KvsStoreTypes::Onode(this, oid);
	} else if (r == KV_SUCCESS) {
		on = KvsStoreTypes::Onode::decode(this, oid, v);
    } else {
		TR << "KV I/O error: ret = " << r ;
		ceph_abort_msg("Failed to read an onode due to an I/O error");
	}

	o.reset(on);
	return onode_map.add(oid, o);
}


void KvsStoreTypes::Collection::split_cache(Collection *dest) {
    FTRACE
    ldout(store->cct, 10) << __func__ << " to " << dest << dendl;

    {
        // lock (one or both) cache shards
        std::lock(cache->lock, dest->cache->lock);
        std::lock_guard l(cache->lock, std::adopt_lock);
        std::lock_guard l2(dest->cache->lock, std::adopt_lock);

        int destbits = dest->cnode.bits;
        spg_t destpg;
        bool is_pg = dest->cid.is_pg(&destpg);
        assert(is_pg);

        auto p = onode_map.onode_map.begin();
        while (p != onode_map.onode_map.end()) {
            OnodeRef o = p->second;

            if (!p->second->oid.match(destbits, destpg.pgid.ps())) {
                // onode does not belong to this child
                ldout(store->cct, 20) << __func__ << " not moving " << o << " " << o->oid
                                      << dendl;
                ++p;
            } else {

                TR<< " moving " << o << " " << o->oid
                        ;
                ldout(store->cct, 20)
                        << __func__ << " moving " << o << " " << o->oid
                        << dendl
                        ;
                onode_map.cache->_rm(p->second);
                p = onode_map.onode_map.erase(p);

                o->c = dest;
                dest->onode_map.cache->_add(o, 1);
                dest->onode_map.onode_map[o->oid] = o;
                dest->onode_map.cache = dest->onode_map.cache;

                //TODO: verify the following code (cache - current cache, dest->cache - new cache)
                if (dest->cache != cache) {
                    for (auto& i : o->bc.buffer_map) {
                        if (!i.second->is_writing()) {
                            dest->cache->_move(cache, i.second.get());
                        }
                    }
                }
            }
        }
    }
    // clear data caches
    dest->cache->_trim();
}



///--------------------------------------------------------
/// TransContext
///--------------------------------------------------------

void KvsStoreTypes::TransContext::aio_finish(KvsStore *store) {
    //FTRACE
	store->txc_aio_finish(this);
}


///--------------------------------------------------------
/// OpSequencer
///--------------------------------------------------------

KvsStoreTypes::OpSequencer::OpSequencer(KvsStore *store, uint32_t sequencer_id, const coll_t &c)
	: RefCountedObject(store->cct),
	store(store), cid(c), sequencer_id(sequencer_id) {
}


