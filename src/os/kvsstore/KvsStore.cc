#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unordered_set>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <bitset>
#include <memory.h>
#include <functional>
#include <algorithm>

#include "osd/osd_types.h"
#include "os/kv.h"
#include "include/compat.h"
#include "include/mempool.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/safe_io.h"
#include "common/Formatter.h"
#include "common/EventTrace.h"

#include "KvsStore.h"
#include "kvsstore_types.h"
#include "kvsstore_debug.h"
#include "kvsstore_db.h"
#include "kvio/kvio.h"
#include "kvio/kadi/kadi_types.h"

MEMPOOL_DEFINE_OBJECT_FACTORY(KvsOnode, kvsstore_onode, kvsstore_cache_onode);
MEMPOOL_DEFINE_OBJECT_FACTORY(KvsTransContext, kvsstore_transcontext, kvsstore_txc);

// set up dout_context and dout_prefix here
// -----------------------------------------
#define dout_context cct
#define dout_subsys ceph_subsys_kvs

#undef dout_prefix
#define dout_prefix *_dout << "[kvsstore] "

///--------------------------------------------------------
/// Journal
///--------------------------------------------------------

int KvsStore::_journal_replay() {
	FTRACE
	if (kvsb.is_uptodate == 1) return 0;
	kv_value value;
	value.offset = 0;
	value.length = 2*1024*1024;
	value.value  = malloc(value.length);
	kv_key entry_key;
	kv_value entry_value;

	KADIWrapper *kadi = db.get_adi();

	// JOURNAL REPLAY
	int i=0, ret;

	while (true) {
        value.offset = 0;
        value.length = 2*1024*1024;
		if (!db.read_journal(i++, &value)) break;
		derr << "read journal  " << dendl;
		KvsJournal journal ((char*)value.value);
		journal.read_journal_entry([&] (
				kvs_journal_entry *entry, char *keypos, char *datapos) {

			entry_key.key = keypos;
			entry_key.length = entry->key_length;

			if (entry->key_length == 0) return;

			if (entry->op_type == 1)
			{
				// delete
				ret = kadi->sync_delete(entry->spaceid, &entry_key);
			}
			else {
				// write
				entry_value.value  = datapos;
				entry_value.length = entry->length;
				entry_value.offset = 0;
				ret = kadi->sync_write(entry->spaceid, &entry_key, &entry_value);
			}

			if (ret != 0 && ret != 784) {
				derr << "journal recovery failed: ret = " << ret << dendl;
				ceph_abort("jorunal replay failed");
			}
		});

	}

	free(value.value);
	_delete_journal();

	return 0;
}

void KvsStore::_delete_journal() {
	FTRACE
	const int num_entries = KvsJournal::journal_index;
	for (int i =0 ;i < num_entries; i++) {
		if (!db.rm_journal(i)) break;
	}
}

///--------------------------------------------------------
/// Index Threads
///--------------------------------------------------------

void KvsStore::_kv_index_thread() {
	FTRACE
	derr << "index thread..." << dendl;
	// load pages from the AOL
	static double index_interval_us = 1000000.0; // 1 second
	while(true) {
	    //db.compact();

        {
            std::unique_lock l (kv_lock );
            if (kv_index_stop) break;
        }

		usleep(index_interval_us);
	}

	derr << "index thread...done" << dendl;
}

///--------------------------------------------------------
/// Iterate Functions
///--------------------------------------------------------

static void get_coll_key_range(const coll_t& cid, int bits, kv_key *temp_start, kv_key *temp_end, kv_key *start, kv_key *end ) {

    //TR << "coll key range " << cid << ", bits = " << bits;
	struct kvs_onode_key* temp_s_key = (struct kvs_onode_key*)temp_start->key;
	struct kvs_onode_key* temp_e_key = (struct kvs_onode_key*)temp_end->key;
	struct kvs_onode_key* s_key = (struct kvs_onode_key*)start->key;
	struct kvs_onode_key* e_key = (struct kvs_onode_key*)end->key;

	s_key->prefix = GROUP_PREFIX_ONODE;
	e_key->prefix = GROUP_PREFIX_ONODE;
	temp_s_key->prefix = GROUP_PREFIX_ONODE;
	temp_e_key->prefix = GROUP_PREFIX_ONODE;

	spg_t pgid;
	if (cid.is_pg(&pgid)) {
		// shard id

		s_key->shardid 	    = int8_t(pgid.shard) + 0x80;
		e_key->shardid 		= s_key->shardid;
		temp_s_key->shardid = s_key->shardid;
		temp_e_key->shardid = s_key->shardid;

		// poolid
		s_key->poolid = pgid.pool() + 0x8000000000000000ull;
		e_key->poolid = s_key->poolid;
		temp_s_key->poolid = (-2ll - pgid.pool()) + 0x8000000000000000ull;
        temp_e_key->poolid = temp_s_key->poolid;

        // hash, start key
		uint32_t reverse_hash = hobject_t::_reverse_bits(pgid.ps());
		s_key->bitwisekey = reverse_hash;
		temp_s_key->bitwisekey = reverse_hash;

		// hash, end key
		uint64_t end_hash = reverse_hash + (1ull << (32 - bits));
		if (end_hash > 0xffffffffull)
			end_hash = 0xffffffffull;

        //TR << "PG end hash = " << end_hash;
		e_key->bitwisekey = end_hash;
		temp_e_key->bitwisekey = end_hash;

	} else {
		// shardid
		s_key->shardid 	    = int8_t(shard_id_t::NO_SHARD) + 0x80;
		e_key->shardid 		= s_key->shardid;

		// poolid
		s_key->poolid = -1ull + 0x8000000000000000ull;
		e_key->poolid = s_key->poolid;

		// hash
		s_key->bitwisekey = 0;
		e_key->bitwisekey = 0xffffffff;

		// no separate temp section
		temp_s_key->shardid = e_key->shardid;
		temp_e_key->shardid = e_key->shardid;

		temp_s_key->poolid  = e_key->poolid;
		temp_e_key->poolid  = e_key->poolid;

		temp_s_key->bitwisekey = e_key->bitwisekey;
		temp_e_key->bitwisekey = e_key->bitwisekey;
	}
}


int KvsStore::_collection_list(KvsCollection *c, const ghobject_t &start,
		const ghobject_t &end, int max, vector<ghobject_t> *ls,
		ghobject_t *pnext, bool destructive) {
	FTRACE
	int r = 0;
	bool set_next = false;
	kv_key pend;
	bool temp;
	// key buffers;
	char buf1[17], buf2[17], buf3[17], buf4[17], tempbuf[256];
	kv_key temp_start_key = {buf1, 17}, temp_end_key = {buf2, 17};
	kv_key start_key= {buf3, 17}, end_key= {buf4, 17};
	KvsIterator *it = 0;
	ghobject_t static_next;
	if (!pnext)
		pnext = &static_next;

	if (start == ghobject_t::get_max() || start.hobj.is_max()) {
		goto out;
	}

	get_coll_key_range(c->cid, c->cnode.bits, &temp_start_key, &temp_end_key, &start_key, &end_key);



    dout(20) << __func__ << " range " << print_kvssd_key(temp_start_key.key, temp_start_key.length)
						<< " to " << print_kvssd_key(temp_end_key.key, temp_end_key.length) << " and "
						<< print_kvssd_key(start_key.key, start_key.length) << " to "
						<< print_kvssd_key(end_key.key, end_key.length) << " start " << start << dendl;

	{
        db.compact();

        TR << " range " << print_kvssd_key(temp_start_key.key, temp_start_key.length)
           << " to " << print_kvssd_key(temp_end_key.key, temp_end_key.length) << " and "
           << print_kvssd_key(start_key.key, start_key.length) << " to "
           << print_kvssd_key(end_key.key, end_key.length) << " start " << start;
		/*
        {
            auto it = db.get_iterator(GROUP_PREFIX_ONODE);
            int index = 0;
            bool a = false,b = false, c = false, d = false;
            while (it->valid()) {
                kv_key key = it->key();

                if (!a && db.is_key_ge(it->key(), temp_start_key)) {
                    //TR << "range temp start key " << print_kvssd_key(temp_start_key.key, temp_start_key.length);
                    a = true;
                }

                if (!b && db.is_key_ge(it->key(), start_key)) {
                    //TR << "range start key " << print_kvssd_key(start_key.key, start_key.length);
                    b = true;
                }

                if (!c && db.is_key_ge(it->key(), temp_end_key)) {
                    //TR << "range temp end key " << print_kvssd_key(temp_end_key.key, temp_end_key.length);
                    c = true;
                }

                if (!d && db.is_key_ge(it->key(), end_key)) {
                    //TR << "range temp end key " << print_kvssd_key(end_key.key, end_key.length);
                    d = true;
                }

                ghobject_t oid;
                construct_onode_ghobject_t(cct, key, &oid);
                TR << "iter  keys #"<< index++ << "= " << print_kvssd_key(key.key, key.length) << ", oid = " << oid;

                it->next();
            }
        }*/
		it = db.get_iterator(GROUP_PREFIX_ONODE);
		if (start == ghobject_t() || start == c->cid.get_min_hobj()) {
			it->upper_bound(temp_start_key);
			temp = true;
		} else {
			kv_key k = {tempbuf, 255} ;
			k.length = construct_onode_key(cct,start, tempbuf);
			if (start.hobj.is_temp()) {
				temp = true;
			} else {
				temp = false;
			}

			it->lower_bound(k);
		}

		if (end.hobj.is_max()) {
			pend = temp ? temp_end_key : end_key;
		} else {
			end_key.length = construct_onode_key(cct, end, end_key.key);

			if (end.hobj.is_temp()) {
				if (temp)
					pend = end_key;
				else
					goto out;
			} else {
				pend = temp ? temp_end_key : end_key;
			}
		}

		while (true) {
			if (!it->valid() || db.is_key_ge(it->key(), pend)) {
				if (!it->valid())
					dout(20) << __func__ << " iterator not valid (end of db?)" << dendl;
				else {
					kv_key key = it->key();
					dout(20) << __func__ << " key " << print_kvssd_key(key.key, key.length) << " > " << end << dendl;
				}

				if (temp) {
					if (end.hobj.is_temp()) {
						break;
					}

					dout(30) << __func__ << " switch to non-temp namespace" << dendl;

					temp = false;
					it->upper_bound(start_key);
					pend = end_key;
					dout(30) << __func__ << " pend " << print_kvssd_key(pend.key, pend.length) << dendl;
					continue;
				}
				break;
			}
            TR << __func__ << " key " << it->key().key << ", length " << (int)it->key().length;
			TR << __func__ << " key " << print_kvssd_key(it->key().key, it->key().length);
			kv_key key = it->key();
			if (key.length > 0) {
                ghobject_t oid;
                construct_onode_ghobject_t(cct, key, &oid);
                TR << __func__ << " oid = " << oid << max;
                //TR << "object hash = " << oid.get_nibblewise_key_u32() << ", end hash " << 67108864 << " ok? " << (oid.get_nibblewise_key_u32() < 67108864);
                ceph_assert(r == 0);
                if (ls->size() >= (unsigned) max) {
                    //TR << __func__ << " reached max " << max;
                    *pnext = oid;
                    set_next = true;
                    break;
                }
                ls->push_back(oid);
            }
            TR << __func__ << "ls size = " << ls->size() << "\n";
			it->next();
		}
	}
out:
	if (!set_next) {
		*pnext = ghobject_t::get_max();
	}
	if (it) delete it;
	return r;
}

static int open_count =0;
int KvsStore::_open_collections() {

	FTRACE

	db.compact();

    KvsIterator *it = db.get_iterator(GROUP_PREFIX_COLL);
    for (it->begin(); it->valid(); it->next()) {
        coll_t cid;
        kv_key collkey = it->key();
        TR << "returned key : " << print_kvssd_key(std::string((char *) collkey.key, collkey.length)) ;
        std::string name((char *) collkey.key + sizeof(kvs_coll_key), collkey.length - sizeof(kvs_coll_key));
        TR << "found collection: " << name ;
        if (cid.parse(name)) {
            //TR << "CID parse" ;
            auto c = ceph::make_ref<KvsCollection>(this,
                                                   onode_cache_shards[cid.hash_to_shard(onode_cache_shards.size())],
                                                   buffer_cache_shards[cid.hash_to_shard(
                                                           buffer_cache_shards.size())],
                                                   cid);

            bufferlist bl;
            db.read_kvkey(&collkey, bl);

            auto p = bl.cbegin();
            try {
                decode(c->cnode, p);
            } catch (buffer::error &e) {
                derr << __func__ << " failed to decode cnode, key:"
                     << print_kvssd_key((char *) collkey.key, collkey.length) << dendl;
                //workaround for fw issue
                if (1) continue;

                if (it) delete it;
                return -EIO;
            }
            dout(20) << __func__ << " opened " << cid << " " << c << dendl;
            _osr_attach(c.get());
            coll_map[cid] = c;

        } else {

            derr << __func__ << " unrecognized collection " << print_kvssd_key(it->key().key, it->key().length)
                 << dendl;
            //ceph_abort_msg("unrecognized collection");
            // workaround for fw issue
        }
    }
    if (it) delete it;

	if ( open_count > 0 && coll_map.size() == 0) {
        ceph_abort_msg("no collections found");
        open_count++;
	}

	return 0;
}


/// -------------------------------------------------------------------------
/// Transaction
/// -------------------------------------------------------------------------

int KvsStore::queue_transactions(CollectionHandle &ch, vector<Transaction> &tls, TrackedOpRef op, ThreadPool::TPHandle *handle) {
	FTRACE
	//list<Context*> on_applied, on_commit, on_applied_sync;
    Context *onreadable;
    Context *ondisk;
    Context *onreadable_sync;

    ObjectStore::Transaction::collect_contexts(tls, &onreadable, &ondisk, &onreadable_sync);

    KvsCollection *c = static_cast<KvsCollection*>(ch.get());
    if (c == 0) ceph_abort_msg("cannot find the collection");
    KvsOpSequencer *osr = c->osr.get();

    KvsTransContext *txc = _txc_create(static_cast<KvsCollection*>(ch.get()), osr);
    txc->onreadable = onreadable;
    txc->onreadable_sync = onreadable_sync;
    txc->oncommit = ondisk;

    for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
		txc->bytes += (*p).get_num_bytes();
		_txc_add_transaction(txc, &(*p));
	}

	// journal write
	_txc_write_nodes(txc);

	_txc_state_proc(txc);

	// we're immediately readable (unlike FileStore)
	/*for (auto c : on_applied_sync) {
		c->complete(0);
	}
	if (!on_applied.empty()) {
		if (c->commit_queue) {
			c->commit_queue->queue(on_applied);
		} else {
			//  finisher.queue(on_applied);
			finisher.queue(on_applied);
		}
	}*/

	return 0;
}


void KvsStore::_txc_write_nodes(KvsTransContext *txc) {
	FTRACE
	dout(20) << __func__ << " txc " << txc << " onodes " << txc->onodes << dendl;

	for (auto o : txc->onodes) {
		if (!o->exists)
			continue;

		size_t bound = 0;
		denc(o->onode, bound);

        //TR << "ONODE WRITE oid " << o->oid << "bound = " << bound ;

		bufferlist bl;
        {
            auto p = bl.get_contiguous_appender(bound, true);
            denc(o->onode, p);
        }

		db.add_onode(&txc->ioc, o->oid, bl);

		o->flushing_count++;
        {
            std::lock_guard<std::mutex> l(o->flush_lock);
            o->flush_txns.insert(txc);
        }
	}

	// objects we modified but didn't affect the onode
	auto p = txc->modified_objects.begin();
	while (p != txc->modified_objects.end()) {
		if (txc->onodes.count(*p) == 0) {
			(*p)->flushing_count++;
            {
                std::lock_guard<std::mutex> l((*p)->flush_lock);
                (*p)->flush_txns.insert(txc);
            }
			++p;
		} else {
			// remove dups with onodes list to avoid problems in _txc_finish
			p = txc->modified_objects.erase(p);
		}
	}

	// write journal
	int ret = db.write_journal(txc);
	if (ret != 0) {
		derr << "journal write failed" << dendl;
		ceph_abort();
	}


}


void KvsStore::_txc_state_proc(KvsTransContext *txc) {

	//TRBACKTRACE;

	while (true) {

		switch (txc->state) {

		case KvsTransContext::STATE_PREPARE:
            TR << "TXC" << (void*) txc << ", STATE: PREPARE";
            txc->state = KvsTransContext::STATE_AIO_WAIT;
            //TR << "txc " << (void*)txc << ", state = " << txc->state ;

			if (txc->ioc.has_pending_aios()) {
				_txc_aio_submit(txc);
				return;
			} else {
                TR << "TXC" << (void*) txc << "is EMPTY";
			}

			break; // fall through

		case KvsTransContext::STATE_AIO_WAIT:
			// io finished
            TR << "TXC" << (void*) txc << ", STATE: IOFINISHED -> FINALIZE";
            {
                std::unique_lock l(kv_finalize_lock);
                kv_committing_to_finalize.push_back(txc);
                kv_finalize_cond.notify_one();
            }
			//_txc_finish_io(txc);

			return;
#if 0
		case KvsTransContext::STATE_IO_DONE:

            //TR << "txc " << (void*)txc << ", state = " << txc->state ;
			_txc_committed_kv(txc);
			/*{
				std::unique_lock l(kv_finalize_lock);
                //TR << "comming - txc " << (void*)txc << ", state = " << txc->state ;
				kv_committing_to_finalize.push_back(txc);
				kv_finalize_cond.notify_one();
			}*/
			return;

		case KvsTransContext::STATE_FINISHING:
            //TR << "txc " << (void*)txc << ", final state" ;
			_txc_finish(txc);
			return;
#endif
		default:
			derr << __func__ << " unexpected txc " << txc << " state "
								<< txc->get_state_name() << dendl;
			assert(0 == "unexpected txc state");
			return;
		}
	}
}

// callback for txc_aio_submit
void KvsStore::txc_aio_finish(kv_io_context *op, KvsTransContext *txc) {
	if (--txc->ioc.num_running == 0) {
		// last I/O -> proceed the transaction status
		txc->store->_txc_state_proc(txc);
	}
}

// IO finished
void KvsStore::_txc_finish_io(KvsTransContext *txc) {
	FTRACE
	dout(20) << __func__ << " " << txc << dendl;

	/*
	 * we need to preserve the order of kv transactions,
	 * even though aio will complete in any order.
	 */
	KvsOpSequencer *osr = txc->osr.get();
    // osr-> release buffers
	std::lock_guard l(osr->qlock);
	txc->state = KvsTransContext::STATE_IO_DONE;
    // TR << "txc " << (void*)txc << ", state = " << txc->state ;
	KvsOpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
	while (p != osr->q.begin()) {
		--p;
		if (p->state < KvsTransContext::STATE_IO_DONE) {
			dout(20) << __func__ << " " << txc << " blocked by " << &*p
								<< " " << p->get_state_name() << dendl;
			return;
		}
		if (p->state > KvsTransContext::STATE_IO_DONE) {
			++p;
			break;
		}
	}
	do {
		_txc_state_proc(&*p++);

	} while (p != osr->q.end() && p->state == KvsTransContext::STATE_IO_DONE);

	if (osr->kv_submitted_waiters /*&& oslockdep_will_lockr->_is_all_kv_submitted()*/) {
		osr->qcond.notify_all();
	}
}

void KvsStore::_txc_committed_kv(KvsTransContext *txc) {
	FTRACE
    txc->state = KvsTransContext::STATE_FINISHING;
	bool locked = txc->osr->qlock.try_lock();

	dout(20) << __func__ << " txc " << txc << dendl;
	{
		//	std::lock_guard<ceph::mutex> l(txc->osr->qlock);
		if (txc->ch->commit_queue) {
		  txc->ch->commit_queue->queue(txc->oncommits);
		} else {
		  finisher.queue(txc->oncommits);
		}
	}

	if (locked) {
		txc->osr->qlock.unlock();
	}
}

void KvsStore::_txc_finish(KvsTransContext *txc) {
	FTRACE
	dout(20) << __func__ << " " << txc << " onodes " << txc->onodes << dendl;
	assert(txc->state == KvsTransContext::STATE_FINISHING);

	while (!txc->removed_collections.empty()) {
		_queue_reap_collection(txc->removed_collections.front());
		txc->removed_collections.pop_front();
	}

	OpSequencerRef osr = txc->osr;
	bool empty = false;

	KvsOpSequencer::q_list_t releasing_txc;
	{
		std::lock_guard l(osr->qlock);

		txc->state = KvsTransContext::STATE_DONE;
		bool notify = false;
		while (!osr->q.empty()) {
			KvsTransContext *txc = &osr->q.front();

			dout(20) << __func__ << "  txc " << txc << " "
								<< txc->get_state_name() << dendl;
			if (txc->state != KvsTransContext::STATE_DONE || txc->submitted == false) {
				break;
			}

			osr->pop_front_nolock();
			releasing_txc.push_back(*txc);
			notify = true;
		}

		if (osr->q.empty()) {
			dout(20) << __func__ << " osr " << osr << " q now empty" << dendl;
			empty = true;
            //TR << "osr is empty now";
		}
		if (notify || empty) {
			osr->qcond.notify_all();
		}
	}

	while (!releasing_txc.empty()) {
		auto txc = &releasing_txc.front();
        releasing_txc.pop_front();

		_txc_release_alloc(txc);
        delete txc;
	}

	if (empty && osr->zombie) {
		std::lock_guard l(zombie_osr_lock);
		if (zombie_osr_set.erase(osr->cid)) {
			dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
		} else {
			dout(10) << __func__ << " empty zombie osr " << osr << " already reaped" << dendl;
		}
	}
}

void KvsStore::_txc_release_alloc(KvsTransContext *txc) {
	FTRACE
	KvsIoContext *ioc = &txc->ioc;



	{
		std::unique_lock<std::mutex> lk ( ioc->running_aio_lock );
		// release memory

        auto it = ioc->running_ios.cbegin();
        while (it != ioc->running_ios.end()) {
            auto ior = (*it);

            if (ior->data != 0) {
                delete ior->data;
            }

            //if (ior->raw_data) {
            //    free(ior->raw_data);
            //}

            if (ior->key) {
                free(ior->key->key);
                delete ior->key;
            }
            delete ior;

            it++;
        }
        ioc->running_ios.clear();
    }

	txc->onodes.clear();
}


KvsTransContext* KvsStore::_txc_create(KvsCollection *c, KvsOpSequencer *osr) {
	FTRACE

    KvsTransContext *txc = new KvsTransContext(cct, c, this, osr);
	osr->queue_new(txc);
	TR << "TXC " << (void*)txc << ": created";
	dout(20) << __func__ << " osr " << osr << " = " << txc << " seq " << txc->seq << dendl;
	return txc;
}

/// ----------------------------------------------------------------------------
/// Add Transaction
/// ----------------------------------------------------------------------------

void KvsStore::_txc_add_transaction(KvsTransContext *txc, Transaction *t) {
	FTRACE
	Transaction::iterator i = t->begin();

	vector<CollectionRef> cvec(i.colls.size());

	unsigned j = 0;
	for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
			++p, ++j) {
		cvec[j] = _get_collection(*p);
	}



	vector<OnodeRef> ovec(i.objects.size());

	for (int pos = 0; i.have_op(); ++pos) {
		Transaction::Op *op = i.decode_op();
		int r = 0;

        //TR << "TR op= " << op << ", cid = " << op->cid ;
		// no coll or obj
		if (op->op == Transaction::OP_NOP) {
            continue;
		}

		// collection operations
		CollectionRef &c = cvec[op->cid];

		switch (op->op) {
		case Transaction::OP_RMCOLL: {
			const coll_t &cid = i.get_cid(op->cid);

				r = _remove_collection(txc, cid, &c);
				if (!r)
					continue;
			}
			break;

		case Transaction::OP_MKCOLL: {
				const coll_t &cid = i.get_cid(op->cid);
				r = _create_collection(txc, cid, op->split_bits, &c);
				if (!r) {
                    continue;
                }
            //TR << "ERR: mkcoll failed" ;
			}
			break;

		case Transaction::OP_SPLIT_COLLECTION:
			assert(0 == "deprecated");
			break;

		case Transaction::OP_SPLIT_COLLECTION2: {
				uint32_t bits = op->split_bits;
				uint32_t rem = op->split_rem;

				r = _split_collection(txc, c, cvec[op->dest_cid], bits, rem);
				if (!r)
					continue;
			}
			break;

		case Transaction::OP_MERGE_COLLECTION: {
				uint32_t bits = op->split_bits;
				r = _merge_collection(txc, &c, cvec[op->dest_cid], bits);
				if (!r)
					continue;
			}
			break;
		case Transaction::OP_COLL_HINT: {
				uint32_t type = op->hint_type;
				bufferlist hint;
				i.decode_bl(hint);
				auto hiter = hint.cbegin();
				if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
					uint32_t pg_num;
					uint64_t num_objs;
					decode(pg_num, hiter);
					decode(num_objs, hiter);
					dout(10) << __func__ << " collection hint objects is a no-op, "
							 << " pg_num " << pg_num << " num_objects " << num_objs << dendl;
				} else {
					// Ignore the hint
					dout(10) << __func__ << " unknown collection hint " << type
										<< dendl;
				}
				continue;
			}
			break;

		case Transaction::OP_COLL_SETATTR:
			r = -EOPNOTSUPP;
			break;

		case Transaction::OP_COLL_RMATTR:
			r = -EOPNOTSUPP;
			break;

		case Transaction::OP_COLL_RENAME:
			assert(0 == "not implemented");
			break;
		}

		if (r < 0) {
			derr << __func__ << " 1. error " << cpp_strerror(r) << " not handled on operation " << op->op
								<< " (op " << pos << ", counting from 0)"
								<< dendl;
			assert(0 == "unexpected error");
		}

		// these operations implicity create the object
		bool create = false;
		if (op->op == Transaction::OP_TOUCH
				|| op->op == Transaction::OP_CREATE
				|| op->op == Transaction::OP_WRITE
				|| op->op == Transaction::OP_ZERO) {
			create = true;
		}

		// object operations
		std::unique_lock l(c->lock);
		OnodeRef &o = ovec[op->oid];

		if (!o) {
			ghobject_t oid = i.get_oid(op->oid);

			o = c->get_onode(oid, create, op->op == Transaction::OP_CREATE);
		}

		if (!create && (!o || !o->exists)) {
			dout(10) << __func__ << " op " << op->op << " got ENOENT on "
								<< i.get_oid(op->oid) << dendl;

			r = -ENOENT;
			goto endop;
		}

		switch (op->op) {
		case Transaction::OP_CREATE:
		case Transaction::OP_TOUCH:
			r = _touch(txc, c, o);
			break;

		case Transaction::OP_WRITE: {
				uint64_t off = op->off;
				uint64_t len = op->len;
				uint32_t fadvise_flags = i.get_fadvise_flags();
				bufferlist bl;
				i.decode_bl(bl);
                //TR << "OP_WRITE o->oid " << o->oid << ", c->cid " << c->cid << ", bl length " << bl.length() << ", len " << len;
				r = _write(txc, c, o, off, len, bl, fadvise_flags);

			}
			break;

		case Transaction::OP_ZERO: {
				uint64_t off = op->off;
				uint64_t len = op->len;
				r = _zero(txc, c, o, off, len);
			}
			break;

		case Transaction::OP_TRIMCACHE: {
			// deprecated, no-op
			}
			break;

		case Transaction::OP_TRUNCATE: {
				uint64_t off = op->off;
				r = _truncate(txc, c, o, off);
			}
			break;

		case Transaction::OP_REMOVE: {
				r = _remove(txc, c, o);
			}
			break;

		case Transaction::OP_SETATTR: {
				string name = i.decode_string();
				bufferptr bp;
				i.decode_bp(bp);
				r = _setattr(txc, c, o, name, bp);
			}
			break;

		case Transaction::OP_SETATTRS: {
				map<string, bufferptr> aset;
				i.decode_attrset(aset);
				r = _setattrs(txc, c, o, aset);
			}
			break;

		case Transaction::OP_RMATTR: {
				string name = i.decode_string();
				r = _rmattr(txc, c, o, name);
			}
			break;

		case Transaction::OP_RMATTRS: {
				r = _rmattrs(txc, c, o);
			}
			break;

		case Transaction::OP_CLONE: {
				OnodeRef &no = ovec[op->dest_oid];
				if (!no) {
					const ghobject_t &noid = i.get_oid(op->dest_oid);
					no = c->get_onode(noid, true);
				}
				r = _clone(txc, c, o, no);
			}
			break;

		case Transaction::OP_CLONERANGE:
			assert(0 == "deprecated");
			break;

		case Transaction::OP_CLONERANGE2: {
				OnodeRef &no = ovec[op->dest_oid];
				if (!no) {
					const ghobject_t &noid = i.get_oid(op->dest_oid);
					no = c->get_onode(noid, true);
				}
				uint64_t srcoff = op->off;
				uint64_t len = op->len;
				uint64_t dstoff = op->dest_off;
				r = _clone_range(txc, c, o, no, srcoff, len, dstoff);
			}
			break;

		case Transaction::OP_COLL_ADD:
			ceph_abort_msg("not implemented");
			break;

		case Transaction::OP_COLL_REMOVE:
			ceph_abort_msg("not implemented");
			break;

		case Transaction::OP_COLL_MOVE:
			ceph_abort_msg("deprecated");
			break;

		case Transaction::OP_COLL_MOVE_RENAME:
		case Transaction::OP_TRY_RENAME: {
				assert(op->cid == op->dest_cid);
				const ghobject_t &noid = i.get_oid(op->dest_oid);
				OnodeRef &no = ovec[op->dest_oid];
				if (!no) {
					no = c->get_onode(noid, true);
				}
				r = _rename(txc, c, o, no, noid);
			}
			break;

		case Transaction::OP_OMAP_CLEAR: {

				r = _omap_clear(txc, c, o);
			}
			break;
		case Transaction::OP_OMAP_SETKEYS: {
				bufferlist aset_bl;
				i.decode_attrset_bl(&aset_bl);
				r = _omap_setkeys(txc, c, o, aset_bl);
			}
			break;
		case Transaction::OP_OMAP_RMKEYS: {
				bufferlist keys_bl;
				i.decode_keyset_bl(&keys_bl);
				r = _omap_rmkeys(txc, c, o, keys_bl);
			}
			break;
		case Transaction::OP_OMAP_RMKEYRANGE: {
				string first, last;
				first = i.decode_string();
				last = i.decode_string();
				r = _omap_rmkey_range(txc, c, o, first, last);
			}
			break;
		case Transaction::OP_OMAP_SETHEADER: {
				bufferlist bl;
				i.decode_bl(bl);
				r = _omap_setheader(txc, c, o, bl);
				//derr << __func__ << " [ "<< op->op  << " . OMAP_SETHEADER = " << r << dendl;
			}
			break;

		case Transaction::OP_SETALLOCHINT: {
				r = 0; // alloc hint not needed
			}
			break;

		default:
			derr << __func__ << "bad op " << op->op << dendl;
			ceph_abort();
		}

	endop:
		if (r < 0) {
			bool ok = false;

			if (r == -ENOENT
					&& !(op->op == Transaction::OP_CLONERANGE
							|| op->op == Transaction::OP_CLONE
							|| op->op == Transaction::OP_CLONERANGE2
							|| op->op == Transaction::OP_COLL_ADD
							|| op->op == Transaction::OP_SETATTR
							|| op->op == Transaction::OP_SETATTRS
							|| op->op == Transaction::OP_RMATTR
							|| op->op == Transaction::OP_OMAP_SETKEYS
							|| op->op == Transaction::OP_OMAP_RMKEYS
							|| op->op == Transaction::OP_OMAP_RMKEYRANGE
							|| op->op == Transaction::OP_OMAP_SETHEADER))
				// -ENOENT is usually okay
				ok = true;
			if (r == -ENODATA)
				ok = true;

			if (r == -E2BIG
					&& (op->op == Transaction::OP_WRITE
							|| op->op == Transaction::OP_TRUNCATE
							|| op->op == Transaction::OP_ZERO))
				ok = true;

			if (!ok) {
				const char *msg = "unexpected error code";

				if (r == -ENOENT
						&& (op->op == Transaction::OP_CLONERANGE
								|| op->op == Transaction::OP_CLONE
								|| op->op == Transaction::OP_CLONERANGE2))
					msg = "ENOENT on clone suggests osd bug";

				if (r == -ENOSPC)
					// For now, if we hit _any_ ENOSPC, crash, before we do any damage
					// by partially applying transactions.
					msg = "ENOSPC from bluestore, misconfigured cluster";

				if (r == -ENOTEMPTY) {
					msg = "ENOTEMPTY suggests garbage data in osd data dir";
				}

				derr << __func__ << " 2. error: code = " << r << "("
									<< cpp_strerror(r) << ") not handled on operation " << op->op
									<< " (op " << pos << ", counting from 0)"
									<< dendl;
				derr << msg << dendl;
				ceph_abort_msg("unexpected error");
			}
		}
	}
}


int KvsStore::_touch(KvsTransContext *txc, CollectionRef &c, OnodeRef &o) {
	FTRACE
	//int r = this->_write(txc, c, o, 0, 0, 0, 0);
	txc->write_onode(o);
	return 0;
}


int KvsStore::_remove(KvsTransContext *txc, CollectionRef &c, OnodeRef &o) {
	FTRACE
	dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
	int r = _do_remove(txc, c, o);
	dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r
						<< dendl;
	return r;
}


int KvsStore::_truncate(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
		uint64_t offset) {
	FTRACE
	dout(15) << __func__ << " " << c->cid << " " << o->oid << " 0x"
						<< std::hex << offset << std::dec << dendl;
	int r = _do_truncate(txc, o, offset);
	dout(10) << __func__ << " " << c->cid << " " << o->oid << " 0x"
						<< std::hex << offset << std::dec << " = " << r << dendl;
	return r;
}

int KvsStore::_zero(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
		uint64_t offset, size_t length) {
	FTRACE
	dout(15) << __func__ << " " << c->cid << " " << o->oid << " 0x"
						<< std::hex << offset << "~" << length << std::dec
						<< dendl;

	int r = _do_zero(txc, c, o, offset, length);
	dout(10) << __func__ << " " << c->cid << " " << o->oid << " 0x"
						<< std::hex << offset << "~" << length << std::dec
						<< " = " << r << dendl;
	return r;
}

int KvsStore::_rename(KvsTransContext *txc, CollectionRef &c, OnodeRef &oldo,
		OnodeRef &newo, const ghobject_t &new_oid) {
	FTRACE
	dout(10) << __func__ << " cid = " << c->cid << ", old->oid =  "
						<< oldo->oid << " -> " << ", new->oid =  " << newo->oid
						<< dendl;

	int r = 0;

	ghobject_t old_oid = oldo->oid;
	bufferlist bl;
	string old_key, new_key;

	if (newo) {
		if (newo->exists) {
			r = -EEXIST;
			if (r < 0) {
				derr << __func__ << " New Object " << new_oid
					 << " exists" << " r = " << r << dendl;
				goto release;
			}
		}
	}

	// copy old object to new object
	r = this->_clone(txc, c, oldo, newo);
	if (r < 0) {
		derr << __func__ << " clone failed" << r << dendl;
		return r;
	}

	txc->note_modified_object(oldo);

	r = _do_remove(txc, c, oldo);
	if (r < 0) {
		derr << __func__ << " _do_remove old object = " << oldo->oid
							<< ", r = " << r << dendl;
		goto release;
	}

	r = 0;

release:
	/*derr << __func__ << " cid = " << c->cid << ", old->oid =  " << oldo->oid << " -> "
	 << ", new_oid "<< new_oid << ", ret = " << r << dendl;*/

	return r;
}


/// ------------------------------------------------------------------------------------------------
/// Attributes
/// ------------------------------------------------------------------------------------------------

int KvsStore::_setattr(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
		const string &name, bufferptr &val) {
	FTRACE
	dout(15) << __func__ << " " << c->cid << " " << o->oid << " " << name
						<< " (" << val.length() << " bytes)" << dendl;
	int r = 0;

	if (val.is_partial()) {
		auto &b = o->onode.attrs[name.c_str()] = bufferptr(val.c_str(),
				val.length());
		b.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
	} else {
		auto &b = o->onode.attrs[name.c_str()] = val;
		b.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
	}
	txc->write_onode(o);
	dout(10) << __func__ << " " << c->cid << " " << o->oid << " " << name
						<< " (" << val.length() << " bytes)" << " = " << r
						<< dendl;
	return r;
}

int KvsStore::_setattrs(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
		const map<string, bufferptr> &aset) {
	FTRACE
	dout(15) << __func__ << " " << c->cid << " " << o->oid << " "
						<< aset.size() << " keys" << dendl;
	int r = 0;

	for (map<string, bufferptr>::const_iterator p = aset.begin();
			p != aset.end(); ++p) {

		if (p->second.is_partial()) {
			auto &b = o->onode.attrs[p->first.c_str()] = bufferptr(
					p->second.c_str(), p->second.length());
			b.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
		} else {
			auto &b = o->onode.attrs[p->first.c_str()] = p->second;
			b.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
		}
	}
	txc->write_onode(o);
	dout(10) << __func__ << " " << c->cid << " " << o->oid << " "
						<< aset.size() << " keys" << " = " << r << dendl;
	return r;
}

int KvsStore::_rmattr(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
		const string &name) {
	FTRACE
	dout(15) << __func__ << " " << c->cid << " " << o->oid << " " << name
						<< dendl;
	int r = 0;
	auto it = o->onode.attrs.find(name.c_str());
	if (it == o->onode.attrs.end())
		goto out;

	o->onode.attrs.erase(it);
	txc->write_onode(o);

	out:
	dout(10) << __func__ << " " << c->cid << " " << o->oid << " " << name
						<< " = " << r << dendl;
	return r;
}

int KvsStore::_rmattrs(KvsTransContext *txc, CollectionRef &c, OnodeRef &o) {
	FTRACE
	dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
	int r = 0;

	if (o->onode.attrs.empty())
		goto out;

	o->onode.attrs.clear();
	txc->write_onode(o);

	out:
	dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r
						<< dendl;
	return r;
}

/// ------------------------------------------------------------------------------------------------
/// Clone
/// ------------------------------------------------------------------------------------------------


int KvsStore::_clone_range(KvsTransContext *txc, CollectionRef &c,
		OnodeRef &oldo, OnodeRef &newo, uint64_t srcoff, uint64_t length,
		uint64_t dstoff) {

    dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
             << newo->oid << " from " << srcoff << "~" << length
             << " to offset " << dstoff << dendl;
    int r = 0;

    bufferlist bl;
    newo->exists = true;

    r = _do_read(oldo, srcoff, length, bl, 0);
    if (r < 0)
        goto out;

    r = _do_write(txc, newo, dstoff, bl.length(), bl, 0);
    if (r < 0)
        goto out;

    txc->write_onode(newo);

    r = 0;

    out:
    dout(10) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
             << newo->oid << " from " << srcoff << "~" << length
             << " to offset " << dstoff
             << " = " << r << dendl;
    return r;
}


/// ------------------------------------------------------------------------------------------------
/// Collections
/// ------------------------------------------------------------------------------------------------

void KvsStore::_queue_reap_collection(CollectionRef &c) {
	FTRACE
	dout(10) << __func__ << " " << c << " " << c->cid << dendl;
	removed_collections.push_back(c);
}

void KvsStore::_reap_collections() {
	FTRACE
    using ceph::decode;

    list<CollectionRef> removed_colls;
	{
		// _queue_reap_collection and this in the same thread.
		// So no need a lock.
		if (!removed_collections.empty())
		  removed_colls.swap(removed_collections);
		else
		  return;
	}


	list<CollectionRef>::iterator p = removed_colls.begin();
	while (p != removed_colls.end()) {
		CollectionRef c = *p;
		dout(10) << __func__ << " " << c << " " << c->cid << dendl;
		if (c->onode_map.map_any([&](OnodeRef o) {
			  ceph_assert(!o->exists);
			  if (o->flushing_count.load()) {
				dout(10) << __func__ << " " << c << " " << c->cid << " " << o->oid
					 << " flush_txns " << o->flushing_count << dendl;
				return true;
			  }
			  return false;
		})) {
		  ++p;
		  continue;
		}
		c->onode_map.clear();
		p = removed_colls.erase(p);
		dout(10) << __func__ << " " << c << " " << c->cid << " done" << dendl;
	}
	if (removed_colls.empty()) {
		dout(10) << __func__ << " all reaped" << dendl;
	} else {
		removed_collections.splice(removed_collections.begin(), removed_colls);
	}
}
int KvsStore::_create_collection(KvsTransContext *txc, const coll_t &cid,
		unsigned bits, CollectionRef *c) {
	FTRACE
	dout(15) << __func__ << " " << cid << " bits " << bits << dendl;

	int r;

    bufferlist bl;
    {
        std::unique_lock l(coll_lock);
        if (*c) {
            derr << " the collection already exists: " << cid << dendl;
            r = -EEXIST;
            goto out;
        }
        auto p = new_coll_map.find(cid);
        if (p == new_coll_map.end()) {
            TR << "new coll map does not have " << cid;
            for (const auto &c : new_coll_map) {
                TR << "new coll map has " << c;
            }
            TR << "done";
        }
        ceph_assert(p != new_coll_map.end());
        *c = p->second;
        (*c)->cnode.bits = bits;
        coll_map[cid] = *c;
        new_coll_map.erase(p);
    }
    encode((*c)->cnode, bl);

    db.add_coll(&txc->ioc, cid, bl);
	r = 0;

out:
	dout(10) << __func__ << " " << cid << " bits " << bits << " = " << r
						<< dendl;
	return r;
}

int KvsStore::_remove_collection(KvsTransContext *txc, const coll_t &cid,
		CollectionRef *c) {
	FTRACE
	int r;
    if (*c == 0) return 0;

	(*c)->flush_all_but_last();
	{
		std::unique_lock l(coll_lock);
		if (!*c) {
			r = -ENOENT;
			goto out;
		}

		size_t nonexistent_count = 0;
		assert((*c)->exists);
		if ((*c)->onode_map.map_any([&](OnodeRef o) {
			if (o->exists) {
				derr << __func__ << " " << o->oid << "(" << &o->oid <<") " << o
					 << " exists in onode_map" << dendl;
				return true;
			}
			++nonexistent_count;
			return false;
			})) {
			r = -ENOTEMPTY;
			goto out;
		}

		vector<ghobject_t> ls;
		ghobject_t next;

		// Enumerate onodes in db, up to nonexistent_count + 1
		// then check if all of them are marked as non-existent.
		// Bypass the check if returned number is greater than nonexistent_count
		r = _collection_list(c->get(), ghobject_t(), ghobject_t::get_max(),
				nonexistent_count + 1, &ls, &next, true);

		if (r >= 0) {
			bool exists = (!next.is_max());

			for (auto it = ls.begin(); !exists && it < ls.end(); ++it) {
				dout(10) << __func__ << " oid " << *it << dendl;
				auto onode = (*c)->onode_map.lookup(*it);
				exists = !onode || onode->exists;
				if (exists) {
				  dout(1) << __func__ << " " << *it
				  << " exists in db, "
				  << (!onode ? "not present in ram" : "present in ram")
				  << dendl;
				}
			}
			if (!exists) {
				_do_remove_collection(txc, c);
				r = 0;
			} else {
				derr << __func__ << " " << cid << " is non-empty"
									<< dendl;
				r = -ENOTEMPTY;
			}
		}
	}

	out:
	dout(10) << __func__ << " " << cid << " = " << r << dendl;
	return r;
}

int KvsStore::_do_remove_collection(KvsTransContext *txc, CollectionRef *c)
{
	FTRACE
	  coll_map.erase((*c)->cid);
	  txc->removed_collections.push_back(*c);
	  (*c)->exists = false;
	  _osr_register_zombie((*c)->osr.get());
	  db.rm_coll(&txc->ioc, (*c)->cid);
	  c->reset();
	  return 0;
}

int KvsStore::_split_collection(KvsTransContext *txc, CollectionRef &c,
		CollectionRef &d, unsigned bits, int rem) {

	dout(20) << __func__ << " " << c->cid << " to " << d->cid << " "
						<< " bits " << bits << dendl;
	std::unique_lock l(c->lock);
	std::unique_lock l2(d->lock);

	// flush all previous deferred writes on this sequencer.  this is a bit
	// heavyweight, but we need to make sure all deferred writes complete
	// before we split as the new collection's sequencer may need to order
	// this after those writes, and we don't bother with the complexity of
	// moving those TransContexts over to the new osr.
	{
		KvsOpSequencer *osr = txc->osr.get();
		osr->drain_preceding(txc);
	}

	// move any cached items (onodes and referenced shared blobs) that will
	// belong to the child collection post-split.  leave everything else behind.
	// this may include things that don't strictly belong to the now-smaller
	// parent split, but the OSD will always send us a split for every new
	// child.

	spg_t pgid, dest_pgid;
	bool is_pg = c->cid.is_pg(&pgid);
	assert(is_pg);
	is_pg = d->cid.is_pg(&dest_pgid);
	assert(is_pg);

	// the destination should initially be empty.
	assert(d->onode_map.empty());
	assert(d->cnode.bits == bits);

	c->split_cache(d.get());

	// adjust bits.  note that this will be redundant for all but the first
	// split call for this parent (first child).
	c->cnode.bits = bits;
	assert(d->cnode.bits == bits);

	bufferlist bl;
	encode(c->cnode, bl);
	db.add_coll(&txc->ioc, c->cid, bl);

	return 0;
}

// new feature
int KvsStore::_merge_collection(KvsTransContext *txc, CollectionRef *c,
		CollectionRef &d, unsigned bits) {
	FTRACE
	dout(15) << __func__ << " " << (*c)->cid << " to " << d->cid << " bits "
						<< bits << dendl;

	std::unique_lock l((*c)->lock);
	std::unique_lock l2(d->lock);
	int r;

	coll_t cid = (*c)->cid;

	// flush all previous deferred writes on the source collection to ensure
	// that all deferred writes complete before we merge as the target collection's
	// sequencer may need to order new ops after those writes.

	_osr_drain((*c)->osr.get());

	// move any cached items (onodes and referenced shared blobs) that will
	// belong to the child collection post-split.  leave everything else behind.
	// this may include things that don't strictly belong to the now-smaller
	// parent split, but the OSD will always send us a split for every new
	// child.

	spg_t pgid, dest_pgid;
	bool is_pg = cid.is_pg(&pgid);
	ceph_assert(is_pg);
	is_pg = d->cid.is_pg(&dest_pgid);
	ceph_assert(is_pg);

	// adjust bits.  note that this will be redundant for all but the first
	// merge call for the parent/target.
	d->cnode.bits = bits;

	// behavior depends on target (d) bits, so this after that is updated.
	(*c)->split_cache(d.get());

	// remove source collection
	{
		std::unique_lock l3(coll_lock);
		_do_remove_collection(txc, c);
	}


	r = 0;

	bufferlist bl;
	encode(d->cnode, bl);
	db.add_coll(&txc->ioc, d->cid, bl);

	dout(10) << __func__ << " " << cid << " to " << d->cid << " " << " bits "
						<< bits << " = " << r << dendl;
	return r;
}

void KvsCollection::split_cache(KvsCollection *dest) {
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
				;
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
				//dest->onode_map.cache = dest->onode_map.cache;
			}

		}
	}
	// clear data caches
	cache->trim();
	dest->cache->_trim();

}


/// -----------------------------------------------------------------------------------------------
///  Constructor / Mount / Unmount
/// -----------------------------------------------------------------------------------------------

KvsStore::KvsStore(CephContext *cct, const std::string &path) :
		ObjectStore(cct, path), db(cct), finisher(cct, "kvs_commit_finisher", "kcfin") , kv_callback_thread(this),  kv_finalize_thread(this), kv_index_thread(this){

	FTRACE
	// perf counter
	_init_perf_logger(cct);

	// create onode LRU cache
	set_cache_shards(1);

}

KvsStore::~KvsStore() {
	FTRACE
	if (logger) {
		cct->get_perfcounters_collection()->remove(logger);
		delete logger;
	}

	assert(!mounted);
	assert(fsid_fd < 0);
	assert(path_fd < 0);

	{ // remove caches
		for (auto i : onode_cache_shards) {
			delete i;
		}
		onode_cache_shards.clear();

		for (auto i : buffer_cache_shards) {
			delete i;
		}
		buffer_cache_shards.clear();
	}
}

// configure onode and data caches
void KvsStore::set_cache_shards(unsigned num) {
	FTRACE
	// should be called one time
	if (onode_cache_shards.size() > 0 || buffer_cache_shards.size() > 0) return;

	onode_cache_shards.resize(num);
	buffer_cache_shards.resize(num);

	uint64_t max_shard_onodes = KVS_CACHE_MAX_ONODES / num;
	uint64_t max_shard_buffer = KVS_CACHE_MAX_DATA_SIZE / num;


	for (unsigned i = 0; i < num; ++i) {
		auto p = KvsOnodeCacheShard::create(cct, "", logger);
		p->set_max(max_shard_onodes);
		onode_cache_shards[i] = p;
	}

	for (unsigned i = 0; i < num; ++i) {
		auto p = KvsBufferCacheShard::create(cct, "", logger);
		p->set_max(max_shard_buffer);
		buffer_cache_shards[i] =p;
	}

	derr << "KvsStore Cache: max_shard_onodes: " << max_shard_onodes << " max_shard_buffer: " << max_shard_buffer << dendl;
}

int KvsStore::mount() {
	FTRACE

    TR   <<  "Mount ----------------------------------------  " ;
	int r = _open_path();
	if (r < 0)
		return r;
	r = _open_fsid(false);
	if (r < 0)
		goto out_path;

	r = _read_fsid(&fsid);
	if (r < 0)
		goto out_fsid;

	r = _lock_fsid();
	if (r < 0)
		goto out_fsid;

	r = _open_db(false);
	if (r < 0)
		goto out_fsid;
	r = _fsck();

	if (r < 0)
		goto out_db;

	// load lid_last for atomic accesses
	this->lid_last = this->kvsb.lid_last;

	// to update superblock
	this->kvsb.is_uptodate = 0;
	r = _write_sb();
	derr << __func__ << " new KVSB superblock is written, ret = " << r << dendl;
	if (r < 0)
		goto out_db;

	r = _open_collections();
	derr << __func__ << " _open_collections, ret = " << r << dendl;
	if (r < 0)
		goto out_db;

	mounted = true;


	derr <<  "Mounted " << dendl;
    TR   <<  "Mounted ----------------------------------------  " ;

	return 0;

    TR   <<  "Mount failed ----------------------------------------  " ;

    out_db: _close_db();
	out_fsid: _close_fsid();
	out_path: _close_path();

	return r;
}

int KvsStore::_fsck_with_mount() {
	FTRACE
	int r = this->mount();  // includes fsck
	if (r < 0)
		return r;
	r = this->umount();
	if (r < 0)
		return r;
	return 0;
}



int KvsStore::_fsck() {
	FTRACE

	int ret = _read_sb();
	if (ret < 0) return 0;
	derr <<  "read superblock, ret = " << ret  << dendl;

	ret = _journal_replay();
    derr <<  "journal, ret = " << ret  << dendl;

	return ret;
}

int KvsStore::umount() {
	FTRACE

	assert(mounted);
	_osr_drain_all();

	mounted = false;

	_delete_journal();

	this->kvsb.is_uptodate = 1;
	this->kvsb.lid_last = this->lid_last;   // atomic -> local

	int r = _write_sb();
	if (r < 0) {
		derr << __func__ << "err: could not write a superblock. retcode = " << r << dendl;
		ceph_abort();
	}

	dout(20) << __func__ << " stopping kv thread" << dendl;

	_close_db();
	_reap_collections();
	_flush_cache();

	_close_fsid();
	_close_path();

	return 0;
}

void KvsStore::_init_perf_logger(CephContext *cct) {
	FTRACE
	PerfCountersBuilder b(cct, "KvsStore", l_kvsstore_first, l_kvsstore_last);
	b.add_u64_counter(l_kvsstore_read_lat, "read latency", "read latency");
	b.add_u64_counter(l_kvsstore_queue_to_submit_lat, "queue_to_submit_lat",
			"queue_to_submit_lat");
	b.add_u64_counter(l_kvsstore_submit_to_complete_lat,
			"submit_to_complete_lat", "submit_to_complete_lat");
	b.add_u64_counter(l_kvsstore_onode_read_miss_lat, "onode_read_miss_lat",
			"onode_read_miss_lat");
	b.add_u64(l_kvsstore_pending_trx_ios,
			"# of pending write I/Os in the device queue",
			"# of pending write I/Os in the device queue");

	b.add_time_avg(l_kvsstore_read_latency, "read_lat", "Average read latency");
	b.add_time_avg(l_kvsstore_write_latency, "write_lat",
			"Average write latency");
	b.add_time_avg(l_kvsstore_tr_latency, "tr_lat",
			"Average transaction latency");
	b.add_time_avg(l_kvsstore_delete_latency, "delete_lat",
			"Average delete latency");
// Async
	b.add_time_avg(l_kvsstore_iterate_latency, "iterateAsync_lat",
			"Average iterate Async latency");

	// Transaction latency breakdown

	b.add_time_avg(l_kvsstore_1_add_tr_latency, "1_add_tr_latency",
			"kv_txc: Add  latency t1 - t0");
	b.add_time_avg(l_kvsstore_2_add_onode_latency, "2_add_onode_latency",
			"kv_txc: Add  onode latency t2-t1");
	b.add_time_avg(l_kvsstore_3_add_journal_write_latency,
			"3_add_journal_write_latency",
			"kv_txc: Add journal write latency t3-t2");
	b.add_time_avg(l_kvsstore_4_aio_submit_latency, "4_aio_submit_latency",
			"kv_txc: aio submit latency t4 - t3 ");
	b.add_time_avg(l_kvsstore_5_device_io_latency, "5_device_io_latency",
			"kv_txc: Device latency t5 - t4");
	b.add_time_avg(l_kvsstore_6_tr_ordering_latency, "6_tr_ordering_latency",
			"kv_txc: tr ordering latency t6-t5");
	b.add_time_avg(l_kvsstore_7_add_finisher_latency, "7_add_finisher_latency",
			"kv_txc: Add finisher latency t7-t6");
	b.add_time_avg(l_kvsstore_8_finisher_latency, "8_finisher_latency",
			"kv_txc: Finisher latency t8 - t7");
	b.add_time_avg(l_kvsstore_9_release_latency, "9_release_latency",
			"kv_txc: Release allocated resource latency t9 -t8 ");
	b.add_time_avg(l_kvsstore_10_full_tr_latency, "10_full_tr_latency",
			"kv_txc: Complete transaction latency t9-t0");

//
	// measute prefetch onode cache hit and miss
	b.add_u64_counter(l_prefetch_onode_cache_hit, "prefetch_onode_cache_hit",
			"# of onode cache hit");
	b.add_u64_counter(l_prefetch_onode_cache_slow, "prefetch_onode_cache_wait",
			"# of onode cache waits");
	b.add_u64_counter(l_prefetch_onode_cache_miss, "prefetch_onode_cache_miss",
			"# of onode cache miss");

	logger = b.create_perf_counters();
	cct->get_perfcounters_collection()->add(logger);

	logger->set(l_kvsstore_pending_trx_ios, 0);
}



int KvsStore::_flush_cache() {
	FTRACE
	dout(10) << __func__ << dendl;
	for (auto i : onode_cache_shards) {
		i->flush();
	}
	for (auto i : buffer_cache_shards) {
		i->flush();
	}

    coll_map.clear();
	return 0;
}

int KvsStore::flush_cache(ostream *os) {
    for (auto i : onode_cache_shards) {
        i->flush();
    }
    for (auto i : buffer_cache_shards) {
        i->flush();
    }
    return 0;
}

/// -------------------
///  PATH & FSID
/// -------------------

int KvsStore::_open_path() {
	FTRACE
	assert(path_fd < 0);
	path_fd = ::open(path.c_str(), O_DIRECTORY);
	if (path_fd < 0) {
		return -errno;
	}
	return 0;
}

void KvsStore::_close_path() {
	FTRACE
	VOID_TEMP_FAILURE_RETRY(::close(path_fd));
	path_fd = -1;
}

int KvsStore::_open_fsid(bool create) {
	FTRACE
	assert(fsid_fd < 0);
	int flags = O_RDWR;
	if (create)
		flags |= O_CREAT;
	fsid_fd = ::openat(path_fd, "fsid", flags, 0644);
	if (fsid_fd < 0) {
		int err = -errno;
		derr << __func__ << " " << cpp_strerror(err) << dendl;
		return err;
	}
	return 0;
}

int KvsStore::_read_fsid(uuid_d *uuid) {
	FTRACE
	char fsid_str[40];
	memset(fsid_str, 0, sizeof(fsid_str));
	int ret = safe_read(fsid_fd, fsid_str, sizeof(fsid_str));
	if (ret < 0) {
		derr << __func__ << " failed: " << cpp_strerror(ret) << dendl;
		return ret;
	}
	if (ret > 36)
		fsid_str[36] = 0;
	else
		fsid_str[ret] = 0;
	if (!uuid->parse(fsid_str)) {
		derr << __func__ << " unparsable uuid " << fsid_str << dendl;
		return -EINVAL;
	}
	return 0;
}

int KvsStore::_write_fsid() {
	FTRACE
	int r = ::ftruncate(fsid_fd, 0);
	if (r < 0) {
		r = -errno;
		derr << __func__ << " fsid truncate failed: " << cpp_strerror(r) << dendl;
		return r;
	}
	string str = stringify(fsid) + "\n";
	r = safe_write(fsid_fd, str.c_str(), str.length());
	if (r < 0) {
		derr << __func__ << " fsid write failed: " << cpp_strerror(r) << dendl;
		return r;
	}
	r = ::fsync(fsid_fd);
	if (r < 0) {
		r = -errno;
		derr << __func__ << " fsid fsync failed: " << cpp_strerror(r) << dendl;
		return r;
	}
	return 0;
}

void KvsStore::_close_fsid() {
	FTRACE
	VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
	fsid_fd = -1;
}

int KvsStore::_lock_fsid() {
	FTRACE
	struct flock l;
	memset(&l, 0, sizeof(l));
	l.l_type = F_WRLCK;
	l.l_whence = SEEK_SET;
	int r = ::fcntl(fsid_fd, F_SETLK, &l);
	if (r < 0) {
		int err = errno;
		derr << __func__ << " failed to lock " << path << "/fsid"
							<< " (is another ceph-osd still running?)"
							<< cpp_strerror(err) << dendl;
		return -err;
	}
	return 0;
}

int KvsStore::_open_db(bool create) {
    FTRACE
    finisher.start();

    kv_stop = false;

    if (cct->_conf->kvsstore_dev_path == "") {
        return -1;
    }
    if (cct->_conf->kvsstore_csum_type == "crc32c") {
        csum_type = kvsstore_csum_crc32c;
    } else if (cct->_conf->kvsstore_csum_type == "none") {
        csum_type = kvsstore_csum_none;
    } else {
        derr << "unknown checksum algorithm: "
             << cct->_conf->kvsstore_csum_type << dendl;
    }

    if (this->db.open(cct->_conf->kvsstore_dev_path) != 0) {
        return -1;
    }

    this->db.close_iterators();

	kv_callback_thread.create("kvscallback");
	kv_index_thread.create("kvsindex");
	kv_finalize_thread.create("kvsfinalize");

    /*if (create) { // workaround for fw issue
        bptree onode_tree(&db.get_adi()->adi, 1, GROUP_PREFIX_ONODE);
        onode_tree.remove_all();
        bptree coll_tree(&db.get_adi()->adi, 1, GROUP_PREFIX_COLL);
        coll_tree.remove_all();
    }*/

	return 0;
}

int KvsStore::_read_sb() {
	FTRACE
	bufferlist v;

	int ret = db.read_sb(v);
	//int ret = db.sync_read(std::move(kvcmds.read_sb()), v);

	if (v.length() == 0) {
		derr << "sb not found: ret = " << ret << dendl;
		return -1;
	} else {
		assert(ret == KV_SUCCESS);

		// not in the performance path
		bufferptr::const_iterator p = v.front().begin_deep();

		this->kvsb.decode(p);

	}
	return 0;
}

int KvsStore::_write_sb() {
	FTRACE
	bufferlist bl;
	encode(this->kvsb, bl);

	return db.write_sb(bl);
	// derr << __func__ << " superblock bufferlist length = " << bl.length() << dendl;
}

void KvsStore::_close_db() {
	FTRACE
    derr << "kv_finalize " << dendl;
	{
		std::unique_lock l ( kv_finalize_lock );
		while (!kv_finalize_started) {
			kv_finalize_cond.wait(l);
		}
		kv_finalize_stop = true;
		kv_finalize_cond.notify_all();
	}

    kv_finalize_thread.join();

    derr << "kv_index_thread " << dendl;

    {
        std::unique_lock l ( kv_lock );
        kv_index_stop = true;
    }

    kv_index_thread.join();

    derr << "finisher " << dendl;
    finisher.wait_for_empty();
    
	finisher.stop();

	derr << "kv_callback_thread " << dendl;
    {
        kv_stop = true;
        kv_callback_thread.join();
    }

    derr << "db_close " << dendl;
	this->db.close();
    kv_finalize_stop = false;
	kv_stop = false;
    kv_index_stop = false;
}

int KvsStore::mkfs() {
	FTRACE

	int r;
	uuid_d old_fsid;
	r = _open_path();
	if (r < 0)
		return r;
	r = _open_fsid(true);
	if (r < 0)
		goto out_path_fd;

	r = _lock_fsid();
	if (r < 0)
		goto out_close_fsid;

	r = _read_fsid(&old_fsid);
	if (r < 0 || old_fsid.is_zero()) {
		if (fsid.is_zero()) {
			fsid.generate_random();
		} else {
		}
		// we'll write it last.
	} else {
		if (!fsid.is_zero() && fsid != old_fsid) {
			r = -EINVAL;
			goto out_close_fsid;
		}
		fsid = old_fsid;
		goto out_close_fsid;
	}

	r = _open_db(true);
	if (r < 0)
		goto out_close_fsid;

	this->kvsb.is_uptodate = 1;
	r = _write_sb();
	// derr << __func__ << " _write_sb r = " << r << dendl;
	if (r < 0)
		goto out_close_db;

	r = write_meta("type", "kvsstore");
	if (r < 0)
		goto out_close_db;

	// indicate mkfs completion/success by writing the fsid file
	r = _write_fsid();
	if (r == 0)
		dout(10) << __func__ << " success" << dendl;
	else
		derr << __func__ << " error writing fsid: " << cpp_strerror(r) << dendl;

	out_close_db: _close_db();

	out_close_fsid: _close_fsid();

	out_path_fd: _close_path();

	return r;
}


bool KvsStore::test_mount_in_use() {
	FTRACE
	// most error conditions mean the mount is not in use (e.g., because
	// it doesn't exist).  only if we fail to lock do we conclude it is
	// in use.
	bool ret = false;
	int r = _open_path();
	if (r < 0)
		return false;
	r = _open_fsid(false);
	if (r < 0)
		goto out_path;
	r = _lock_fsid();
	if (r < 0)
		ret = true; // if we can't lock, it is in use
	_close_fsid();
	out_path: _close_path();
	return ret;
}

int KvsStore::fiemap(const coll_t &cid, const ghobject_t &oid, uint64_t offset,
		size_t len, bufferlist &bl) {
	FTRACE
	CollectionHandle c = _get_collection(cid);
	if (!c)
		return -ENOENT;
	return fiemap(c, oid, offset, len, bl);
}

int KvsStore::fiemap(CollectionHandle &c_, const ghobject_t &oid,
		uint64_t offset, size_t length, bufferlist &bl) {
	FTRACE
	map<uint64_t, uint64_t> m;
	int r = _fiemap(c_, oid, offset, length, m);
	if (r >= 0) {
		encode(m, bl);
	}
	return r;
}

int KvsStore::fiemap(const coll_t &cid, const ghobject_t &oid, uint64_t offset,
		size_t len, map<uint64_t, uint64_t> &destmap) {
	FTRACE
	CollectionHandle c = _get_collection(cid);
	if (!c)
		return -ENOENT;
	return fiemap(c, oid, offset, len, destmap);
}

int KvsStore::fiemap(CollectionHandle &c_, const ghobject_t &oid,
		uint64_t offset, size_t length, map<uint64_t, uint64_t> &destmap) {
	FTRACE
	int r = _fiemap(c_, oid, offset, length, destmap);
	if (r < 0) {
		destmap.clear();
	}
	return r;
}

int KvsStore::_fiemap(CollectionHandle &c_, const ghobject_t &oid,
		uint64_t offset, size_t len, map<uint64_t, uint64_t> &destmap) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	if (!c->exists)
		return -ENOENT;

	std::shared_lock l(c->lock);

	OnodeRef o = c->get_onode(oid, false);
	if (!o || !o->exists) {
		return -ENOENT;
	}

	if (offset > o->onode.size)
		goto out;

	if (offset + len > o->onode.size) {
		len = o->onode.size - offset;
	}

	dout(20) << __func__ << " " << offset << "~" << len << " size "
						<< o->onode.size << dendl;

	destmap[0] = o->onode.size;

	out:
	dout(20) << __func__ << " " << offset << "~" << len << " size = 0 ("
						<< destmap << ")" << dendl;
	return 0;
}

/// ------------------------------------------------------------------------------------------------
/// OP Sequencer
/// ------------------------------------------------------------------------------------------------

void KvsStore::_osr_attach(KvsCollection *c) {
	FTRACE
// caller has RWLock on coll_map
	auto q = coll_map.find(c->cid);
	if (q != coll_map.end()) {
		c->osr = q->second->osr;
        //TR << "reusing osr " << c->cid ;
		ldout(cct, 10) << __func__ << " " << c->cid << " reusing osr " << c->osr << " from existing coll " << q->second << dendl;

	} else {
		std::lock_guard l(zombie_osr_lock);
		auto p = zombie_osr_set.find(c->cid);
		if (p == zombie_osr_set.end()) {
			c->osr = ceph::make_ref<KvsOpSequencer>(this, next_sequencer_id++, c->cid);
			ldout(cct, 10) << __func__ << " " << c->cid << " fresh osr " << c->osr << dendl;

		} else {
			c->osr = p->second;
			zombie_osr_set.erase(p);
			ldout(cct, 10) << __func__ << " " << c->cid << " resurrecting zombie osr " << c->osr << dendl;
			c->osr->zombie = false;
		}
	}
}

void KvsStore::_osr_register_zombie(KvsOpSequencer *osr) {
	FTRACE
	std::lock_guard l(zombie_osr_lock);
	dout(10) << __func__ << " " << osr << " " << osr->cid << dendl;

	osr->zombie = true;
	auto i = zombie_osr_set.emplace(osr->cid, osr);
	// this is either a new insertion or the same osr is already there
	ceph_assert(i.second || i.first->second == osr);
}

// _osr_drain_preceding is not necessary for KvsStore

void KvsStore::_osr_drain(KvsOpSequencer *osr) {
	FTRACE

	osr->drain();
}

void KvsStore::_osr_drain_all() {
	FTRACE
	dout(10) << __func__ << dendl;

	set<OpSequencerRef> s;
	vector<OpSequencerRef> zombies;
	{
		std::shared_lock l(coll_lock);
		for (auto &i : coll_map) {
			s.insert(i.second->osr);
		}
	}
	{
		std::lock_guard l(zombie_osr_lock);
		for (auto &i : zombie_osr_set) {
			s.insert(i.second);
			zombies.push_back(i.second);
		}
	}

	for (auto osr : s) {
		dout(20) << __func__ << " drain " << osr << dendl;
		osr->drain();
	}

	{
		std::lock_guard l(zombie_osr_lock);
		for (auto &osr : zombies) {
			if (zombie_osr_set.erase(osr->cid)) {
				dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
				ceph_assert(osr->q.empty());
			} else if (osr->zombie) {
				dout(10) << __func__ << " empty zombie osr " << osr << " already reaped" << dendl;
				ceph_assert(osr->q.empty());
			} else {
				dout(10) << __func__ << " empty zombie osr " << osr << " resurrected" << dendl;
			}
		}
	}

	dout(10) << __func__ << " done" << dendl;

}


/// ------------------------------------------------------------------------------------------------
/// Status
/// ------------------------------------------------------------------------------------------------


int KvsStore::statfs(struct store_statfs_t *buf, osd_alert_list_t *alerts) {

	buf->reset();

	uint64_t bytesused, capacity;
	double utilization;

	this->db.get_freespace(bytesused, capacity, utilization);
	buf->total = capacity;
	buf->available = capacity - bytesused;

	//TR << "total = " << buf->total << ", available " << buf->available ;
	return 0;
}

int KvsStore::pool_statfs(uint64_t pool_id, struct store_statfs_t *buf, bool *per_pool_omap) {
	return -ENOTSUP;
}

int KvsStore::stat(CollectionHandle &c_, const ghobject_t &oid, struct stat *st, bool allow_eio) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	if (!c->exists)
		return -ENOENT;
	dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;

	{
		std::shared_lock l(c->lock);
		OnodeRef o = c->get_onode(oid, false);
		if (!o || !o->exists)
			return -ENOENT;
		st->st_size = o->onode.size;
		derr << __func__ << " onode size = " << o->onode.size << dendl;
		st->st_blksize = 4096;
		st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
		st->st_nlink = 1;
	}

	return 0;
}

///  -----------------------------------------------
///  Exists
///  -----------------------------------------------

bool KvsStore::exists(CollectionHandle &c_, const ghobject_t &oid) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
	if (!c->exists)
		return false;

	bool r = true;

	{
		std::shared_lock l(c->lock);
		OnodeRef o = c->get_onode(oid, false);

		if (!o || !o->exists)
			r = false;
	}

	return r;
}


///  -----------------------------------------------
///  Attributes
///  -----------------------------------------------

int KvsStore::getattr(CollectionHandle &c_, const ghobject_t &oid,
		const char *name, bufferptr &value) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	dout(15) << __func__ << " " << c->cid << " " << oid << " " << name
						<< dendl;
	if (!c->exists)
		return -ENOENT;

	int r;
	{
		std::shared_lock l(c->lock);
		mempool::kvsstore_cache_other::string k(name);

		OnodeRef o = c->get_onode(oid, false);
		if (!o || !o->exists) {
			r = -ENOENT;
			goto out;
		}

		if (!o->onode.attrs.count(k)) {
			r = -ENODATA;
			goto out;
		}
		value = o->onode.attrs[k];

		r = 0;
	}
	out:
		return r;
}

int KvsStore::getattrs(CollectionHandle &c_, const ghobject_t &oid,
		map<string, bufferptr> &aset) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
	if (!c->exists)
		return -ENOENT;

	int r;
	{
		std::shared_lock l(c->lock);

		OnodeRef o = c->get_onode(oid, false);
		if (!o || !o->exists) {
			r = -ENOENT;
			goto out;
		}
		for (auto &i : o->onode.attrs) {
			aset.emplace(i.first.c_str(), i.second);
		}
		r = 0;
	}

	out:
		return r;
}

///  -----------------------------------------------
///  Read
///  -----------------------------------------------

int KvsStore::read(CollectionHandle &c_, const ghobject_t &oid, uint64_t offset,
		size_t length, bufferlist &bl, uint32_t op_flags) {
	FTRACE
    TR << "read: oid " << oid << ", offset " << offset << ", length " << length ;

	KvsCollection *c = static_cast<KvsCollection*>(c_.get());

	const coll_t &cid = c->get_cid();
	dout(15) << __func__ << " " << cid << " " << oid << " 0x" << std::hex
						<< offset << "~" << length << std::dec << dendl;

	if (!c->exists)
		return -ENOENT;

    bl.clear();

    int ret = 0;
    {
        std::shared_lock l(c->lock);
        OnodeRef o = c->get_onode(oid, false);
        if (!o || !o->exists) {
            return -ENOENT;
        }

        if (offset == length && offset == 0) {
            length = o->onode.size;
        }

        ret = _do_read(o, offset, length, bl, op_flags);

        TR << "read done : oid = " << oid << " offset " << offset << ", length " << length << ", hash = " << ceph_str_hash_linux(bl.c_str(), bl.length()) << ", result = " << ret ;
        return ret;
    }

}


void KvsStore::_do_remove_stripe(KvsTransContext *txc, OnodeRef o, uint64_t offset)
{
    auto it = o->pending_stripes.find(offset);
    if (it != o->pending_stripes.end()) {
        kvs_stripe *stripe = it->second;
        o->pending_stripes.erase(it);
        delete stripe;
    }

    db.rm_data(&txc->ioc, o->oid, (offset >> KVS_OBJECT_SPLIT_SHIFT));
}





int KvsStore::_do_read(OnodeRef o,uint64_t offset,size_t length,bufferlist& bl,uint32_t op_flags)
{
    int r = 0;
    uint64_t stripe_size = KVS_OBJECT_SPLIT_SIZE;
    uint64_t stripe_off;

    TR << "_do_read " << offset << "~" << length << " size " << o->onode.size << " oid  " << o->oid;

    bl.clear();

    if (offset > o->onode.size) {
        goto out;
    }

    if (offset + length > o->onode.size) {
        length = o->onode.size - offset;
    }

    o->flush();

    stripe_off = offset % stripe_size;
    while (length > 0) {
        bool cachehit;

        kvs_stripe *stripe = get_stripe_for_read(o, offset - stripe_off, cachehit);
        if (stripe == 0) return -ENOENT;

        TR << "_do_read - read stripe: hash = " << ceph_str_hash_linux(stripe->buffer, stripe->length()) <<", length = " << stripe->length();

        unsigned swant = std::min<unsigned>(stripe_size - stripe_off, length);
        if (stripe->length()) {
            if (swant == stripe->length()) {
                bl.append(stripe->buffer, swant);
                dout(30) << __func__ << " taking full stripe" << dendl;
            } else {
                unsigned l = 0;
                if (stripe_off < stripe->length()) {
                    l = std::min<uint64_t>(stripe->length() - stripe_off, swant);
                    bl.append(stripe->buffer + stripe_off, l);
                    dout(30) << __func__ << " taking " << stripe_off << "~" << l << dendl;
                }
                if (l < swant) {
                    bl.append_zero(swant - l);
                    dout(30) << __func__ << " adding " << swant - l << " zeros" << dendl;
                }
            }
        } else {
            dout(30) << __func__ << " generating " << swant << " zeros" << dendl;
            bl.append_zero(swant);
        }
        if (!cachehit)
            delete stripe;
        else
            o->flush_lock.unlock();
        offset += swant;
        length -= swant;
        stripe_off = 0;
    }
    r = bl.length();
    dout(30) << " result:\n";
            bl.hexdump(*_dout);
            *_dout << dendl;

    out:
    return r;
}


///  -----------------------------------------------
///  OMAPS
///  -----------------------------------------------

int KvsStore::omap_get(CollectionHandle &c_, const ghobject_t &oid,
		bufferlist *header, map<string, bufferlist> *out) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	return _omap_get(c, oid, header, out);
}


/// Get omap header
int KvsStore::omap_get_header(CollectionHandle &c_, const ghobject_t &oid,
		bufferlist *header, bool allow_eio) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	std::shared_lock l(c->lock);
	OnodeRef o = c->get_onode(oid, false);
	*header = o->onode.omap_header;
	return 0;
}


/// Get keys defined on oid
int KvsStore::omap_get_keys(CollectionHandle &c_, const ghobject_t &oid,
		set<string> *keys) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	std::shared_lock l(c->lock);
	OnodeRef o = c->get_onode(oid, false);

	for (const auto &p: o->onode.omaps) {
		keys->insert(p);
	}
	return 0;
}

/// Filters keys into out which are defined on oid
int KvsStore::omap_check_keys(CollectionHandle &c_, const ghobject_t &oid,
		const set<string> &keys, set<string> *out) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	std::shared_lock l(c->lock);
	OnodeRef o = c->get_onode(oid, false);

	for (const std::string &p : keys) {
		auto it = o->onode.omaps.find(p);
		if (it != o->onode.omaps.end()) {
			out->insert(p);
		}
	}
	return 0;
}


class KvsStore::OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  CollectionRef c;
  OnodeRef o;
  map<string,bufferlist> omap;
  map<string,bufferlist>::iterator it;
public:
  OmapIteratorImpl(KvsStore *store, CollectionRef c, OnodeRef o)
    : c(c), o(o) {

	  o->onode.fill_omap_values(&omap, [&] (const std::string &key, bufferlist &bl)->int {
			return store->db.read_omap(o->oid, o->onode.lid, key, bl);
	  });

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


ObjectMap::ObjectMapIterator KvsStore::get_omap_iterator(CollectionHandle &c_,
		const ghobject_t &oid) {
	FTRACE
	  KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	  if (!c->exists) {
	    return ObjectMap::ObjectMapIterator();
	  }

	  std::shared_lock l(c->lock);
	  OnodeRef o = c->get_onode(oid, false);
	  if (!o || !o->exists) {
	      dout(10) << __func__ << " " << oid << "doesn't exist" <<dendl;
	      return ObjectMap::ObjectMapIterator();
	  }
	  o->flush();
	  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(this, c, o));
}

/// ------------------------------------------------------------------------------------------------
/// Omap
/// ------------------------------------------------------------------------------------------------

void KvsStore::_do_omap_clear(KvsTransContext *txc, OnodeRef &o) {
	FTRACE
	if (!o->onode.has_omap()) return;

	o->flush();

	for (const std::string &user_key :o->onode.omaps) {
		db.rm_omap(&txc->ioc, o->oid, o->onode.lid, user_key);
	}

	o->onode.omaps.clear();
	o->onode.omap_header.clear();
}

int KvsStore::_omap_clear(KvsTransContext *txc, CollectionRef &c, OnodeRef &o) {
	FTRACE
	dout(15) << __func__ << " " << c->cid << " " << o->oid << dendl;
	int r = 0;
	if (o->onode.has_omap()) {
		o->flush();
		_do_omap_clear(txc, o);
		txc->write_onode(o);
	}
	dout(10) << __func__ << " " << c->cid << " " << o->oid << " = " << r
						<< dendl;
	return r;
}

int KvsStore::_omap_setkeys(KvsTransContext *txc, CollectionRef &c,
		OnodeRef &o, bufferlist &bl) {
	FTRACE
	auto p = bl.cbegin();
	__u32 num;
	decode(num, p);

	if (num > 0) {
		txc->write_onode(o);
	}

	while (num--) {

		string key;
		bufferlist list;
		decode(key, p);
		decode(list, p);
		// store key-value pair (key -> onod , value -> ssd)
		o->onode.omaps.insert(key);
		db.add_omap(&txc->ioc, o->oid, o->onode.lid, key, list);
	}

	return 0;
}

int KvsStore::_omap_setheader(KvsTransContext *txc, CollectionRef &c,
		OnodeRef &o, bufferlist &bl) {
	FTRACE
	o->onode.omap_header = bl;
	txc->write_onode(o);

	return 0;
}

int KvsStore::_omap_rmkeys(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
		bufferlist &bl) {
	FTRACE
	if (!o->onode.has_omap()) {
		return 0;
	}

	auto p = bl.cbegin();
	__u32 num;
	decode(num, p);

	if (num > 0) {
		txc->write_onode(o);
	}

	while (num--) {
		string key;
		decode(key, p);
		o->onode.omaps.erase(key);
		db.rm_omap(&txc->ioc, o->oid, o->onode.lid, key);
	}

	return 0;
}

int KvsStore::_omap_rmkey_range(KvsTransContext *txc, CollectionRef &c,
		OnodeRef &o, const string &first, const string &last) {
	FTRACE
	if (!o->onode.has_omap())
		return 0;

    set<string>::iterator p = o->onode.omaps.lower_bound(first);
	set<string>::iterator e = o->onode.omaps.lower_bound(last);

	for (auto it = p; it != e; it++) {
		const string user_key = *it;
		db.rm_omap(&txc->ioc, o->oid, o->onode.lid, user_key);
	}

	o->onode.omaps.erase(p, e);

	return 0;
}



///--------------------------------------------------------------
/// Callback Thread
///--------------------------------------------------------------

void KvsStore::_make_ordered_transaction_list(KvsTransContext *txc, deque<KvsTransContext*> &list)
{
    KvsOpSequencer *osr = txc->osr.get();
    std::lock_guard l(osr->qlock);
    txc->state = KvsTransContext::STATE_IO_DONE;
    KvsOpSequencer::q_list_t::iterator p = osr->q.iterator_to(*txc);
    while (p != osr->q.begin()) {
        --p;
        if (p->state < KvsTransContext::STATE_IO_DONE) {
            TR << "skip to next";
            return;
        }
        if (p->state > KvsTransContext::STATE_IO_DONE) {
            TR << "found prev";
            ++p;
            break;
        }
    }

    // process the stored transactions
    do {
        // add to finisher
        //KvsTransContext *t = &*p++;
        list.push_back(&*p++);

    } while (p != osr->q.end() && p->state == KvsTransContext::STATE_IO_DONE);
}


void KvsStore::_kv_finalize_thread() {
	FTRACE
	deque<KvsTransContext*> kv_committed;
    deque<KvsTransContext*> ordered_txc;
    deque<KvsTransContext*> releasing_txc;

	std::unique_lock l(kv_finalize_lock);
	assert(!kv_finalize_started);
	kv_finalize_started = true;
	kv_finalize_cond.notify_all();

    bool empty  = false;
	while (true) {

		assert(kv_committed.empty());
		if (kv_committing_to_finalize.empty()) {
			if (kv_finalize_stop)
				break;

			kv_finalize_cond.wait(l);
		} else {
			kv_committed.swap(kv_committing_to_finalize);
			l.unlock();

            TR << "kv_finalize-start: # of transactions " << kv_committed.size();
            //int num = 0;
            for (KvsTransContext *txc: kv_committed) {
                //TR << "transaction #" << num++ << ", " << (void*) txc << ": IO finished";
                _make_ordered_transaction_list(txc, ordered_txc);
            }

            kv_committed.clear();

            //TR << "Cleaning up transactions in order";
			while (!ordered_txc.empty()) {
				KvsTransContext *txc = ordered_txc.front();

				// clear write buffers

                // decrease flushing count
                for (auto ls : { &txc->onodes, &txc->modified_objects }) {
                    for (auto& o : *ls) {
                        {
                            std::lock_guard<std::mutex> l(o->flush_lock);
                            o->flush_txns.erase(txc);
                        }

                        dout(20) << __func__ << " onode " << o << " had " << o->flushing_count << dendl;
                        if (--o->flushing_count == 0 && o->waiting_count.load()) {
                            std::lock_guard l(o->flush_lock);
                            o->flush_cond.notify_all();
                            o->clear_pending_stripes();
                        }
                    }
                }

                //TR << "transactions " << (void*)txc << ":  clean up ";
                while (!txc->removed_collections.empty()) {
                    _queue_reap_collection(txc->removed_collections.front());
                    txc->removed_collections.pop_front();
                }

                OpSequencerRef osr = txc->osr;
                empty = false;
                {
                    std::lock_guard l(osr->qlock);
                    txc->state = KvsTransContext::STATE_DONE;
                    if (txc->onreadable_sync) {
                        txc->onreadable_sync->complete(0);
                        txc->onreadable_sync = NULL;
                    }
                    if (txc->onreadable) {
                        finisher.queue(txc->onreadable);
                        txc->onreadable = NULL;
                    }
                    if (txc->oncommit) {
                        finisher.queue(txc->oncommit);
                        txc->oncommit = NULL;
                    }
                    if (!txc->oncommits.empty()) {
                        finisher.queue(txc->oncommits);
                    }
                    /*
                    if (txc->ch->commit_queue) {
                        TR << "transaction " << (void*)txc << ": sending to finisher - ch->commit_queue";
                        txc->ch->commit_queue->queue(txc->oncommits);
                        TR << "transaction " << (void*) txc << ": sent to finisher - ch->commit_queue ";
                    } else {
                        TR << "transaction " << (void*) txc << ": sending to finisher - finisher ";
                        finisher.queue(txc->oncommits);
                        TR << "transaction " << (void*) txc << ": sent to finisher - finisher ";
                    }

                    */
                    while (!osr->q.empty()) {
                        KvsTransContext *t = &osr->q.front();
                        if (t->state != KvsTransContext::STATE_DONE) {
                            break;
                        }
                        //TR << "adding to releasing_txc " << (void*)t;
                        osr->pop_front_nolock();
                        releasing_txc.push_back(t);
                    }

                    empty = osr->q.empty();
                    if (empty)
                        osr->qcond.notify_all();
                }

                if (empty && osr->zombie) {
                    std::lock_guard l(zombie_osr_lock);
                    if (zombie_osr_set.erase(osr->cid)) {
                        dout(10) << __func__ << " reaping empty zombie osr " << osr << dendl;
                    } else {
                        dout(10) << __func__ << " empty zombie osr " << osr << " already reaped" << dendl;
                    }
                }


                ordered_txc.pop_front();
			}

            // reap txc
            while (!releasing_txc.empty()) {
                auto t = releasing_txc.front();
                releasing_txc.pop_front();

                //TR << "transaction " << (void*) t << ": free memory";
                _txc_release_alloc(t);
                delete t;
            }

            //TR << "reap collections";
			// this is as good a place as any ...
			_reap_collections();

            //TR << "kv-finalize-end ";
			l.lock();
		}
	}

	kv_finalize_started = false;

}

void KvsStore::_kv_callback_thread() {
	FTRACE
	uint32_t toread = 128;

	while (!kv_stop) {
		if (kv_stop) {
			derr << "kv_callback_thread: stop requested" << dendl;
			break;
		}

		if (this->db.is_opened()) {
			this->db.poll_completion(toread, 100);
		}
	}

}



///  -----------------------------------------------
///  Collections
///  -----------------------------------------------


CollectionRef KvsStore::_get_collection(const coll_t &cid) {
	FTRACE
	std::shared_lock l(coll_lock);
	ceph::unordered_map<coll_t, CollectionRef>::iterator cp = coll_map.find(cid);
	if (cp == coll_map.end()) {
		return CollectionRef();
	}
	return cp->second;
}

ObjectStore::CollectionHandle KvsStore::open_collection(const coll_t &cid) {
	return _get_collection(cid);
}

ObjectStore::CollectionHandle KvsStore::create_new_collection(
		const coll_t &cid) {
	FTRACE

	std::unique_lock l{coll_lock};
	KvsCollection *c(
			new KvsCollection(this,
				    onode_cache_shards[cid.hash_to_shard(onode_cache_shards.size())],
				    buffer_cache_shards[cid.hash_to_shard(buffer_cache_shards.size())],
					cid));
	new_coll_map[cid] = c;

	_osr_attach(c);

    return c;
}

void KvsStore::set_collection_commit_queue(const coll_t &cid,
		ContextQueue *commit_queue) {
	FTRACE
	/*
	if (commit_queue) {
		std::shared_lock l(coll_lock);
		if (coll_map.count(cid)) {
			coll_map[cid]->commit_queue = commit_queue;
		} else if (new_coll_map.count(cid)) {
			new_coll_map[cid]->commit_queue = commit_queue;
		}
	}*/
}


int KvsStore::set_collection_opts(const coll_t &cid, const pool_opts_t &opts) {
	CollectionHandle ch = _get_collection(cid);
	if (!ch)
		return -ENOENT;
	KvsCollection *c = static_cast<KvsCollection*>(ch.get());
	if (!c->exists)
		return -ENOENT;
	return 0;
}

int KvsStore::set_collection_opts(CollectionHandle &ch,
		const pool_opts_t &opts) {
	dout(15) << __func__ << " " << ch->cid << " options " << opts << dendl;
	KvsCollection *c = static_cast<KvsCollection*>(ch.get());
	if (!c->exists)
		return -ENOENT;
	return 0;
}





int KvsStore::list_collections(vector<coll_t> &ls) {
	FTRACE
	std::shared_lock l(coll_lock);
	for (ceph::unordered_map<coll_t, CollectionRef>::iterator p =
			coll_map.begin(); p != coll_map.end(); ++p)
		ls.push_back(p->first);
	return 0;
}

bool KvsStore::collection_exists(const coll_t &c) {
	FTRACE
	std::shared_lock l(coll_lock);
	return coll_map.count(c);
}

int KvsStore::collection_empty(CollectionHandle &ch, bool *empty) {
	FTRACE
	dout(15) << __func__ << " " << ch->cid << dendl;
	vector<ghobject_t> ls;
	ghobject_t next;
	int r = collection_list(ch, ghobject_t(), ghobject_t::get_max(), 1, &ls,
			&next);
	if (r < 0) {
		derr << __func__ << " collection_list returned: "
							<< cpp_strerror(r) << dendl;
		return r;
	}
	*empty = ls.empty();
	dout(10) << __func__ << " " << ch->cid << " = " << (int) (*empty) << dendl;
	return 0;
}

int KvsStore::collection_bits(CollectionHandle &ch) {
	FTRACE
	dout(15) << __func__ << " " << ch->cid << dendl;
	KvsCollection *c = static_cast<KvsCollection*>(ch.get());
	std::shared_lock l(c->lock);
	dout(10) << __func__ << " " << ch->cid << " = " << c->cnode.bits << dendl;
	return c->cnode.bits;
}


int KvsStore::collection_list(CollectionHandle &c_, const ghobject_t &start,
		const ghobject_t &end, int max, vector<ghobject_t> *ls,
		ghobject_t *pnext) {
	FTRACE
	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	c->flush();

	dout(15) << __func__ << "-MAIN: " << c->cid << " bits " << c->cnode.bits
						<< " start_oid " << start << " end_oid " << end
						<< " max " << max << dendl;

	int r;
	{
		std::shared_lock l(c->lock);
		r = _collection_list(c, start, end, max, ls, pnext);
        //TR << "collection list result = " << ls->size() ;
	}

	dout(15) << __func__ << "-DONE: " << c->cid << " start " << start
						<< " end " << end << " max " << max << " = " << r
						<< ", ls.size() = " << ls->size() << ", next = "
						<< (pnext ? *pnext : ghobject_t()) << dendl;
	return r;
}





///--------------------------------------------------------
/// Write Functions
///--------------------------------------------------------

int KvsStore::_write(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
		uint64_t offset, size_t length, bufferlist &bl, uint32_t fadvise_flags) {
	FTRACE

	TR << "_write: start - oid " << o->oid << ", onode size " << o->onode.size << ", offset " << offset << ", length " << length << ", hash = " << ceph_str_hash_linux(bl.c_str(), bl.length());
    int r = _do_write(txc, o, offset, length, bl, fadvise_flags);
    txc->write_onode(o);

    TR << "_write: finished - oid " << o->oid << ", onode size " << o->onode.size << ", offset " << offset << ", length " << length << ", hash = " << ceph_str_hash_linux(bl.c_str(), bl.length());
	return r;
}

kvs_stripe* KvsStore::get_stripe_for_write(OnodeRef o,int stripe_off) {
    auto it = o->pending_stripes.find(stripe_off);
    if (it == o->pending_stripes.end()) {
        kvs_stripe *stripe = new kvs_stripe((stripe_off >> KVS_OBJECT_SPLIT_SHIFT));
        o->pending_stripes[stripe_off] = stripe;
        return stripe;
    } else {
        return it->second;
    }
}

kvs_stripe* KvsStore::get_stripe_for_read(OnodeRef o, int stripe_off, bool &cachehit) {
    auto it = o->pending_stripes.find(stripe_off);
    if (it == o->pending_stripes.end()) {
        kvs_stripe *stripe = new kvs_stripe((stripe_off >> KVS_OBJECT_SPLIT_SHIFT));
        int r = db.read_block(o->oid, stripe->pgid, stripe->buffer, stripe->len);
        if (r != 0) {
            delete stripe;
            return 0;
        }

        cachehit = false;
        return stripe;
    } else {
        cachehit = true;
        o->flush_lock.lock();
        return it->second;
    }
}


kvs_stripe* KvsStore::get_stripe_for_rmw(OnodeRef o, int stripe_off) {
    auto it = o->pending_stripes.find(stripe_off);
    if (it == o->pending_stripes.end()) {
        kvs_stripe *stripe = new kvs_stripe((stripe_off >> KVS_OBJECT_SPLIT_SHIFT));
        int r = db.read_block(o->oid, stripe->pgid, stripe->buffer, stripe->len);
        if (r != 0) {
            stripe->clear();
        }

        o->pending_stripes[stripe_off] = stripe;
        return stripe;
    } else {
        return it->second;
    }
}

void KvsStore::_do_write_stripe(KvsTransContext *txc, OnodeRef o, kvs_stripe *stripe)
{
    db.add_userdata(&txc->ioc, o->oid, stripe->buffer, stripe->get_pos(), stripe->pgid);
}

int KvsStore::_do_write(KvsTransContext *txc,
                      OnodeRef o,
                      uint64_t offset, uint64_t length,
                      bufferlist& orig_bl,
                      uint32_t fadvise_flags)
{
    static const uint64_t stripe_size = KVS_OBJECT_SPLIT_SIZE;

    int r = 0;

    TR << o->oid << " " << offset << "~" << length<< " - have " << o->onode.size
             << " bytes, oid " << o->oid;

    o->exists = true;

    if (length == 0) {
        return 0;
    }

    unsigned bl_off = 0;

    // First fill the holes i.e. if offset > o->onode.size -- that is if its a hole.
    uint64_t prev_offset = o->onode.size; // size of object
    uint64_t desired_offset = offset;

    int64_t hole_size = (desired_offset - prev_offset);
    TR << o->oid  << " 1";
    if (hole_size > 0){
    	TR << o->oid  << " 2. ";
    	//bufferlist hole_bf;   // Intitalize hole bufferlist 00000000000000000//
    	//hole_bf.append_zero(hole_size); 
		int64_t holebl_off = 0;  // offset of the hole bufferlist
    	
    	// Now fill the hole in the stripes
 		/* 
			Case 1: hole_size <= end of current stripe  | aaaa0000 |
			Case 2: hole_size > end of current stripe   | aaaa0000 | 00 
			Case 3: hole starts in a new stripe and end in boundary |aaaaaaaa|0000000|0000000|
			Case 4: hole starts in a new stripe and ends in new     |aaaaaaaa|0000000|00000  |
 		*/
    	while (hole_size > 0){
    		TR << o->oid  << " 3.";
    		uint64_t offset_rem = prev_offset % stripe_size; // Offset in the current stripe from where writing is allowed if offset_rem = 0, it means new stripe should be created
    		uint64_t end_rem = (prev_offset + hole_size) % stripe_size; // end offset in the stripe where it ends, end_rem = 0 means the stripe is to completely filled
    		
    		// Case 3: offset & length are aligned
    		if (offset_rem == 0 && end_rem == 0){ // 
    			kvs_stripe *stripe = get_stripe_for_write(o, bl_off); // new stripe
    			stripe->set_pos(0);
    			stripe->append_zero(stripe_size);
    			//stripe->substr_of(hole_bf, holebl_off, stripe_size);
    			_do_write_stripe(txc, o, stripe);
    			// update hole offset, size of object, size of hole
    			holebl_off += stripe_size;
    			prev_offset += stripe_size; // size of obj
    			hole_size -= stripe_size;
    			continue;
    		} // end of if

    		uint64_t stripe_off = prev_offset - offset_rem; // beginning offset of the current stripe, if offset #5, stripe_off = 5*8192
    		kvs_stripe *stripe = 0; 

    		// Case 1 : Stripe already exists
    		if (stripe_off < o->onode.size && (stripe_off > 0 || hole_size < stripe_size)){
            	stripe = get_stripe_for_rmw(o, stripe_off);
            	if (stripe->length() != stripe_size)
                	throw "invalid stripe size";    			
    		}
    		else{ // create new stripe
    			stripe = get_stripe_for_write(o, stripe_off);
    		}
    		TR << o->oid  << " 4.";
    		stripe->set_pos(0);
    		// Now we know which stripe to write
    		int64_t total_usable = stripe_size - offset_rem; // total usable in cur stripe
    		
    		// if stripe has already been written to
    		stripe->set_pos(offset_rem);
 			
 			int64_t total_zerofill = total_usable;
 			if (hole_size <= total_usable){
 				total_zerofill = hole_size;
 			}

    		stripe->append_zero(total_zerofill);
    		_do_write_stripe(txc, o, stripe);
    		holebl_off += total_zerofill;
    		prev_offset += total_zerofill;
    		hole_size -= total_zerofill;
    	} // end of while
    	TR << o->oid  << " 5 offset = " << offset << ", prev_offset = " << prev_offset 
    	   << ", holebl_off = " << holebl_off ;
    	offset = prev_offset;
    	TR << o->oid  << " 5 hole filled";
    } // end of hole 

    TR << o->oid  << " 6 length = " << length;
    while (length > 0) {
        uint64_t offset_rem = offset % stripe_size;     // remaining data
        uint64_t end_rem = (offset + length) % stripe_size;
        if (offset_rem == 0 && end_rem == 0) {  // offset & length are aligned
            kvs_stripe *stripe = get_stripe_for_write(o, bl_off);
            stripe->substr_of(orig_bl, bl_off, stripe_size);
            _do_write_stripe(txc, o, stripe);
            offset += stripe_size;
            length -= stripe_size;
            bl_off += stripe_size;
            continue;
        }

        uint64_t stripe_off = offset - offset_rem;
        kvs_stripe *stripe = 0;
        TR << "stripe_off " << stripe_off << ", length " << length;

        if (stripe_off < o->onode.size && (stripe_off > 0 || (length < stripe_size))) {
            stripe = get_stripe_for_rmw(o, stripe_off);
            if (stripe->length() != stripe_size)
                throw "invalid stripe size";
        } else {
            stripe = get_stripe_for_write(o, stripe_off);
        }

        TR << "prev length == " << stripe->length();

        stripe->set_pos(0);

        if (offset_rem) {
            // start_offset > 0 --> reuse ( 0 ~ start offset )
            unsigned p = std::min<uint64_t>(stripe->length(), offset_rem);
            if (p) {
                TR << __func__ << " reuse leading " << p << " bytes";
                stripe->set_pos(p);
            }
           // if (p < offset_rem) {
           //     TR << __func__ << " add leading " << offset_rem - p << " zeros" ;
           //     stripe->append_zero(offset_rem - p);
           // }
    
        } 

        // update bytes between start_offset ~ end_offset
        unsigned use = stripe_size - offset_rem;
        if (use > length)
            use -= stripe_size - end_rem;

 		//TR << " before stripe->append , bl_off= " << bl_off;
        stripe->append(orig_bl, bl_off, use);
       // cout << "-- after:\n";
      //  orig_bl.hexdump(cout);
       // TR << " after stripe->append ";
       
        bl_off += use;

        if (end_rem) {
            // end_offset > 0 --> reuse ( end_offset ~ length )
            if (end_rem < stripe->length()) {
                unsigned l = stripe->length() - end_rem;
                stripe->inc_pos(l);
            }
        }

        _do_write_stripe(txc, o, stripe);

        offset += use;
        length -= use;
    }

    if (offset > o->onode.size) {
        dout(20) << __func__ << " extending size to " << offset + length
                 << dendl;
        o->onode.size = offset;
    }
  TR << " END-- " << o->oid << " " << offset << "~" << length<< " - have " << o->onode.size
             << " bytes, oid " << o->oid;   
    return r;

}

int KvsStore::_do_zero(KvsTransContext *txc, CollectionRef &c, OnodeRef &o,
		uint64_t offset, size_t length) {
	FTRACE
    int r = 0;
    o->exists = true;

    const uint64_t stripe_size = KVS_OBJECT_SPLIT_SIZE;

    if (stripe_size) {
        uint64_t end = offset + length;
        uint64_t pos = offset;
        uint64_t stripe_off = pos % stripe_size;
        while (pos < offset + length) {
            if (stripe_off || end - pos < stripe_size) {

                kvs_stripe *stripe = get_stripe_for_rmw(o, pos - stripe_off);

                stripe->set_pos(std::min<uint32_t>(stripe->length(), (uint32_t)stripe_off));

                if (end >= pos - stripe_off + stripe_size ||
                    end >= o->onode.size) {
                } else {

                    auto len = end - (pos - stripe_off + stripe->get_pos());
                    stripe->append_zero(len);
                    if (stripe->length() > stripe->get_pos()) {
                        unsigned l = stripe->length() - stripe->get_pos();
                        stripe->inc_pos(l);
                    }
                }
                _do_write_stripe(txc, o, stripe);
                pos += stripe_size - stripe_off;
                stripe_off = 0;
            } else {
                dout(20) << __func__ << " rm stripe " << pos << dendl;
                _do_remove_stripe(txc, o, pos - stripe_off);
                pos += stripe_size;
            }
        }
    }
    if (length > 0 && offset + length > o->onode.size) {
        o->onode.size = offset + length;
        dout(20) << __func__ << " extending size to " << offset + length
                 << dendl;
    }
    txc->write_onode(o);

    dout(10) << __func__ << " " << c->cid << " " << o->oid
             << " " << offset << "~" << length
             << " = " << r << dendl;
    return r;
}

int KvsStore::_do_truncate(KvsTransContext *txc,  OnodeRef o,
		uint64_t offset) {
	FTRACE
	dout(15) << __func__  << " " << o->oid << " 0x"
						<< std::hex << offset << std::dec << dendl;

    const uint64_t stripe_size = KVS_OBJECT_SPLIT_SIZE;

    o->flush();

    // trim down stripes
    if (stripe_size) {
        uint64_t pos = offset;
        uint64_t stripe_off = pos % stripe_size;
        while (pos < o->onode.size) {
            if (stripe_off) {
                kvs_stripe *stripe = get_stripe_for_rmw(o, pos - stripe_off);

                stripe->set_pos(std::min<uint32_t>(stripe_off, stripe->length()));

                _do_write_stripe(txc, o, stripe);

                pos += stripe_size - stripe_off;
                stripe_off = 0;
            } else {
                dout(20) << __func__ << " rm stripe " << pos << dendl;
                _do_remove_stripe(txc, o, pos - stripe_off);
                pos += stripe_size;
            }
        }
        /*
        // trim down cached tail
        if (o->tail_bl.length()) {
            if (offset / stripe_size != o->onode.size / stripe_size) {
                dout(20) << __func__ << " clear cached tail" << dendl;
                o->clear_tail();
            }
        }*/
    }

    o->onode.size = offset;
    dout(10) << __func__ << " truncate size to " << offset << dendl;

    txc->write_onode(o);
    return 0;
}


void KvsStore::_txc_aio_submit(KvsTransContext *txc) {
	FTRACE

	db.aio_submit(txc);

    if (txc->osr->kv_submitted_waiters) {
        std::lock_guard l(txc->osr->qlock);
        txc->osr->qcond.notify_all();
    }
    txc->submitted = true;

}



int KvsStore::_clone(KvsTransContext *txc, CollectionRef &c, OnodeRef &oldo, OnodeRef &newo) {

    dout(15) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
             << newo->oid << dendl;
    int r = 0;
    if (oldo->oid.hobj.get_hash() != newo->oid.hobj.get_hash()) {
        derr << __func__ << " mismatched hash on " << oldo->oid
             << " and " << newo->oid << dendl;
        return -EINVAL;
    }

    bufferlist bl;
    newo->exists = true;

    // data
    oldo->flush();

    r = _do_read(oldo, 0, oldo->onode.size, bl, 0);
    if (r < 0)
        goto out;

    // truncate any old data
    r = _do_truncate(txc, newo, 0);
    if (r < 0)
        goto out;

    r = _do_write(txc, newo, 0, oldo->onode.size, bl, 0);
    if (r < 0)
        goto out;

    newo->onode.attrs = oldo->onode.attrs;

    // clear newo's omap
    if (newo->onode.has_omap()) {
        dout(20) << __func__ << " clearing old omap data" << dendl;
        newo->flush();
        _do_omap_clear(txc, newo);
    }

    // clone attrs, omap , omap header
    newo->onode.omaps = oldo->onode.omaps;
    newo->onode.omap_header = oldo->onode.omap_header;

    for (const std::string &name : newo->onode.omaps) {
        bufferlist bl;
        int ret = db.read_omap(oldo->oid, oldo->onode.lid, name, bl);
        if (ret != 0) {
            derr << "omap_get_values failed: ret = " << ret << dendl;
            r = -ENOENT;
        }
        db.add_omap(&txc->ioc, newo->oid, newo->onode.lid, name, bl);
    }


    txc->write_onode(newo);
    r = 0;

    out:
    dout(10) << __func__ << " " << c->cid << " " << oldo->oid << " -> "
             << newo->oid << " = " << r << dendl;
    return r;
}


///--------------------------------------------------------
/// Remove Functions
///--------------------------------------------------------


int KvsStore::_do_remove(KvsTransContext *txc, CollectionRef &c, OnodeRef o) {
	FTRACE
	if (!o->exists)
		return 0;

    _do_truncate(txc, o, 0);

    o->onode.size = 0;
	if (o->onode.has_omap()) {
		o->flush();
		_do_omap_clear(txc, o);
	}

	o->exists = false;
    o->onode = kvsstore_onode_t();
    txc->note_removed_object(o);

	db.rm_onode(&txc->ioc, o->oid);

	return 0;
}

///--------------------------------------------------------
/// Read Functions
///--------------------------------------------------------
#if 0
int KvsStore::_read_data(KvsTransContext *txc, const ghobject_t &oid, uint64_t offset, size_t length, bufferlist &bl) {
	FTRACE
	bl.clear();
	KvsStoreDataObject *datao = 0;
	if (txc) {
        datao = txc->get_databuffer(oid);
        datao->readers++;
    }
	else {
        std::lock_guard<std::mutex> l (writing_lock);
        auto it = pending_datao.find(oid);
        if (it != pending_datao.end()) {
            datao = it->second;
        }
        else {
            datao = new KvsStoreDataObject;
            datao->persistent = true;
        }
        datao->readers++;
	}

	int r = datao->read(offset, length, bl, [&] (char* data, int pageid, uint32_t &nread)->int  {
        //TR << "read block: oid " << oid << ", pageid " << pageid << ", into " << (void *)data;
        return db.read_block(oid, pageid, data, nread);
	});

	{
        std::lock_guard<std::mutex> l (writing_lock);
        datao->readers--;
        if (!txc && datao->persistent && datao->readers == 0) {
            auto it = pending_datao.find(oid);
            if (it != pending_datao.end()) {
                KvsStoreDataObject *d = it->second;
                pending_datao.erase(it);
                //TR << "Delete pending DAO on reads " << (void*)d;
		        delete d;
            } else {
                //TR << "Delete local DAO on reads"  << (void*)datao;
                delete datao;
            }
        }
	}


	if (r >= 0) {
		return r;
	} else {
	    bl.clear();
        return -ENOENT;
    }
}
#endif

/// Get key values
int KvsStore::omap_get_values(CollectionHandle &c_, const ghobject_t &oid,
		const set<string> &keys, map<string, bufferlist> *out) {
	FTRACE

	//TR << "omap get values oid = " << oid;
    //TRBACKTRACE

	KvsCollection *c = static_cast<KvsCollection*>(c_.get());
	std::shared_lock l(c->lock);
	OnodeRef o = c->get_onode(oid, false);

	for (const std::string &p : keys) {
	    if (o->onode.omaps.find(p) != o->onode.omaps.end()) {
            //TR << "omap get values key  = " << p;
            int ret = db.read_omap(oid, o->onode.lid, p, (*out)[p]);
            if (ret != 0) {
                derr << "omap_get_values failed: ret = " << ret << dendl;
            }
	    }
	}
	return 0;
}



int KvsStore::_omap_get(KvsCollection *c, const ghobject_t &oid,
		bufferlist *header, map<string, bufferlist> *out) {
	FTRACE
	std::shared_lock l(c->lock);
	OnodeRef o = c->get_onode(oid, false);
	*header = o->onode.omap_header;

	o->onode.fill_omap_values(out, [&] (const std::string &key, bufferlist &bl)->int {
		// read omap value
		return db.read_omap(oid, o->onode.lid, key, bl);
	});
	return 0;
}
