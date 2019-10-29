
#include "kvsstore_kadi.h"
#include "kvsstore_types.h"
#include "kvs_debug.h"
#include "KvsStore.h"





/// Callback functions
/// -----------------------------------------------------------------

void write_callback(kv_io_context &op, void* private_data) {
    KvsTransContext *txc= (KvsTransContext *)private_data;
    //lderr(txc->cct) << "write callback: opcode " << op.opcode <<  dendl;
    //txc->update_latency(op.opcode, op.latency);
    txc->aio_finish(&op);
}

void prefetch_callback(kv_io_context &op, void *private_data) {
	bufferlist bl;
	KvsPrefetchContext *txc = (KvsPrefetchContext*) private_data;
	bool exist = false;

	KvsOnode *on = txc->onode;

	if (op.retcode == KV_SUCCESS && op.value->length != 0) {

		auto v = bufferlist::static_from_mem((char*) op.value->value, op.value->length);
		bufferptr::const_iterator p = v.front().begin_deep();
		on->onode.decode(p);

		for (auto &i : on->onode.attrs) {
			i.second.reassign_to_mempool(mempool::mempool_kvsstore_cache_other);
		}
		exist = true;
	}

	delete txc;

	{
		std::unique_lock < std::mutex > plock(on->prefetch_lock);
		on->exists = exist;
		on->prefetched = true;
		on->prefetch_cond.notify_all();
	}

}

kv_result KvsStoreKADIAdapter::sync_write(const std::tuple<uint8_t, kv_key *, kv_value *> &&pair) {
	uint8_t spaceid; kv_key *key; kv_value *value;
	std::tie(spaceid, key, value) = pair;
	kv_result ret = kv_store(spaceid, key, value);
	KvsMemPool::Release_key(key);
	KvsMemPool::Release_value(value);
	return ret;
}

kv_result KvsStoreKADIAdapter::sync_read (const std::tuple<uint8_t, kv_key *, kv_value *> &&pair, uint64_t offset, size_t length, bufferlist &bl, bool &ispartial) {
	uint8_t spaceid; kv_key *key; kv_value *value;
	std::tie(spaceid, key, value) = pair;

	kv_result ret = kv_retrieve(spaceid, key, value, offset, length, bl, ispartial);
#if 0
	derr << __func__ << " (read) key = " << printHexKey((const char*)key->key, key->length) 
					 << " (read) bufferlist length = " << bl.length()
					 << ", ret = " << ret << dendl;
#endif

	KvsMemPool::Release_key(key);
	KvsMemPool::Release_value(value);
	return ret;
}

kv_result KvsStoreKADIAdapter::sync_read (const std::tuple<uint8_t, kv_key *, kv_value *> &&pair, bufferlist &bl) {
	bool ispartial;
	return sync_read(std::move(pair), 0, 0, bl, ispartial);
}
/// Retrieve
/// -----------------------------------------------------------------

kv_result KvsStoreKADIAdapter::kv_store(uint8_t space_id, kv_key *key, kv_value *value) {
    if (key == 0 || key->key == 0 || value == 0 || value->value == 0) {
        ceph_abort_msg("NULL parameters in kv_store");
    }
    return adi.kv_store_sync(space_id, key, value);
}

kv_result KvsStoreKADIAdapter::kv_retrieve(uint8_t space_id, kv_key *key, kv_value *value){
    if (key == 0 || key->key == 0 || value == 0 || value->value == 0) {
        ceph_abort_msg("NULL parameters in kv_retrieve");
    }

    const std::function< kv_value*(kv_value *,int) > realloc_func =  [&] (kv_value *v, int length) {
    	iolog("kv_retrieve: kv_retrieve_sync - realloc, new length = ", key ,length);
		KvsMemPool::free_memory(v->value);
		v->length = KvsMemPool::get_aligned_size(length, 4096);
		v->value  = KvsMemPool::alloc_memory(v->length);
		return v;
	};


	const int ret = adi.kv_retrieve_sync(space_id, key, value, realloc_func);

	iolog("kv_retrieve: kv_retrieve_sync ", key, ret);

	return ret;
}

kv_result KvsStoreKADIAdapter::kv_retrieve(uint8_t space_id, kv_key *key, kv_value *value, uint64_t offset, size_t length, bufferlist &bl, bool &ispartial) {

	const int retcode = kv_retrieve(space_id, key, value);

	if (retcode == KV_SUCCESS) {
        int64_t len = length;
        if (offset + length > value->length || (offset == 0 && length == 0)) {
            len  = (int64_t)value->length - offset;
            if (len < 0) len = 0;
        }

        if (len > 0) {
            bl.append((const char *)((char*)value->value + offset), len);
        }

        ispartial = value->actual_value_size == 0 || offset != 0 || len != value->actual_value_size;
    }

    return retcode;
}

kv_result KvsStoreKADIAdapter::kv_retrieve(uint8_t space_id, kv_key *key, bufferlist &bl, int valuesize) {

	malloc_unique_ptr<char> buffer = make_malloc_unique<char>(valuesize);
	kv_value value;
	value.length = valuesize;
	value.value = buffer.get();
	value.offset = 0;
	value.needfree = false;

	iolog("kv_retrieve - buferlist: ", key);
	const kv_result retcode = kv_retrieve(space_id, key, &value);

    if (retcode == KV_SUCCESS) {
        bl.append((const char *)value.value, value.length);
    }
    return retcode;
}


/// Delete
/// -----------------------------------------------------------------


kv_result KvsStoreKADIAdapter::kv_delete(uint8_t space_id, kv_key *key) {
    if (key == 0 || key->key == 0) {
        ceph_abort_msg("NULL parameters in kv_delete");
    }

    int ret = adi.kv_delete_sync(space_id, key);
    if (ret != 0) {
		return KVS_ERR_IO;
	}
	return KVS_SUCCESS;
}

/// Transaction
/// -----------------------------------------------------------------


kv_result KvsStoreKADIAdapter::write_journal(KvsTransContext *txc) {
	kv_result res = KV_SUCCESS;

	int num_batch_cmds = txc->ioc.journal.size();
	if (num_batch_cmds > 0) {

		res = adi.batch_submit(&txc->ioc.journal, 0);
		iolog("write journal", 0, res);
	}
	return res;
}

kv_result KvsStoreKADIAdapter::aio_submit(KvsPrefetchContext *ctx)
{
	uint8_t spaceid;
	kv_key *key;
	kv_value *value;
    std::tie(spaceid, key, value) = ctx->command;

	return adi.kv_retrieve_aio(spaceid, key, value, { prefetch_callback, ctx });
}

kv_result KvsStoreKADIAdapter::aio_submit(KvsTransContext *txc)
{
    kv_result res = 0;

    const int num_batch_cmds = txc->ioc.batchctx.size();

    std::list<std::tuple<uint8_t, kv_key *, kv_value *> >::iterator e = txc->ioc.running_aios.begin();
    txc->ioc.running_aios.splice(e, txc->ioc.pending_aios);
    txc->ioc.num_running = txc->ioc.running_aios.size() + num_batch_cmds;
    txc->ioc.start = ceph_clock_now();

   // derr << __func__ << " submit: # of kv requests " << txc->ioc.running_aios.size() << ", # of batch requests " << num_batch_cmds << dendl;

    {
        std::unique_lock<std::mutex> lk(txc->ioc.running_aio_lock);
        res = submit_aio_commands(txc->ioc.running_aios.begin(), txc->ioc.running_aios.end(), static_cast<void*>(txc));
    }

    iolog("KvsTransContext - batch submit aios: num batch commands ", 0, num_batch_cmds);
    adi.batch_submit_aio(&txc->ioc.batchctx, 0, { write_callback, static_cast<void*>(txc) });

    return res;
}


kv_result KvsStoreKADIAdapter::submit_aio_commands(aio_iter begin, aio_iter end, void *priv)
{
	uint8_t spaceid;
	kv_key *key;
	kv_value *value;
	kv_result res;

    aio_iter cur = begin;
    while (cur != end) {

        std::tie(spaceid, key, value) = *cur;

		if (value == 0) { // delete
			res = adi.kv_delete_aio(spaceid, key, { write_callback, priv });
			iolog("submit running aios: kv_delete_aio", key, res);
		}
		else {
			res = adi.kv_store_aio(spaceid, key, value, { write_callback, priv });
			iolog("submit_running_aios: kv_store_aio", key, res);
		}

        if (res != 0) return res;

        ++cur;
    }

    return KV_SUCCESS;
}



void KvsStoreKADIAdapter::iolog_impl(const std::string &message, kv_key *key, int ret) {
	if (key)
		derr << message << " : " << print_key((const char*)key->key, key->length) << "--> ret = " << ret << dendl;
	else
		derr << message << " --> ret = " << ret << dendl;
}
