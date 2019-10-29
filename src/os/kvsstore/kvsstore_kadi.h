#ifndef SRC_KVSSD_KVSSTORE_KADI_H_
#define SRC_KVSSD_KVSSTORE_KADI_H_

#include "os/ObjectStore.h"
#include "kadi/kadi_cmds.h"
#include "kvsstore_types.h"

//class KvsTransContext;
//class KvsReadContext;
//class KvsIterContext;
//class KvsReadIterContext;
//class KvsSyncWriteContext;

//#define ENABLE_KVSSTORE_IOLOG

class KvsStoreKADIAdapter
{
	CephContext* cct;
	KADI adi;

public:

	typedef std::list<std::tuple<uint8_t, kv_key *, kv_value *> >::iterator aio_iter;
	KvsStoreKADIAdapter(void *c): cct((CephContext*)c), adi(cct){}

	inline kv_result iter_readall(kv_iter_context *iter_ctx, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, int space_id)
	{
		iolog("iter_readall", 0,0);

		return adi.iter_readall(iter_ctx, buflist, space_id);
	}

	inline kv_result iter_readall_aio(kv_iter_context *iter_ctx, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, int space_id)
	{
		iolog("iter_readall_aio", 0,0);
		return adi.iter_readall_aio(iter_ctx, buflist, space_id);
	}

	inline int get_freespace(uint64_t &bytesused, uint64_t &capacity, double &utilization) {
		return adi.get_freespace(bytesused, capacity, utilization);
	}

	inline bool is_opened() {
		return adi.is_opened();
	}

	inline int poll_completion(uint32_t &num_events, uint32_t timeout_us) {
		return adi.poll_completion(num_events, timeout_us);
	}

	inline int open(const std::string &devpath, int csum_type)
	{
		return adi.open(devpath, csum_type);
	}

	inline int close() {
		return adi.close();
	}

	kv_result aio_submit(KvsPrefetchContext *ctx);

	// Transaction
	kv_result aio_submit(KvsTransContext *txc);

	// Journal
	kv_result write_journal(KvsTransContext *txc);

	// Raw IO operations
	kv_result kv_retrieve(uint8_t space_id, kv_key *key, kv_value *value);
	kv_result kv_retrieve(uint8_t space_id, kv_key *key, bufferlist &bl, int valuesize);
	kv_result kv_retrieve(uint8_t space_id, kv_key *key, kv_value *value, uint64_t offset, size_t length, bufferlist &bl, bool &ispartial);

	kv_result kv_store(uint8_t space_id, kv_key *key, kv_value *value);

	kv_result kv_delete(uint8_t space_id, kv_key *key);

	// Helpers
	kv_result sync_write(const std::tuple<uint8_t, kv_key *, kv_value *> &&pair);
	kv_result sync_read (const std::tuple<uint8_t, kv_key *, kv_value *> &&pair, uint64_t offset, size_t length, bufferlist &bl, bool &ispartial);
	kv_result sync_read (const std::tuple<uint8_t, kv_key *, kv_value *> &&pair, bufferlist &bl);

#ifdef ENABLE_KVSSTORE_IOLOG
	inline void iolog(const std::string &message, kv_key *key, int ret = 0) {
		iolog_impl(message, key, ret);
	}
#else
	inline void iolog(const std::string &message, kv_key *key, int ret = 0) {}
#endif

	inline void iolog_impl(const std::string &message, kv_key *key, int ret = 0);

private:
	kv_result submit_aio_commands(aio_iter begin, aio_iter end, void *priv);
};


#endif /* SRC_KVSSD_KVSSTORE_KADI_H_ */
