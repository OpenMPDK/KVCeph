/*
 * kvio.h
 *
 *
 *
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_KVIO_H_
#define SRC_OS_KVSSTORE_KVSSTORE_KVIO_H_

#include "kadi/kadi_cmds.h"
#include "kadi/kadi_types.h"
#include "keyencoder.h"
#include "ondisk_types.h"

const static bool PARTIAL_READ_SUPPORTED = false;
#define USE_BL_AS_READBUFFER

class KADIWrapper
{
	CephContext* cct;

public:
	KADI adi;
	KADIWrapper(void *c): cct((CephContext*)c), adi(cct){}

public:
	// inline functions
	inline kv_result async_write(uint8_t space_id, kv_value *value, const kv_cb& cb, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill)
	{
		return adi.kv_store_aio(space_id, value, cb, fill);
	}

	//const std::function< void (struct nvme_passthru_kv_cmd&)> &fill)
	template<typename Func>
	inline kv_result sync_write(uint8_t space_id, kv_value *value, Func &&fill)	{
		return adi.kv_store_sync(space_id, value, fill);
	}

	inline kv_result sync_write(uint8_t space_id, kv_key *key, kv_value *value)	{
		return adi.kv_store_sync(space_id, key, value);
	}

	inline kv_result sync_delete(uint8_t space_id, kv_key *key)	{
		return adi.kv_delete_sync(space_id, key);
	}

	template<typename Func>
	inline kv_result sync_delete(uint8_t space_id, Func &&fill)	{
		return adi.kv_delete_sync(space_id, fill);
	}

	inline kv_result sync_read(uint8_t space_id, kv_value *value, bool &ispartial, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill) {
		int ret;
		const int   offset = value->offset;
		if (offset == 0 || PARTIAL_READ_SUPPORTED) {
			// directly process the request using ADI
			ret = adi.kv_retrieve_sync(space_id, value, fill);
		}
		else {
			// handles offset reads without FW support
			void *output_buf = value->value;
			int   buf_length = value->length;
			const int toread_bytes = offset + value->length;

			auto mem = make_malloc_unique<char>(toread_bytes);

			// read the first 'toread_bytes'
			value->offset = 0;
			value->length = toread_bytes;
			value->value  = mem.get();

			ret = adi.kv_retrieve_sync(space_id, value, fill);

			if (ret == KV_SUCCESS) {
				// copy from the offset
				const int bytes_to_copy = std::min((int)value->length - offset, buf_length);
				if (bytes_to_copy > 0) {
					memcpy(output_buf, ((char*)value->value + offset), bytes_to_copy);
				}
				ispartial = value->offset != 0 || value->length != value->actual_value_size || value->actual_value_size == 0;
				value->length = bytes_to_copy;
			} else {
				value->length = buf_length;
			}
			value->offset = offset;
			value->value  = output_buf;
		}

		return ret;
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

	// async
	//-----------------

	inline int kv_delete_aio(uint8_t space_id, kv_key *key, const kv_cb& cb) {
		return adi.kv_delete_aio(space_id, key, cb);
	}

	inline int kv_store_aio(uint8_t space_id, kv_key *key, kv_value *value, const kv_cb& cb) {
		return adi.kv_store_aio(space_id, key, value, cb);
	}

	inline int batch_submit_aio(kv_batch_context *batch_handle, int space_id, const kv_cb& cb) {
		return adi.batch_submit_aio(batch_handle, space_id, cb);
	}

	// sync
	//-----------------

	inline kv_result kv_store(uint8_t space_id, kv_key *key, kv_value *value) {
	    if (key == 0 || key->key == 0 || value == 0 || value->value == 0) {
	        ceph_abort_msg("NULL parameters in kv_store");
	    }
	    return adi.kv_store_sync(space_id, key, value);
	}

	inline kv_result kv_delete(uint8_t space_id, kv_key *key) {
	    if (key == 0 || key->key == 0) {
	        ceph_abort_msg("NULL parameters in kv_delete");
	    }

	    int ret = adi.kv_delete_sync(space_id, key);
	    if (ret != 0) {
			return KVS_ERR_IO;
		}
		return KVS_SUCCESS;
	}


public:

	inline kv_result iter_readall(kv_iter_context *iter_ctx, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, int space_id)
	{
		return adi.iter_readall(iter_ctx, buflist, space_id);
	}

	inline kv_result iter_readall_aio(kv_iter_context *iter_ctx, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, int space_id)
	{
		return adi.iter_readall_aio(iter_ctx, buflist, space_id);
	}




	// Raw IO operations
	kv_result kv_retrieve(uint8_t space_id, kv_key *key, kv_value *value);
	kv_result kv_retrieve(uint8_t space_id, kv_key *key, bufferlist &bl, int valuesize);
	kv_result kv_retrieve(uint8_t space_id, kv_key *key, kv_value *value, uint64_t offset, size_t length, bufferlist &bl, bool &ispartial);



	// Helpers

	kv_result sync_write(const std::tuple<uint8_t, kv_key *, kv_value *> &&pair);
	kv_result sync_read (const std::tuple<uint8_t, kv_key *, kv_value *> &&pair, uint64_t offset, size_t length, bufferlist &bl, bool &ispartial);
	kv_result sync_read (const std::tuple<uint8_t, kv_key *, kv_value *> &&pair, bufferlist &bl);


};




#endif /* SRC_OS_KVSSTORE_KVSSTORE_KVIO_H_ */
