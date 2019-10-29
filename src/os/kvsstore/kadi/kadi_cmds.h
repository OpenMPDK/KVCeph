/*
 * kadi_cmds.h
 *
 *  Created on: Jul 26, 2019
 *      Author: ywkang
 */

#ifndef SRC_API_KADI_CMDS_H_
#define SRC_API_KADI_CMDS_H_

#include "kadi_types.h"
#include "kadi_helpers.h"

class KADI {
	int fd = -1;		// device file descriptor
	void *cct;			// debug context
    //int space_id;		// key-space id
    unsigned nsid;		// namespace id
    int csum_type = 0;  // checksum type
    const int qdepth = MAX_AIO_EVENTS;		// io queue depth
	//std::mutex aioevent_lock;
	int aioctx_ctxid;


public:
    typedef std::list<std::pair<kv_key *, kv_value *> >::iterator aio_iter;

    KADI(void *c): cct(c),nsid(0), aioctx_ctxid(0) { }
    ~KADI() { close(); }

private:
    // helpers
    cmd_ctx_manager cmd_ctx_mgr;
    ioevent_listener ioevent_mgr;
    kv_result fill_ioresult(const aio_cmd_ctx &ioctx,
    			const struct nvme_aioevent &event, kv_io_context &ioresult);
public:
    int open(const std::string &devpath, int csum_type);
    int close();

    int kv_store_aio(uint8_t space_id, kv_key *key, kv_value *value, const kv_cb& cb);
    int kv_store_sync(uint8_t space_id, kv_key *key, kv_value *value);

    int kv_retrieve_aio(uint8_t space_id, kv_key *key, kv_value *value, const kv_cb& cb);
    int kv_retrieve_sync(uint8_t space_id, kv_key *key, kv_value *value);
    int kv_retrieve_sync(uint8_t space_id, kv_key *key, kv_value *value, const std::function< kv_value*(kv_value *,int) >& kv_realloc);

    int kv_delete_aio(uint8_t space_id, kv_key *key, const kv_cb& cb);
    int kv_delete_sync(uint8_t space_id, kv_key *key);

    int iter_open(kv_iter_context *iter_handle, int space_id);
    int iter_close(kv_iter_context *iter_handle, int space_id);
    int iter_read(kv_iter_context *iter_handle, int space_id);
    int iter_read_aio(int space_id, unsigned char handle, void *buf, uint32_t buflen, const kv_cb& cb);
    int iter_readall(kv_iter_context *iter_ctx, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, int space_id);
    int iter_readall_aio(kv_iter_context *iter_ctx, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, int space_id);

    int batch_submit(kv_batch_context *batch_handle, int space_id);
    int batch_submit_aio(kv_batch_context *batch_handle, int space_id, const kv_cb& cb);

    int poll_completion(uint32_t &num_events, uint32_t timeout_us);
    int get_freespace(uint64_t &bytesused, uint64_t &capacity, double &utilization);

    bool exist(void *key, int length, int spaceid);
    bool is_opened() { return (fd != -1); }
};





#endif /* SRC_API_KADI_CMDS_H_ */
