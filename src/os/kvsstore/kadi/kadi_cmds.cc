#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <sys/select.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>
#include <math.h>
#include <time.h>
#include <vector>
#include "kadi_cmds.h"


using namespace std;

#undef dout_prefix
#define dout_prefix *_dout << "[kadi] "

#ifndef derr
#define derr std::cerr
#endif

#ifndef dendl
#define dendl std::endl
#endif


constexpr char hex_map[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

inline string printHexKey(const char* in, unsigned length){
     std::string out(length*2, ' ');
     for (unsigned i = 0; i < length; i++){
         out[2*i] = hex_map[(in[i] & 0xF0) >> 4];
         out[2*i+1] = hex_map[(in[i]) & 0x0F];
     }

     return out;
 }


int KADI::open(const std::string &devpath, int csum_type_) {

    int ret = 0;
    this->csum_type = csum_type_;

    this->fd = ::open(devpath.c_str(), O_RDWR);
    if (this->fd < 0) {
    	std::cout << devpath << std::endl;
        derr <<  "can't open a device : " << devpath << dendl;
        return this->fd;
    }


    this->nsid = ioctl(fd, NVME_IOCTL_ID);
    if (this->nsid == (unsigned) -1) {
        derr <<  "can't get an ID" << dendl;
        return -1;
    }

    cmd_ctx_mgr.init(this->qdepth);

    aioctx_ctxid   = ioevent_mgr.init(this->fd);
    if (aioctx_ctxid == -1) return aioctx_ctxid;

    derr << "KVSSD is opened: " << devpath.c_str() << ", ctx id = " << aioctx_ctxid << dendl;
    return ret;
}

int KADI::close() {
    if (this->fd > 0) {
    	ioevent_mgr.close(this->fd);
        ::close(fd);

        cmd_ctx_mgr.close();
        derr << "KV device is closed: fd " << fd << dendl;
        fd = -1;

    }
    return 0;
}

/// Write
/// -----------------------------------------------------------------------

int KADI::kv_store_aio(uint8_t space_id, kv_key *key, kv_value *value, const kv_cb& cb) {
	aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);

	memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    ioctx->cmd.opcode = nvme_cmd_kv_store;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw5 = value->offset;
    ioctx->cmd.key_length = key->length;
    ioctx->cmd.cdw11 = key->length -1;
    ioctx->cmd.data_addr = (__u64)value->value;
    ioctx->cmd.data_length = value->length;
    ioctx->cmd.cdw10 = (value->length >>  2);
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->cmd.reqid = ioctx->index;
    ioctx->key = key;
    ioctx->value = value;

    if (key->length > KVCMD_INLINE_KEY_MAX) {
        ioctx->cmd.key_addr = (__u64)key->key;
    } else {
        memcpy((void*)ioctx->cmd.key, (void*)key->key, key->length);
    }

    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd) < 0) {
    	std::cout << "release" << std::endl;
    	cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }
    return 0;
}

int KADI::kv_store_sync(uint8_t space_id, kv_key *key, kv_value *value) {
    struct nvme_passthru_kv_cmd cmd;
    memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    cmd.opcode = nvme_cmd_kv_store;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw5 = value->offset;
    cmd.key_length = key->length;
    cmd.cdw11 = key->length -1;
    cmd.data_addr = (__u64)value->value;
    cmd.data_length = value->length;
    cmd.cdw10 = (value->length >>  2);


    if (key->length > KVCMD_INLINE_KEY_MAX) {
        cmd.key_addr = (__u64)key->key;
    } else {
        memcpy((void*)cmd.key, (void*)key->key, key->length);
    }

    if (ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd) < 0) {
        return -1;
    }
#if 0
    derr << __func__ << " (store) key = " << printHexKey((const char*)key->key, key->length) 
                     << ", value->length = " << value->length  << dendl;
#endif
    return 0;
}

/// Read
/// -----------------------------------------------------------------------

int KADI::kv_retrieve_aio(uint8_t space_id, kv_key *key, kv_value *value, const kv_cb& cb) {
	aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);
	memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    ioctx->cmd.opcode = nvme_cmd_kv_retrieve;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw4 = 0;
    ioctx->cmd.cdw5 = value->offset;
    ioctx->cmd.data_addr = (__u64)value->value;
    ioctx->cmd.data_length = value->length;
    ioctx->cmd.key_length = key->length;
    ioctx->cmd.reqid = ioctx->index;
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->key = key;
	ioctx->value = value;
    if (key->length <= KVCMD_INLINE_KEY_MAX) {
        memcpy((void*)ioctx->cmd.key, (void*)key->key, key->length);
    } else {
        ioctx->cmd.key_addr = (__u64)key->key;
    }
    ioctx->t1 = std::chrono::high_resolution_clock::now();
    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd) < 0) {
    	cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

    
    return 0;
}

int KADI::kv_retrieve_sync(uint8_t space_id, kv_key *key, kv_value *value) {
    struct nvme_passthru_kv_cmd cmd;
    memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw4 = 0;
    cmd.cdw5 = value->offset;
    cmd.data_addr = (__u64)value->value;
    cmd.data_length = value->length;
    cmd.key_length = key->length;

    if (key->length <= KVCMD_INLINE_KEY_MAX) {
        memcpy((void*)cmd.key, (void*)key->key, key->length);
    } else {
        cmd.key_addr = (__u64)key->key;
    }
    

    if (ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd) == 0) {
        value->actual_value_size = cmd.result;
        value->length = std::min(cmd.result, value->length);
         return 0;
    }
	return -1;
}

int KADI::kv_retrieve_sync(uint8_t space_id, kv_key *key, kv_value *value,
							 const std::function< kv_value*(kv_value *,int) >& kv_realloc) {
    struct nvme_passthru_kv_cmd cmd;
    memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    
    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw4 = 0;
    cmd.cdw5 = value->offset;
    cmd.data_addr = (__u64)value->value;
    cmd.data_length = value->length;
    cmd.key_length = key->length;

    if (key->length <= KVCMD_INLINE_KEY_MAX) {
        memcpy((void*)cmd.key, (void*)key->key, key->length);
    } else {
        cmd.key_addr = (__u64)key->key;
    }

retry:
	int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
  
    if (ret == 0) {
        value->actual_value_size = cmd.result;
        value->length = std::min(cmd.result, value->length);

        if (value->length != value->actual_value_size) {
        	value = kv_realloc(value, value->actual_value_size);
            cmd.data_addr = (__u64)value->value;
            cmd.data_length = value->length;
            goto retry;
        }
    }
#if 0
   derr << " kv_retrieve_sync : key = " << printHexKey((const char*)key->key, key->length) 
         << ", value offset = " << value->offset << ", valuelength = " << cmd.result
         << ", keylength = " << (int)key->length << ", ret = " << ret << dendl;
#endif   
  return ret;
}

/// Delete
/// -----------------------------------------------------------------------

int KADI::kv_delete_sync(uint8_t space_id, kv_key *key) {
    struct nvme_passthru_kv_cmd cmd;
    memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    cmd.opcode = nvme_cmd_kv_delete;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw4 = 1;
    cmd.key_length = key->length;

    if (key->length <= KVCMD_INLINE_KEY_MAX) {
        memcpy((void*)cmd.key, (void*)key->key, key->length);
    } else {
        cmd.key_addr = (__u64)key->key;
    }

    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &cmd) < 0) {
        return -1;
    }

    return 0;
}

int KADI::kv_delete_aio(uint8_t space_id, kv_key *key, const kv_cb& cb) {
    aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);

    memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    ioctx->cmd.opcode = nvme_cmd_kv_delete;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw4 = 1;
    if (key->length <= KVCMD_INLINE_KEY_MAX) {
        memcpy((void*)ioctx->cmd.key, (void*)key->key, key->length);
    } else {
        ioctx->cmd.key_addr = (__u64)key->key;
    }
    ioctx->cmd.key_length = key->length;
    ioctx->cmd.reqid = ioctx->index;
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->key = key;
    ioctx->value = 0;
    ioctx->t1 = std::chrono::high_resolution_clock::now();

    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd) < 0) {
        cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

    return 0;
}

/// Iterator
/// -----------------------------------------------------------------------

int KADI::iter_open(kv_iter_context *iter_handle, int space_id)
{
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    cmd.opcode = nvme_cmd_kv_iter_req;
    cmd.cdw3 = space_id;
    cmd.nsid = nsid;
    cmd.cdw4 = (ITER_OPTION_OPEN | ITER_OPTION_KEY_ONLY);
    cmd.cdw12 = iter_handle->prefix;
    cmd.cdw13 = iter_handle->bitmask;

    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
    if (ret < 0) {
        return -1;
    }

    iter_handle->handle = cmd.result & 0xff;
    iter_handle->end    = false;

    return cmd.status;
}

int KADI::iter_close(kv_iter_context *iter_handle, int space_id) {
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_iter_req;
    cmd.cdw3 = space_id;
    cmd.nsid = nsid;
    cmd.cdw4 = ITER_OPTION_CLOSE;
    cmd.cdw5 = iter_handle->handle;

    if (ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd) < 0) {
        return -1;
    }
    return cmd.status;
}


int KADI::iter_read(kv_iter_context *iter_handle, int space_id) {
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_iter_read;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw5 = iter_handle->handle;
    cmd.data_addr = (__u64)iter_handle->buf.get();
    cmd.data_length = iter_handle->buflen;

    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
    if (ret < 0) { return -1; }

    iter_handle->byteswritten = cmd.result & 0xffff;
    if (iter_handle->byteswritten > iter_handle->buflen) {
    	derr <<" # of read bytes > buffer length" << dendl;
    	return -1;
    }

    if (cmd.status == 0x0393) { /* scan finished, but data is valid */
        iter_handle->end = true;
    }
    else
        iter_handle->end = false;

    return cmd.status;
}

int KADI::iter_read_aio(int space_id, unsigned char handle, void *buf, uint32_t buflen, const kv_cb& cb) {
    aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);

    memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    ioctx->cmd.opcode = nvme_cmd_kv_iter_read;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw5 = handle;
    ioctx->cmd.data_addr = (__u64)buf;
    ioctx->cmd.data_length = buflen;
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->cmd.reqid = ioctx->index;
    ioctx->key   = 0;
    ioctx->value = 0;

    int ret = ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd);
    if (ret < 0) {
        derr << "fail to send aio command ret = " << ret << dendl;
        return -1;
    }

    return 0;
}


/// A callback function for an asynchronous iterator
/// ---------------------------------------------------

void iter_callback(kv_io_context &op, void* private_data)
{
	kv_iter_context *ctx = (kv_iter_context *)private_data;

    ctx->end = op.hiter.end;
	ctx->retcode = op.retcode;
	ctx->byteswritten = op.hiter.byteswritten;

	if (ctx->parent)
    	((KvsIterAioContext*)ctx->parent)->fire_event(ctx);
}

int KADI::iter_readall_aio(kv_iter_context *iter_ctx, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, int space_id)
{
	int r = iter_open(iter_ctx, space_id);
	if (r != 0) return r;

	KvsIterAioContext iter_aioctx;

	iter_aioctx.init(8, iter_ctx->handle);

	int sent = 0;

	bool finished = false;
	do {
		const size_t max = iter_aioctx.free_iter_contexts.size();

		if (!finished) {
			for (size_t i =0 ; i < max; i++) {
				kv_iter_context *sub_iter_ctx = iter_aioctx.get_free_context();
				if (sub_iter_ctx == 0) break;

				sub_iter_ctx->buf = make_malloc_unique<char>(ITER_BUFSIZE);

				r = iter_read_aio(space_id, iter_ctx->handle,
							sub_iter_ctx->buf.get(), ITER_BUFSIZE,
							{ iter_callback, sub_iter_ctx } );

				sent++;

				if (r != 0) { goto error_exit; }
			}
		}

		if (sent == 0) break;

		std::vector<kv_iter_context *> ctxs;
		iter_aioctx.get_events(ctxs);

		sent -= ctxs.size();

		for (kv_iter_context *sub_iter_ctx: ctxs) {

			if (sub_iter_ctx->byteswritten > 0) {
				buflist.push_back(std::make_pair(
						std::move(sub_iter_ctx->buf), sub_iter_ctx->byteswritten));
			}

			finished |= sub_iter_ctx->end;
			iter_aioctx.return_free_context(sub_iter_ctx);
		}

	} while (true);

	r = iter_close(iter_ctx, space_id);
	return r;

error_exit:
	buflist.clear();
	if (iter_ctx) iter_close(iter_ctx, space_id);
	return r;

}

kv_result KADI::iter_readall(kv_iter_context *iter_ctx, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, int space_id)
{
    kv_result r = iter_open(iter_ctx, space_id);
    if (r != 0) return r;

    const auto start = kadi_now();

    while (!iter_ctx->end) {
        iter_ctx->byteswritten = 0;
        iter_ctx->buf = make_malloc_unique<char>(ITER_BUFSIZE); //= malloc(iter_ctx->buflen);
        int ret = iter_read(iter_ctx, space_id);
        if (ret) {

            if (ret == 0x311){
            	goto exit;
            } else if (ret == 0x390) {
            	goto exit;
            } else if (ret == 0x308 || ret == 0x394 ) {
            	goto exit;
            }
        }

        if (iter_ctx->byteswritten > 0) {
            buflist.push_back(std::make_pair(std::move(iter_ctx->buf), iter_ctx->byteswritten));
        }

    }

    iter_ctx->elapsed_us = kadi_timediffus(kadi_now(), start);

exit:
    r = iter_close(iter_ctx, space_id);
    return r;
}

/// Batch
/// -----------------------------------------------------------------------

int KADI::batch_submit(kv_batch_context *batch_handle, int space_id) {
	int ret = 0;

	for (auto it = batch_handle->begin(); it != batch_handle->end(); it++) {
		KvBatchCmd *batchcmd = *it;
		struct nvme_passthru_kv_cmd cmd;
	    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
	    const int payload_size = batchcmd->payload_size();
	    cmd.opcode = nvme_cmd_kv_batch;  //DW0
	    cmd.nsid = nsid;                 //DW1
	    cmd.data_addr = (__u64)batchcmd->get_payload();  //DPTR
	    cmd.data_length = payload_size;
	    cmd.cdw10 = (payload_size >> 2); // Payload size in dword
	    cmd.cdw11 = ((0x00 << 8) | batchcmd->get_cmdcnts()) & 0xFFFF;
		ret = ioctl(this->fd, NVME_IOCTL_IO_KV_CMD, &cmd);
	}
	return ret;
}

int KADI::batch_submit_aio(kv_batch_context *batch_handle, int space_id, const kv_cb& cb) {
	int ret = 0;

	for (auto it = batch_handle->begin(); it != batch_handle->end(); it++) {
		KvBatchCmd *batchcmd = *it;
	    aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);

	    memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
	    const int payload_size = batchcmd->payload_size();
	    ioctx->cmd.opcode = nvme_cmd_kv_batch;  //DW0
	    ioctx->cmd.nsid = nsid;                 //DW1
	    ioctx->cmd.data_addr = (__u64)batchcmd->get_payload();  //DPTR
	    ioctx->cmd.data_length = payload_size;
	    ioctx->cmd.cdw10 = (payload_size >> 2); // Payload size in dword
	    ioctx->cmd.cdw11 = ((0x00 << 8) | batchcmd->get_cmdcnts()) & 0xFFFF;
	    ioctx->cmd.ctxid = aioctx_ctxid;
		ioctx->cmd.reqid = ioctx->index;
		ioctx->key   = 0;
		ioctx->value = 0;
	    ioctx->t1 = std::chrono::high_resolution_clock::now();

	    ret = ioctl(this->fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd);
	}
	return ret;
}


/// Misc
/// -----------------------------------------------------------------------

int KADI::get_freespace(uint64_t &bytesused, uint64_t &capacity, double &utilization)
{
	malloc_unique_ptr<char> data_ptr = make_malloc_unique<char>(4096);
    struct nvme_passthru_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_cmd));
    cmd.opcode = nvme_cmd_admin_identify;
    cmd.nsid = nsid;
    cmd.addr = (__u64)data_ptr.get();
    cmd.data_len = 4096;
    cmd.cdw10 = 0;

    if (ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd) < 0)
    {
        return -1;
    }

    char* data = data_ptr.get();

    const __u64 namespace_size = *((__u64 *)data);
    const __u64 namespace_utilization = *((__u64 *)&data[16]);
    capacity = namespace_size * 512;
    bytesused = namespace_utilization * 512;
    utilization = (1.0 * namespace_utilization) / namespace_size;
    return 0;
}

bool KADI::exist(void *key, int length, int spaceid)
{
	struct nvme_passthru_kv_cmd cmd;
	memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
	cmd.opcode = nvme_cmd_kv_exist;
	cmd.nsid = nsid;
	cmd.cdw3 = spaceid;
	cmd.key_length = length;
	if (length > KVCMD_INLINE_KEY_MAX) {
			cmd.key_addr = (__u64)key;
	} else {
			memcpy(cmd.key, key, length);
	}

	int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
	return (ret == 0);
}

int KADI::poll_completion(uint32_t &num_events, uint32_t timeout_us) {

	int num_finished_ios = std::min(ioevent_mgr.poll(timeout_us), MAX_AIO_EVENTS);
	int ret = 0;
	int events = 0;
    while (num_finished_ios) {
        struct nvme_aioevents aioevents;
        aioevents.nr = num_finished_ios;
        aioevents.ctxid = aioctx_ctxid;
        
        if (ioctl(fd, NVME_IOCTL_GET_AIOEVENT, &aioevents) < 0) {
            fprintf(stderr, "fail to read IOEVETS \n");
            ret = -1; goto exit;
        }

        for (int i = 0; i < aioevents.nr; i++) {
            const struct nvme_aioevent &event  = aioevents.events[i];
            aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_pending_cmdctx(event.reqid);
            if (ioctx == 0) {
            	ret = KVS_ERR_INDEX; goto exit;
            }

			kv_io_context ioresult;
			fill_ioresult(*ioctx, event, ioresult);
			ioctx->call_post_fn(ioresult);
			cmd_ctx_mgr.release_cmd_ctx(ioctx);
			events++;
        }

        num_finished_ios -= aioevents.nr;
    }
exit:
    num_events = events;
    return ret;
}

kv_result KADI::fill_ioresult(const aio_cmd_ctx &ioctx, const struct nvme_aioevent &event,
                              kv_io_context &ioresult)
{
    ioresult.opcode = ioctx.cmd.opcode;
    ioresult.retcode = event.status;
    ioresult.key = ioctx.key;
    ioresult.value = ioctx.value;
    ioresult.latency = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - ioctx.t1).count();

    // exceptions
    switch (ioresult.retcode)
    {
    case 0x393:
        if (ioresult.opcode == nvme_cmd_kv_iter_read)
        {
            ioresult.hiter.buf = (void*) ioctx.cmd.data_addr;
            ioresult.hiter.buflength = (event.result & 0xffff);
            ioresult.hiter.end = true;
            ioresult.retcode = 0;
            return 0;
        }
        break;
    case 0x3A1: // partial commands of batch success
        if (ioresult.opcode == nvme_cmd_kv_batch) {


            for (uint32_t i = 0; i < 8; i++) {
            	ioresult.batch_results[i] =  (event.result >> (i * 4)) & 0xF;
            }
        }
        break;
    }

    if (ioresult.retcode != 0) {
        return 0;
    }

    switch (ioresult.opcode)
    {

    case nvme_cmd_kv_retrieve:

        if (ioctx.value)
        {
            ioresult.value->actual_value_size = event.result;
            ioresult.value->length = std::min(event.result, ioctx.value->length);
        }
        break;

    case nvme_cmd_kv_iter_req:
        if ((ioctx.cmd.cdw4 & ITER_OPTION_OPEN) != 0)
        {
            ioresult.hiter.id = (event.result & 0x000000FF);
            ioresult.hiter.end = false;
        }
        break;

    case nvme_cmd_kv_iter_read:
        ioresult.hiter.buf = (void*) ioctx.cmd.data_addr;

        ioresult.hiter.byteswritten = (event.result & 0xffff);

        ioresult.hiter.end = false;

        break;
    case nvme_cmd_kv_batch:
        {
            memset(ioresult.batch_results, 0, sizeof ioresult.batch_results); // set all results as success
        }
        break;
    };

    return 0;
}


/// KvsIterAioContext
/// -----------------------------------------------------------------------

KvsIterAioContext::KvsIterAioContext()
{

}
KvsIterAioContext::~KvsIterAioContext() {
	for (auto p: free_iter_contexts) {
		delete p;
	}
}

void KvsIterAioContext::init(int qdepth, unsigned char handle)
{
	for (int i = 0;  i < qdepth; i++) {
		free_iter_contexts.push_back(new kv_iter_context(i, handle, this));
	}
}

kv_iter_context* KvsIterAioContext::get_free_context()
{
	//std::lock_guard<std::mutex> l(lock);
	if (free_iter_contexts.size() == 0) {
		return nullptr;
	}
	kv_iter_context* ptr = free_iter_contexts.back();
	free_iter_contexts.pop_back();
	return ptr;
}

void KvsIterAioContext::return_free_context(kv_iter_context* ctx)
{
	ctx->byteswritten =0;
	if (!ctx->end) {
		free_iter_contexts.push_back(ctx);
	}
	else {
		// if it is done, do not return to the freelist.
		delete ctx;
	}
}

void KvsIterAioContext::fire_event(kv_iter_context *ctx){
	std::lock_guard<std::mutex> l(ready_lock);
	ready_iter_contexts.push_back(ctx);
	ready_cond.notify_all();
}

void KvsIterAioContext::get_events(std::vector<kv_iter_context *> &ctxs) {
	std::unique_lock<std::mutex> l(ready_lock);
	while (ready_iter_contexts.size() == 0){
		ready_cond.wait(l);
	}
	ctxs = std::move(ready_iter_contexts);
}

/// kv_batch_context
/// -----------------------------------------------------------------------

bool KvBatchCmd::add_store_cmd(int nsid, int option, const char *key, uint8_t key_length, const char *value, uint32_t value_length)
{
	if (isfull()) return false;

	const int aligned_keylength = align_64(key_length);
	const int aligned_vallength = align_64(value_length);

	batch_cmd_head* batch_head = (batch_cmd_head*)payload;
	batch_head->attr[subcmd_index].opcode = nvme_cmd_kv_store;
	batch_head->attr[subcmd_index].keySize = key_length;
	batch_head->attr[subcmd_index].valuseSize = value_length;
	batch_head->attr[subcmd_index].nsid = nsid;
	batch_head->attr[subcmd_index].option = option;

	char *body = payload + sizeof(batch_cmd_head);
	subcmd_offset += SUBCMD_HEAD_SIZE;
	memcpy(body + subcmd_offset, key, key_length); subcmd_offset += aligned_keylength;
	memcpy(body + subcmd_offset, value, value_length); subcmd_offset += aligned_vallength;

	subcmd_index++;
	return true;
}


bool KvBatchCmd::add_store_cmd(int nsid, int option, const char *value, uint32_t value_length, const std::function< uint8_t (char*) > &construct_key)
{
	if (isfull()) return false;

	char *body = payload + sizeof(batch_cmd_head);

	subcmd_offset += SUBCMD_HEAD_SIZE;
	const uint8_t key_length = construct_key(body + subcmd_offset);
	subcmd_offset += align_64(key_length);

	memcpy(body + subcmd_offset, value, value_length);
	subcmd_offset += align_64(value_length);


	batch_cmd_head* batch_head = (batch_cmd_head*)payload;
	batch_head->attr[subcmd_index].opcode = nvme_cmd_kv_store;
	batch_head->attr[subcmd_index].keySize = key_length;
	batch_head->attr[subcmd_index].valuseSize = value_length;
	batch_head->attr[subcmd_index].nsid = nsid;
	batch_head->attr[subcmd_index].option = option;

	//std::cerr << "add store cmd : start = " << subcmd_offset << std::endl;
	//std::cerr << "add store cmd : aligned key    = " << align_64(key_length) << std::endl;
	//std::cerr << "add store cmd : value  length  = " << value_length << std::endl;
	//std::cerr << "add store cmd : aligned value  length  = " << ((511 - 1) / 64 + 1)*64 << std::endl;
	//std::cerr << "add store cmd : aligned value  = " << align_64(value_length) << std::endl;
	//std::cerr << "add store cmd : offset = " << subcmd_offset << std::endl;

	subcmd_index++;
	return true;
}

std::string KvBatchCmd::dump() {
	batch_cmd_head* batch_head = (batch_cmd_head*)payload;
	std::stringstream ss;

	ss << "payload size = " << payload_size() << std::endl;

	const char *subcmds = payload + sizeof(batch_cmd_head);
	for (int i = 0 ; i < subcmd_index; i++) {
		ss << "subcmd_index = " << i << std::endl;
		ss<< "opcode  = " << (int)batch_head->attr[i].opcode << std::endl;
		ss<< "keysize = " << (int)batch_head->attr[i].keySize << std::endl;
		ss<< "valsize = " << (int)batch_head->attr[i].valuseSize << std::endl;
		ss<< "nsid    = " << (int)batch_head->attr[i].nsid << std::endl;
		ss<< "option  = " << (int)batch_head->attr[i].option << std::endl;

		ss << "key     = " << print_key(subcmds + (192 * subcmd_index) + 64,(int)batch_head->attr[subcmd_index].keySize) << std::endl;
		//std::cout << "value   = " << std::string(subcmds + (192 * subcmd_index) + 128,(int)batch_head->attr[subcmd_index].valuseSize) << std::endl;
	}
	return ss.str();
}

int kv_batch_context::batch_store(int nsid, int option, const void *key, uint8_t key_length, const void *value, uint32_t value_length)
{
	if (batch == 0) batch = new KvBatchCmd();
	do {
		if (!batch->add_store_cmd(nsid, option, (const char*)key, key_length, (const char*)value, value_length)) {
			batchcmds.push_back(batch);
			batch = new KvBatchCmd;
		} else {
			break;
		}
	} while(true);

	return 0;
}

int kv_batch_context::batch_store(int nsid, int option, const void *value, uint32_t value_length, const std::function< uint8_t (char*) > &construct_key)
{
	if (batch == 0) batch = new KvBatchCmd();
	do {
		if (!batch->add_store_cmd(nsid, option, (const char*)value, value_length, construct_key)) {
			batchcmds.push_back(batch);
			batch = new KvBatchCmd;
		} else {
			break;
		}
	} while(true);

	return 0;
}

