/*
 * kadi_util.h
 *
 *  Created on: Jul 25, 2019
 *      Author: ywkang
 */

#ifndef SRC_KVSSD_KADI_UTIL_H_
#define SRC_KVSSD_KADI_UTIL_H_

#include <chrono>
#include <assert.h>
#include <sys/eventfd.h>
#include <sys/select.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <chrono>
#include "kadi_types.h"

class KADI;

class aio_cmd_ctx {
public:
	struct nvme_passthru_kv_cmd cmd;

	std::chrono::high_resolution_clock::time_point t1;

    int index;
    kv_key   *key   = 0;
    kv_value *value = 0;

    void *post_data;
    void (*post_fn)(kv_io_context &result, void *data);

    void call_post_fn(kv_io_context &result) {
        if (post_fn != NULL) post_fn(result, post_data);
    }
};
/*
typedef struct {
    int index;
    kv_key* key = 0;
    kv_value *value = 0;
    void *buf = 0;
    int buflen = 0;
    std::mutex lk;
    void (*post_fn)(kv_io_context &result, void *data);
    void *post_data;

    volatile struct nvme_passthru_kv_cmd cmd;

    void call_post_fn(kv_io_context &result) {
        if (post_fn != NULL) post_fn(result, post_data);
    }
} aio_cmd_ctx;
*/
class cmd_ctx_manager {
	std::mutex cmdctx_lock;
	std::condition_variable cmdctx_cond;
	std::vector<aio_cmd_ctx *>   free_cmdctxs;
	std::map<int, aio_cmd_ctx *> pending_cmdctxs;
public:
	cmd_ctx_manager() {}

	void init(const int qdepth) {
	    for (int i =0  ; i < qdepth; i++) {
	    	aio_cmd_ctx *ctx = (aio_cmd_ctx *)calloc(1, sizeof(aio_cmd_ctx));
	        ctx->index = i;
	        free_cmdctxs.push_back(ctx);
	    }
	}

	void close() {
        free_cmdctxs.clear();
        pending_cmdctxs.clear();
	}

	aio_cmd_ctx* get_cmd_ctx(const kv_cb& cb);


    inline aio_cmd_ctx* get_pending_cmdctx(int reqid) {
        std::unique_lock<std::mutex> lock (cmdctx_lock);
        auto p = pending_cmdctxs.find(reqid);
        if (p == pending_cmdctxs.end()) {
            return 0;
        }
        else {
            aio_cmd_ctx *ctx = p->second;
            pending_cmdctxs.erase(p);
            return ctx;
        }
    }

    void release_cmd_ctx(aio_cmd_ctx *p)
    {
        std::lock_guard<std::mutex> lock (cmdctx_lock);

        pending_cmdctxs.erase(p->index);
        free_cmdctxs.push_back(p);
        cmdctx_cond.notify_one();
    }
};

class ioevent_listener
{
	int EpollFD_dev;
	struct nvme_aioctx aioctx;	// aio context (open/close)
	struct epoll_event watch_events;
	struct epoll_event list_of_events[1];
public:
	int init(int fd);
	void close(int fd);
	int poll(uint32_t timeout_us);
};

class iterbuf_reader {
    void *cct;
    void *buf;
    int bufoffset;
    int byteswritten;

    int numkeys;

public:
    iterbuf_reader(void *c, void *buf_, int length_);

    bool hasnext() { return byteswritten - bufoffset > 0; }

    bool nextkey(void **key, int *length);

    int numkeys_ret(){ return numkeys;}
};


inline std::chrono::high_resolution_clock::time_point kadi_now() {
	return std::chrono::high_resolution_clock::now();
}

inline size_t kadi_timediffus(std::chrono::high_resolution_clock::time_point end, std::chrono::high_resolution_clock::time_point begin) {
	return std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
}


class KVMemPool {

public:
    static kv_key *Alloc_key(int keybuf_size = 256) {
        kv_key *key = (kv_key *)calloc(1, sizeof(kv_key));
        key->key = alloc_memory(get_aligned_size(keybuf_size, 256));
        key->length = keybuf_size;
        return key;
    }

    static kv_value *Alloc_value(int valuesize = 8192, bool needfree = true)  {
        kv_value *value = (kv_value *)calloc(1, sizeof(kv_value));
        if (needfree) {
            value->value  = alloc_memory(get_aligned_size(valuesize, 4096));
        }
        value->length = valuesize;
        value->needfree = (needfree)? 1:0;

        return value;
    }

    static inline int get_aligned_size(const int valuesize, const int align) {
        return ((valuesize + align -1 ) / align) * align;
    }

    static inline void* alloc_memory(const int valuesize) {
        return calloc(1, valuesize);
    }

     static inline void free_memory(void *ptr) {
        free(ptr);
    }

    static void Release_key(kv_key *key) {
        assert(key != 0);
        free((void*)key->key);
        free(key);
    }

    static void Release_value (kv_value *value) {
        assert(value != 0);
        if (value->needfree && value->length > 0)
            free((void*)value->value);
        free(value);
    }
};

void dump_cmd(struct nvme_passthru_kv_cmd *cmd);
void dump_retrieve_cmd(struct nvme_passthru_kv_cmd *cmd);
void dump_delete_cmd(struct nvme_passthru_kv_cmd *cmd);



#endif /* SRC_KVSSD_KADI_UTIL_H_ */
