/*
 * kadi_types.h
 *
 *  Created on: Jul 25, 2019
 *      Author: ywkang
 */

#ifndef SRC_KVSSD_KADI_TYPES_H_
#define SRC_KVSSD_KADI_TYPES_H_

#include "linux_nvme_ioctl.h"
#include <pthread.h>
#include <map>
#include <vector>
#include <mutex>
#include <memory>
#include <list>
#include <memory>
#include <stdbool.h>
#include <condition_variable>
#include <sys/eventfd.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sstream>
#include <memory.h>
#include <functional>
#include <stdint.h>
#include <unordered_map>
#include <iostream>

#define MAX_SUB_CMD 8
#define MAX_SUB_VALUESIZE 8192
#define KVCMD_INLINE_KEY_MAX	(16)
#define KVCMD_MAX_KEY_SIZE		(255)
#define KVCMD_MIN_KEY_SIZE		(255)
//#define DUMP_ISSUE_CMD
#define KV_SPACE
//#define ITER_CLEANUP
//#define OLD_KV_FORMAT
#define ITER_BUFSIZE 32768


enum nvme_kv_batch_sub_result {
    KV_BATCH_SUB_SUCCESS = 0x00,
    KV_BATCH_SUB_INVALID_VAL_SIZE = 0x01,
    KV_BATCH_SUB_INVALID_KEY_SIZE = 0x02,
    KV_BATCH_SUB_UNSUPPORT_OPCODE = 0x03,
    KV_BATCH_SUB_INVALID_OPTION = 0x04,
    KV_BATCH_SUB_INVALID_KSID = 0x05,
    KV_BATCH_SUB_NO_EXIST_KEY = 0x06,
    KV_BATCH_SUB_UNRECOVERY_ERR = 0x07,
    KV_BATCH_SUB_CAPACITY_EXCEEDED = 0x08,
    KV_BATCH_SUB_IDEMPOTENT_STORE_ERR = 0x09,
    KV_BATCH_SUB_UPDATE_STORE_ERR = 0x0A,
};


typedef uint8_t kv_key_t;
typedef uint32_t kv_value_t;
typedef int kv_result;

enum nvme_kv_store_option {
    STORE_OPTION_NOTHING = 0,
    STORE_OPTION_COMP = 1,
    STORE_OPTION_IDEMPOTENT = 2,
    STORE_OPTION_BGCOMP = 4
};

enum nvme_kv_retrieve_option {
    RETRIEVE_OPTION_NOTHING = 0,
    RETRIEVE_OPTION_DECOMP = 1,
    RETRIEVE_OPTION_ONLY_VALSIZE = 2
};

enum nvme_kv_delete_option {
    DELETE_OPTION_NOTHING = 0,
    DELETE_OPTION_CHECK_KEY_EXIST = 1
};

enum nvme_kv_iter_req_option {
    ITER_OPTION_NOTHING = 0x0,
    ITER_OPTION_OPEN = 0x01,
    ITER_OPTION_CLOSE = 0x02,
    ITER_OPTION_KEY_ONLY = 0x04,
    ITER_OPTION_KEY_VALUE = 0x08,
    ITER_OPTION_DEL_KEY_VALUE = 0x10,
	ITER_OPTION_LOG_KEY_SPACE = 0x20,
};

enum nvme_kv_opcode {
    nvme_cmd_kv_store	= 0x81,
	nvme_cmd_admin_identify	= 0x06,
    nvme_cmd_kv_append	= 0x83,
    nvme_cmd_kv_retrieve	= 0x90,
    nvme_cmd_kv_delete	= 0xA1,
    nvme_cmd_kv_iter_req	= 0xB1,
    nvme_cmd_kv_iter_read	= 0xB2,
    nvme_cmd_kv_exist	= 0xB3,
    nvme_cmd_kv_capacity = 0x06,
    nvme_cmd_kv_batch = 0x85
};

enum {
    KV_SUCCESS = 0,
    KV_ERR_KEY_NOT_EXIST = 0x310
};

template<typename T>
using malloc_unique_ptr = typename std::unique_ptr<T, decltype(free)*>;

template<typename T>
inline malloc_unique_ptr<T> make_malloc_unique(int sz) {
	if (sz == 0) {
		return malloc_unique_ptr<T>{ nullptr, free };
	}
	return malloc_unique_ptr<T>{ reinterpret_cast<T*>(malloc( sizeof(T) * sz)), free };
}

template<typename T>
inline malloc_unique_ptr<T> make_malloc_unique_ptr(void *ptr) {
	if (ptr == nullptr) {
		return malloc_unique_ptr<T>{ nullptr, free };
	}
	return malloc_unique_ptr<T>{ reinterpret_cast<T*>(ptr), free };
}

typedef struct {
    void *key;        ///< a pointer to a key
    kv_key_t length;  ///< key length in byte unit, based on KV_MAX_KEY_LENGHT
} kv_key;

typedef struct {
    void *value;                 ///< buffer address for value
    kv_value_t length;           ///< value buffer size in byte unit for input and the retuned value length for output
    kv_value_t actual_value_size;
    kv_value_t offset;           ///< offset for value
    int needfree;
} kv_value;
/**
  kv_sub_command
  This structure defines information for kv_store_batch() and kv_retrieve_batch() that package mulitiple operations into a batch command.
  The operation can be one of the following operations: store, retrieve, exist, and delete.
 */
typedef struct {
  uint8_t opcode;               ///< command operation opcode
  kv_key *key;                 ///< key data structure
  kv_value *value;             ///< value data structure, used for store and retrieve command
  union {
    int store_opt;  ///< store command option, \see kv_store
    int delete_opt;   ///< delete command option, \see kv_delete
    int retrieve_op;  ///< retrieve command option, \see kv_retrieve
  };
  uint8_t exist_status;         ///< output key exist result, used for exist command in batch retrieve
} kv_sub_command;
/*
typedef struct {
    kv_batch_option option;
    uint32_t cmd_cnt;
    kv_sub_command *cmds;
    kv_result *res_buf;
    uint32_t res_size;
} op_batch_struct_t;
*/
typedef struct {
    int opcode;
    int retcode;

    kv_key key;
    kv_value value;

    struct {
        unsigned char handle;
        unsigned int prefix;
        unsigned int bitmask;
        void *buf;
        int buflen;
        int byteswritten;
        int buflength;
        int bufoffset;
        bool end;
        int id;
    } hiter;

    kv_result batch_results[8];
    uint64_t latency;
	uint64_t bytes_transferred;
} kv_io_context;



typedef struct {
    void (*post_fn)(kv_io_context &op, void *post_data);   ///< asynchronous notification callback (valid only for async I/O)
    void *private_data;       ///< private data address which can be used in callback (valid only for async I/O)
} kv_cb;


struct sub_cmd_attribute
{
  __u8	opcode;          // DW0  support only 0x81 and 0xA0
  __u8	keySize;         // DW0 [15:08] Keys size
  __u8	reservedDw0[2];  // DW0 [31:16] Reserved
  __u32	valuseSize;      // DW1  Value size
  __u8	option;          // DW2 [07:00] Option
  __u8	nsid;            // DW2 [15:08] Key space ID
  __u8	reservedDw2[2];  // DW2 [31:16] Reserved
  __u32	NoUsed;          // DW3
};

struct batch_cmd_head {
  struct sub_cmd_attribute attr[MAX_SUB_CMD];
  __u8                     reserved[384];
};

struct payload_format {
  struct batch_cmd_head          batch_head;
  __u8                           sub_payload[0];
};
enum nvme_kv_invalid_opcode {
  nvme_cmd_kv_invalid_opcode = 0x11,
};
enum nvme_kv_invalid_option {
  nvme_cmd_kv_invalid_option = 0x5,
};


class KvBatchCmd {
public:
	char *payload;
	int subcmd_index;
	int subcmd_offset;
	const int MAX_PAYLOAD_SIZE = 128*1024;
	const int MAX_SUBCOMMANDS  = 8;
	const int SUBCMD_HEAD_SIZE = 64;
public:
	KvBatchCmd(): subcmd_index(0), subcmd_offset(0) {
		payload = (char*)malloc(MAX_PAYLOAD_SIZE);
		memset(payload, 0, MAX_PAYLOAD_SIZE);
	}

	~KvBatchCmd() { free(payload);	}

	inline bool isempty() { return subcmd_index == 0;	}
	inline bool isfull()  { return subcmd_index == MAX_SUBCOMMANDS;	}

	inline int align_64(uint32_t length) { return ((length - 1) / 64 + 1)*64;   }
	inline int get_cmdcnts() { return subcmd_index; }
	inline char *get_payload() { return payload; }
	inline int payload_size() { return subcmd_offset + sizeof(batch_cmd_head); }

	bool add_store_cmd(int nsid, int option, const char *key, uint8_t key_length, const char *value, uint32_t value_length);
	bool add_store_cmd(int nsid, int option, const char *value, uint32_t value_length, const std::function< uint8_t (char*) > &construct_key);
	bool add_store_cmd(int nsid, int option,
			const std::function< uint8_t (char*) > &construct_key,
			const std::function< uint32_t (char*) > &construct_value);
	std::string dump();
};

class kv_batch_context
{
	KvBatchCmd *batch;
	std::vector<KvBatchCmd *> batchcmds;
public:
	kv_batch_context(): batch(0) {}
	~kv_batch_context() {
		for (auto it = begin(); it != end(); it++) {
			KvBatchCmd *cmd = *it;
			delete cmd;
		}
	}
	size_t size() {
		size_t s = batchcmds.size();
		if (batch != 0) {
			s += 1;
		}
		return s;
	}
	inline std::vector<KvBatchCmd *>::iterator begin() {
		if (batch != 0) {
			batchcmds.push_back(batch);
			batch = 0;
		}
		return batchcmds.begin();
	}

	inline std::vector<KvBatchCmd *>::iterator end() {
		return batchcmds.end();
	}
	std::string current_offsets() {
		std::stringstream ss;
		if (batch != 0) {
			ss << "active batch: offset = " << batch->subcmd_offset << std::endl;
		}
		for (KvBatchCmd *cmd : batchcmds) {
			ss << "stored batch: offset = " << cmd->subcmd_offset << std::endl;
		}
		return ss.str();
	}

	int batch_store(int nsid, int option, const void *key, uint8_t key_length, const void *value, uint32_t value_length);
	int batch_store(int nsid, int option, const void *value, uint32_t value_length, const std::function< uint8_t (char*) > &construct_key);

	//KeyFunc const std::function< uint8_t (char*) > ValueFunc const std::function< uint32_t (char*) >
	template<typename KeyFunc, typename ValueFunc>
	int batch_store(int nsid, int option, KeyFunc &&construct_key, ValueFunc &&construct_value)
	{
		if (batch == 0) batch = new KvBatchCmd();
		do {
			if (!batch->add_store_cmd(nsid, option, construct_key, construct_value)) {
				batchcmds.push_back(batch);
				batch = new KvBatchCmd;
			} else {
				break;
			}
		} while(true);

		return 0;
	}

};

class kv_iter_context
{
public:
    unsigned char handle;
    unsigned int prefix;
    unsigned int bitmask;

    char *buf;
    //void *buf2;
    int buflen;
    int byteswritten;
    int buflength;
    int bufoffset;
    bool end;
    int id;
    void *parent;
    int retcode;
    size_t elapsed_us;
    uint8_t spaceid;
    kv_iter_context():
    	kv_iter_context(0, 0, 0)
    {
    }

    // for sub iter contexts
    kv_iter_context(int id_,  unsigned char h, void *parent_):
    	handle(h), prefix(0), bitmask(0), buf(0), buflen(ITER_BUFSIZE), byteswritten(0), buflength(0),
		bufoffset(0), end(false), id (id_), parent(parent_), retcode(0), elapsed_us(0), spaceid(0)
    {

    }


};

template<typename T>
class KvsAioContext {
	std::mutex ready_lock;
	std::condition_variable ready_cond;
	typedef std::vector<T *> aioctx_type;

public:
	aioctx_type free_iter_contexts;
	aioctx_type ready_iter_contexts;

	KvsAioContext() {}
	~KvsAioContext() {
		for (auto p: free_iter_contexts) {
			delete p;
		}
	}
	void init(int qdepth, const std::function<T*(int)> &creator)
	{
        free_iter_contexts.clear();
        free_iter_contexts.reserve(qdepth);
		for (int i = 0;  i < qdepth; i++) {
		    T *inst = creator(i);
			free_iter_contexts.push_back( inst );
		}
	}

	T* get_free_context()
	{
		if (free_iter_contexts.size() == 0) {
			return nullptr;
		}
		T* ptr = free_iter_contexts.back();
		free_iter_contexts.pop_back();
		return ptr;
	}

	void return_free_context(T* ctx)
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

	void fire_event(T *ctx){
		std::lock_guard<std::mutex> l(ready_lock);
		ready_iter_contexts.push_back(ctx);
		ready_cond.notify_all();
	}

	bool get_events(std::vector<T *> &ctxs) {
		std::unique_lock<std::mutex> l(ready_lock);
		if (ready_iter_contexts.size() == 0 ){
            return false;
		}
		ctxs.swap(ready_iter_contexts);
		return true;
	}
};

class kv_read_context
{
public:
	int id;
    int retcode;
    uint8_t spaceid;
	int buflength;
    int byteswritten;
    void *parent;
    bool end;
    int groupid;
    uint64_t sequence;
    char* buf;

    kv_read_context(int id_, int spaceid_,  int buflength_, void *parent_):
    	id(id_), retcode(0), spaceid(spaceid_), buflength(buflength_), byteswritten(0), parent(parent_),
		end(false), groupid(0), sequence(0), buf(0)
	{}
};

struct __attribute__((packed)) oplog_key
{
	char prefix[4];
	uint64_t sequenceid;
	uint32_t groupid;
};

typedef std::list<std::pair<char*, int> > buflist_t;
typedef std::unordered_map<int, std::map<uint64_t, std::pair< char*, int >>> oploglist_t;	// group, sequence
typedef std::vector<std::pair<void *, int>> keylist_t;

struct oplog_info {
	uint8_t spaceid;
	uint32_t prefix;
	buflist_t buflist;
	keylist_t oplog_keys;
	oploglist_t oplog_list;

	~oplog_info() {
	    // free the oplog buffers
        for (const auto &p: buflist) {
            free(p.first);
        }
	}
};

inline std::string print_key(const char* in, unsigned length)
{
	unsigned i;
	std::string out;
    char buf[10];

    out.reserve(length * 2);
    for (i=0; i < length; ++i) {
    	snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
        out.append(buf);
    }
    return out;
}

#endif /* SRC_KVSSD_KADI_TYPES_H_ */
