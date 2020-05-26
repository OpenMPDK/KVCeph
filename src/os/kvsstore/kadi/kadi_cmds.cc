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
#include "kadi_cmds.h"
#include <unistd.h>
#include <limits.h>
#include <math.h>
#include <time.h>
#include <atomic>
#include <vector>

#include <unordered_map>
#include "../kvsstore_debug.h"

using namespace std;

#undef dout_prefix
#define dout_prefix *_dout << "[kadi] "

#ifndef derr
#define derr std::cerr
#endif

#ifndef dendl
#define dendl std::endl
#endif

#define OPTION_DISABLE_ITERATOR 0x10
#define OPTION_DISABLE_AOL      0x20

#define OPTION_LOGGING   (OPTION_DISABLE_ITERATOR)
#define OPTION_NOLOGGING (OPTION_DISABLE_ITERATOR|OPTION_DISABLE_AOL)

int KADI::kv_delete_aio(uint8_t space_id, void *key, kv_key_t keylength, const kv_cb& cb) {
    aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);

    memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    ioctx->cmd.opcode = nvme_cmd_kv_delete;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw4 = 1;
    if (keylength <= KVCMD_INLINE_KEY_MAX) {
        memcpy((void*)ioctx->cmd.key, (void*)key, keylength);
    } else {
        ioctx->cmd.key_addr = (__u64)key;
    }
    ioctx->cmd.key_length = keylength;
    ioctx->cmd.reqid = ioctx->index;
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->key.key = key;
    ioctx->key.length = keylength;
    ioctx->value.value = 0;
    ioctx->value.length = 0;
    ioctx->value.offset = 0;

    int ret = ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd);
    if (ret  < 0) {
        cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

    return 0;
}


int KADI::kv_retrieve_aio(uint8_t space_id, void *key, kv_key_t keylength, void *value, int valoff, int vallength, const kv_cb& cb) {
    aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);
    memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    //TR << "ASYNC READ TRACE: key = " << print_kvssd_key(key->key, key->length) << ", value offset = " << value->offset ;

    ioctx->cmd.opcode = nvme_cmd_kv_retrieve;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw4 = 0;
    ioctx->cmd.cdw5 = valoff;
    ioctx->cmd.data_addr = (__u64)value;
    ioctx->cmd.data_length = vallength;
    ioctx->cmd.key_length = keylength;
    ioctx->cmd.reqid = ioctx->index;
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->key.key = key;
    ioctx->key.length = keylength;
    ioctx->value.value = value;
    ioctx->value.length = vallength;
    ioctx->value.offset = valoff;

    if (keylength <= KVCMD_INLINE_KEY_MAX) {
        memcpy((void*)ioctx->cmd.key, (void*)key, keylength);
    } else {
        ioctx->cmd.key_addr = (__u64)key;
    }

    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd) < 0) {
        cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

    return 0;
}

int KADI::kv_store_aio(uint8_t space_id, void *key, kv_key_t keylength, void *value, int valoff, int vallength, const kv_cb& cb) {
    aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);


    memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    ioctx->cmd.opcode = nvme_cmd_kv_store;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw4 = (space_id == this->keyspace_sorted)? OPTION_LOGGING:OPTION_NOLOGGING;
    ioctx->cmd.cdw5 = valoff;
    ioctx->cmd.key_length = keylength;
    ioctx->cmd.cdw11 = keylength -1;
    ioctx->cmd.data_addr = (__u64)value;
    ioctx->cmd.data_length = vallength;
    ioctx->cmd.cdw10 = (vallength >>  2);
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->cmd.reqid = ioctx->index;
    ioctx->key.key = key;
    ioctx->key.length = keylength;
    ioctx->value.value = value;
    ioctx->value.length = vallength;
    ioctx->value.offset = valoff;

    //TR << "kv_store_aio: spaceid = " << (int)space_id << ", sorted spaceid = " << this->keyspace_sorted << ", logging?" << std::hex << ioctx->cmd.cdw4 << std::dec;

    if (keylength > KVCMD_INLINE_KEY_MAX) {
        ioctx->cmd.key_addr = (__u64)key;
    } else {
        memcpy((void*)ioctx->cmd.key, (void*)key, keylength);
    }

    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd) < 0) {
        cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

    return 0;
}


///-------------------------------------------------------


int KADI::open(const std::string &devpath, int keyspace_sorted_) {

    int ret = 0;

    this->fd = ::open(devpath.c_str(), O_RDWR);
    if (this->fd < 0) {
    	std::cout << devpath << std::endl;
        derr <<  "can't open a device : " << devpath << dendl;
        return this->fd;
    }

    this->keyspace_sorted = keyspace_sorted_;
    this->nsid = ioctl(fd, NVME_IOCTL_ID);
    if (this->nsid == (unsigned) -1) {
        derr <<  "can't get an ID" << dendl;
        return -1;
    }

//    cmd_ctx_mgr.init(this->qdepth);

    aioctx_ctxid   = ioevent_mgr.init(this->fd);
    if (aioctx_ctxid == -1) return aioctx_ctxid;

    derr << ">> KVSSD is opened: " << devpath.c_str() << ", ctx id = " << aioctx_ctxid << dendl;
    return ret;
}

int KADI::close() {
    if (this->fd > 0) {
    	ioevent_mgr.close(this->fd);
        ::close(fd);

        //cmd_ctx_mgr.close();
        derr << ">> KV device is closed: fd " << fd << dendl;
        fd = -1;

    }
    return 0;
}

/// Write
/// -----------------------------------------------------------------------



int KADI::kv_store_sync(uint8_t space_id, kv_key *key, kv_value *value) {
    struct nvme_passthru_kv_cmd cmd;
    memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    cmd.opcode = nvme_cmd_kv_store;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw4 = (space_id == this->keyspace_sorted)? OPTION_LOGGING:OPTION_NOLOGGING;
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

    int ret =ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);

    if (ret < 0) {
        return -1;
    }
    return 0;
}

int KADI::kv_store_sync(uint8_t space_id, kv_value *value, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill) {
    struct nvme_passthru_kv_cmd cmd;
    memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    //TR << "<TEST> value = " << value->value << ", offset = " << value->offset << ", value length = " << value->length ;

    cmd.opcode = nvme_cmd_kv_store;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw4 = (space_id == this->keyspace_sorted)? OPTION_LOGGING:OPTION_NOLOGGING;
    cmd.cdw5 = value->offset;

    fill(cmd);

    cmd.cdw11 = cmd.key_length -1;
    cmd.data_addr = (__u64)value->value;
    cmd.data_length = value->length;
    cmd.cdw10 = (value->length >>  2);


    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
#ifdef ENABLE_IOTRACE
    void *keyptr = 0;
    if (cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
        keyptr = (void*)cmd.key;
    } else {
        keyptr = (void*)cmd.key_addr;
    }
    std::string itermsg = (cmd.cdw4 == OPTION_NOLOGGING)? "NOAOL":"AOL";
    if (ret == 0) {

        TRIO << "<kv_store_sync> " << print_kvssd_key(std::string((const char*)keyptr, cmd.key_length))
           << ", value = " << value->value << ", value offset = " << value->offset << ", value length = " << value->length << ", LOG?" << itermsg << ", hash " << ceph_str_hash_linux((const char*)value->value, value->length) << " OK" ;

    } else {
        TR << "<kv_store_sync> " << print_kvssd_key(std::string((const char*)keyptr, cmd.key_length))
           << ", value = " << value->value << ", value offset = " << value->offset << ", value length = " << value->length
           << ", retcode = " << ret << ", FAILED" ;
    }
#endif
    if (ret < 0) {
        return -1;
    }
    return 0;
}
int KADI::kv_store_aio(uint8_t space_id, kv_value *value, const kv_cb& cb, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill)
{
	aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);

	memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    ioctx->cmd.opcode = nvme_cmd_kv_store;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw4 = (space_id == this->keyspace_sorted)? OPTION_LOGGING:OPTION_NOLOGGING;
    ioctx->cmd.cdw5 = value->offset;

    fill(ioctx->cmd);

    ioctx->cmd.cdw11 = ioctx->cmd.key_length -1;
    ioctx->cmd.data_addr = (__u64)value->value;
    ioctx->cmd.data_length = value->length;
    ioctx->cmd.cdw10 = (value->length >>  2);
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->cmd.reqid = ioctx->index;
    ioctx->key.length = ioctx->cmd.key_length;
	ioctx->key.key = (ioctx->cmd.key_length <= KVCMD_INLINE_KEY_MAX)? (void*)ioctx->cmd.key:(void*)ioctx->cmd.key_addr;
	ioctx->value.value = value->value;
	ioctx->value.length = value->length;
	ioctx->value.offset = value->offset;


    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd) < 0) {
    	cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

#ifdef ENABLE_IOTRACE_SUBMIT
    void *keyptr = 0;
    if (ioctx->cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
        keyptr = (void*)ioctx->cmd.key;
    } else {
        keyptr = (void*)ioctx->cmd.key_addr;
    }
    std::string itermsg = (ioctx->cmd.cdw4 == OPTION_NOLOGGING)? "NOAOL":"AOL";
    TRIO << "{kv_store_aio} " << print_kvssd_key(std::string((const char*)keyptr, ioctx->cmd.key_length))
         << ", value = " << value->value << ", value offset = " << value->offset << ", value length = " << value->length << ", LOG? " << itermsg << ", hash " << ceph_str_hash_linux((const char*)value->value, value->length) << " OK" ;
#endif
    return 0;
}



/// Read
/// -----------------------------------------------------------------------

int KADI::kv_retrieve_aio(uint8_t space_id, kv_key *key, kv_value *value, const kv_cb& cb) {
	aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);
	memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

	//TR << "ASYNC READ TRACE: key = " << print_kvssd_key(key->key, key->length) << ", value offset = " << value->offset ;

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
	ioctx->key.key = key->key;
	ioctx->key.length = key->length;
	ioctx->value.value = value->value;
	ioctx->value.length = value->length;
	ioctx->value.offset = value->offset;

    if (key->length <= KVCMD_INLINE_KEY_MAX) {
        memcpy((void*)ioctx->cmd.key, (void*)key->key, key->length);
    } else {
        ioctx->cmd.key_addr = (__u64)key->key;
    }
    //ioctx->t1 = std::chrono::high_resolution_clock::now();
    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd) < 0) {
    	cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

#ifdef ENABLE_IOTRACE_SUBMIT
    void *keyptr = 0;
    if (ioctx->cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
        keyptr = (void*)ioctx->cmd.key;
    } else {
        keyptr = (void*)ioctx->cmd.key_addr;
    }
    std::string itermsg = (ioctx->cmd.cdw4 == OPTION_NOLOGGING)? "NOAOL":"AOL";
    TRIO << "{kv_retrieve_aio} " << print_kvssd_key(std::string((const char*)keyptr, ioctx->cmd.key_length))
       << ", value = " << value->value << ", value length = " << value->length << ", value offset = " << value->offset;
#endif

    return 0;
}

int KADI::kv_retrieve_sync(uint8_t space_id, kv_key *key, kv_value *value) {
    struct nvme_passthru_kv_cmd cmd;
    memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    //TR << "kv retrieve sync: key = " << print_kvssd_key(key->key, key->length) << ", buffer  length = " << value->length << ", buffer offset =  " << value->offset << ", buffer = " << value->value ;
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

    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
#ifdef ENABLE_IOTRACE
    if (ret == 0) {
        TRIO << "<kv_retrieve_sync> " << print_kvssd_key(std::string((const char*)key->key, key->length))
           << ", value = " << value->value << ", offset = " << value->offset<< ", actual length " << value->actual_value_size  << ", value length = " << std::min(cmd.result, value->length) << ", hash " << ceph_str_hash_linux((const char*)value->value, value->length)<< " OK" ;
    } else {
        TR << "<kv_retrieve_sync> " << print_kvssd_key(std::string((const char*)key->key, key->length))
           << ", value = " << value->value << ", offset = " << value->offset << ", actual length " << value->actual_value_size << ", value length = " << value->length
           << ", retcode = " << ret << ", FAILED" ;
    }
#endif
    if (ret == 0) {
        value->actual_value_size = cmd.result;
        value->length = std::min(cmd.result, value->length);
        return 0;
    }

	return ret;
}



int KADI::kv_retrieve_sync(uint8_t space_id, kv_value *value, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill) {
    struct nvme_passthru_kv_cmd cmd;
    memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw4 = 0;
    cmd.cdw5 = value->offset;
    cmd.data_addr = (__u64)value->value;
    cmd.data_length = value->length;

    fill(cmd);

    int ret= ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
    if (ret == 0) {
        value->actual_value_size = cmd.result;
        value->length = std::min(cmd.result, value->length);
    }

#ifdef ENABLE_IOTRACE
    void *keyptr = 0;
    if (cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
        keyptr = (void*)cmd.key;
    } else {
        keyptr = (void*)cmd.key_addr;
    }
    if (ret == 0) {
        TRIO << "<kv_retrieve_sync> " << print_kvssd_key((const char*)keyptr, cmd.key_length)
           << ", value = " << value->value << ", value offset = " << value->offset << ", actual length " << value->actual_value_size << ", value length = " << std::min(cmd.result, value->length) << ", hash " << ceph_str_hash_linux((const char*)value->value, value->length)<< " OK" ;
    } else {
        TR << "<kv_retrieve_sync> " << print_kvssd_key((const char*)keyptr, cmd.key_length)
           << ", value = " << value->value << ", value offset = " << value->offset << ", actual length " << value->actual_value_size  << ", value length = " << value->length
           << ", retcode = " << ret << ", FAILED" ;
    }
#endif

	return ret;
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
#ifdef ENABLE_IOTRACE
    if (ret == 0) {
        TRIO << "<kv_retrieve_sync> " << print_kvssd_key(std::string((const char*)key->key, key->length))
           << ", value = " << value->value << ", offset = " << value->offset<< ", value length = " << std::min(cmd.result, value->length) << ", hash " << ceph_str_hash_linux((const char*)value->value, value->length)<< " OK" ;
    } else {
        TR << "<kv_retrieve_sync> " << print_kvssd_key(std::string((const char*)key->key, key->length))
           << ", value = " << value->value << ", offset = " << value->offset<< ", value length = " << value->length
           << ", retcode = " << ret << ", FAILED" ;
    }
#endif
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

    //derr << "kv_retrieve: space id = " << (int)space_id << ", value offset = " << value->offset << ", valuelength = " << value->length << ", keylength = " << (int)key->length << ", ret = " << ret << dendl;
    return ret;
}

/// Delete
/// -----------------------------------------------------------------------

int KADI::kv_delete_sync(uint8_t space_id, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill) {
	struct nvme_passthru_kv_cmd cmd;
	memset((void*)&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

	cmd.opcode = nvme_cmd_kv_delete;
	cmd.nsid = nsid;
	cmd.cdw3 = space_id;
	cmd.cdw4 = 1;

	fill(cmd);

	int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
#ifdef ENABLE_IOTRACE
    void *keyptr = 0;
    if (cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
        keyptr = (void*)cmd.key;
    } else {
        keyptr = (void*)cmd.key_addr;
    }
    if (ret == 0) {
        TRIO << "<kv_delete_sync> " << print_kvssd_key(std::string((const char*)keyptr, cmd.key_length)) << " OK" ;
    } else {
        TR << "<kv_delete_sync> , retcode = " << ret << ", FAILED" ;
    }
#endif
	if (ret < 0) {
		return -1;
	}

	return 0;
}

int KADI::kv_delete_aio(uint8_t space_id, const kv_cb& cb, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill) {
    aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);

    memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    ioctx->cmd.opcode = nvme_cmd_kv_delete;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw4 = 1;

    fill(ioctx->cmd);

    ioctx->cmd.reqid = ioctx->index;
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->key.length = ioctx->cmd.key_length;
    if (ioctx->cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
    	ioctx->key.key = (void*)ioctx->cmd.key;
	} else {
		ioctx->key.key = (void*)ioctx->cmd.key_addr;
	}
	ioctx->value.value = 0;
	ioctx->value.length = 0;
	ioctx->value.offset = 0;

    //ioctx->t1 = std::chrono::high_resolution_clock::now();

    int ret = ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd);
    if (ret < 0) {
        cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

#ifdef ENABLE_IOTRACE_SUBMIT
    void *keyptr = 0;
    if (ioctx->cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
        keyptr = (void*)ioctx->cmd.key;
    } else {
        keyptr = (void*)ioctx->cmd.key_addr;
    }
    if (ret == 0) {
        TRIO << "<kv_delete_aio> " << print_kvssd_key(std::string((const char*)keyptr, ioctx->cmd.key_length)) << " OK" ;
    } else {
        TR << "<kv_delete_aio> , retcode = " << ret << ", FAILED" ;
    }
#endif
    return 0;
}

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

    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
#ifdef ENABLE_IOTRACE
    if (ret == 0) {
        TRIO << "<kv_delete_sync> " << print_kvssd_key(std::string((const char*)key->key, key->length))
           << " OK" ;
    } else {
        TR << "<kv_delete_sync> " << print_kvssd_key(std::string((const char*)key->key, key->length))
           << ", retcode = " << ret << ", FAILED" ;
    }
#endif
    if (ret < 0) {
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
	ioctx->key.key = key->key;
	ioctx->key.length = key->length;
	ioctx->value.value = 0;
	ioctx->value.length = 0;
	ioctx->value.offset = 0;

    ioctx->t1 = std::chrono::high_resolution_clock::now();

    int ret = ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd);
    if (ret  < 0) {
        cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

#ifdef ENABLE_IOTRACE_SUBMIT
    void *keyptr = 0;
    if (ioctx->cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
        keyptr = (void*)ioctx->cmd.key;
    } else {
        keyptr = (void*)ioctx->cmd.key_addr;
    }
    if (ret == 0) {
        TRIO << "<kv_delete_aio> " << print_kvssd_key(std::string((const char*)keyptr, ioctx->cmd.key_length)) << " OK" ;
    } else {
        TR << "<kv_delete_aio> , retcode = " << ret << ", FAILED" ;
    }
#endif
    return 0;
}

/// Iterator
/// -----------------------------------------------------------------------

int KADI::iter_open(kv_iter_context *iter_handle, int space_id)
{
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));

    cmd.opcode = nvme_cmd_kv_iter_req;
    cmd.cdw4 = (ITER_OPTION_OPEN | ITER_OPTION_LOG_KEY_SPACE); //ITER_OPTION_OPEN | ITER_OPTION_KEY_ONLY |
    cmd.cdw3 = space_id;
    cmd.nsid = space_id;
    cmd.cdw12 = iter_handle->prefix;
    cmd.cdw13 = iter_handle->bitmask;

    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);

    if (ret < 0) {
    	std::cout << "iter open failed: " << ret << endl;
        return -1;
    }

    iter_handle->handle = cmd.result & 0xff;

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

    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
#ifdef ENABLE_IOTRACE
    if (ret == 0) {
        TRIO << "<ITER_CLOSE>  OK" ;
    } else {
        TR << "<ITER_CLOSE> , retcode = " << ret << ", FAILED" ;
    }
#endif
    if (ret < 0) {
        return -1;
    }
    return cmd.status;
}

int KADI::iter_read(int space_id, unsigned char handle, void *buf, uint32_t buflen, int &byteswritten, bool &end) {
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_iter_read;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw5 = handle;
    cmd.data_addr = (__u64)buf;
    cmd.data_length = buflen;

    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);

    if (ret < 0) { return -1; }

    byteswritten = cmd.result & 0xffff;
    if (byteswritten > (int)buflen) {
        return -1;
    }

    if (cmd.status == 0x0393) { /* scan finished, but data is valid */
        end = true;
        cmd.status =0;
    }
    else
        end = false;

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
	ioctx->key.key = 0;
	ioctx->key.length = 0;
	ioctx->value.value = 0;
	ioctx->value.length = 0;
	ioctx->value.offset = 0;

    int ret = ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd);
    if (ret < 0) {
        derr << "fail to send aio command ret = " << ret << dendl;
        return -1;
    }
#ifdef ENABLE_IOTRACE_SUBMIT
    if (ret == 0) {
        TRIO << "{iter_read_aio} OK" ;
    } else {
        TR << "{iter_read_aio} , retcode = " << ret << ", FAILED" ;
    }
#endif
    return 0;
}
 

/// A callback function for an asynchronous iterator
/// ---------------------------------------------------

void iter_callback(kv_io_context &op, void* private_data)
{
    oplog_page *ctx = (oplog_page *)private_data;
    ctx->end = op.hiter.end;
	ctx->retcode = op.retcode;
	ctx->byteswritten = op.hiter.byteswritten;
	if (ctx->parent) {
        ((KvsAioContext<oplog_page>*)ctx->parent)->fire_event(ctx);
    }
}

void oplogpage_callback(kv_io_context &op, void* private_data)
{
    oplog_page *ctx = (oplog_page *)private_data;
	ctx->retcode = op.retcode;
	ctx->byteswritten = op.value.length;

    if (ctx->parent) {
        //TR << "fire, read bytes = " << op.value.length << ", ret = " << op.retcode;
        ((KvsAioContext<oplog_page>*)ctx->parent)->fire_event(ctx);
    }

}

int KADI::read_oplogpage_dir(struct oplog_info &info)
{
    int r = 0, optype, length;
    void *key;

    kv_iter_context ctx;
    ctx.prefix = info.prefix;
    ctx.bitmask = 0xffffffff;

    r = retrieve_oplogpage_dir_sync(&ctx, info);	// async
    if (r != 0) return r;

    for (const auto &p: info.oplogpage_dir) {
        iterbuf_reader reader(nullptr, p->buf, p->byteswritten);
        while (reader.nextkey(&optype, &key, &length)) {
            info.oplogpage_keys.emplace_back(key, length);
        }
    }

    return r;
    // oplog_keys now contain keys, buflist contains the buffers that have keys
}


int KADI::retrieve_oplogpage_dir_sync(kv_iter_context *iter_ctx, struct oplog_info &info)
{
    FTRACE
    int r = iter_open(iter_ctx, info.spaceid);
    if (r != 0) return r;

    int id = 0;
    bool end = false;

    while (!end) {
            oplog_page *sub_iter_ctx = new oplog_page(id++, iter_ctx->handle, ITER_BUFSIZE, 0);

            r = iter_read(info.spaceid, iter_ctx->handle, sub_iter_ctx->buf, sub_iter_ctx->buflen, sub_iter_ctx->byteswritten, end );
            if (r != 0) { delete sub_iter_ctx; goto error_exit; }

            info.oplogpage_dir.push_back(sub_iter_ctx);
    }


error_exit:

    if (iter_ctx) iter_close(iter_ctx, info.spaceid);

    return r;
}

int KADI::retrieve_oplogpage_dir(kv_iter_context *iter_ctx, struct oplog_info &info)
{
    FTRACE
    int sent = 0;
    bool finished = false;
    static const int qdepth = 4;
    int r = iter_open(iter_ctx, info.spaceid);
    if (r != 0) return r;

    KvsAioContext<oplog_page> iter_aioctx;

    int id = 0;
    do {
        if (!finished) {
            while (sent < qdepth) {
                oplog_page *sub_iter_ctx = new oplog_page(id++, iter_ctx->handle, ITER_BUFSIZE, &iter_aioctx);

                r = iter_read_aio(info.spaceid, iter_ctx->handle, sub_iter_ctx->buf, sub_iter_ctx->buflen, { iter_callback, sub_iter_ctx } );
                if (r != 0) { delete sub_iter_ctx; goto error_exit; }
                sent++;
            }
        }

        if (sent > 0) {

            std::vector<oplog_page *> ctxs;
            if (iter_aioctx.get_ready_iterpages_events(ctxs)) {

                sent -= ctxs.size();

                for (oplog_page *sub_iter_ctx: ctxs) {

                    finished |= sub_iter_ctx->end;

                    if (sub_iter_ctx->byteswritten > 0) {
                        info.oplogpage_dir.push_back(sub_iter_ctx);
                    }
                    else {
                        delete sub_iter_ctx;
                    }
                }
            }
        }
    } while (!finished || sent != 0);

error_exit:

    if (iter_ctx) iter_close(iter_ctx, info.spaceid);

    return r;
}


int KADI::read_oplogpages(struct oplog_info &info)
{
    FTRACE
    int r;
    int sent = 0;
    static const int qdepth = 4;

    KvsAioContext<oplog_page> aioctx;

    auto it  = info.oplogpage_keys.begin();
    auto end = info.oplogpage_keys.end();
    auto max = info.oplogpage_keys.size();
    int id = 0;
    kv_key k;
    kv_value v;
    TRI << "total number of oplogpage keys = " << max;
    do {
        {
            for (size_t i =0 ; i < qdepth && it != end; ++i, ++it) {
                oplog_page *oplog = new oplog_page(id++, 0, ITER_BUFSIZE, &aioctx);
                const struct oplog_key *oplogkey = ((struct oplog_key*)it->first);

                k.key    = it->first;
                k.length = it->second;
                v.value  = oplog->buf;
                v.length = oplog->buflen;
                v.offset = 0;
                oplog->groupid = oplogkey->groupid;
                oplog->sequence = oplogkey->sequenceid;

                //TR << "oplogpage key: id " << oplog->id << "/" << max << ", key = " << print_kvssd_key(k.key, k.length) << ", length = " << (int)k.length;

                r = kv_retrieve_aio(ksid_oplog, &k, &v, { oplogpage_callback, oplog });
                if (r != 0) { TR << "oplog page - read error "; return true; }
                sent++;
            }
        }

        if (sent > 0) {
            std::vector<oplog_page *> ctxs;
            if (aioctx.get_ready_iterpages_events(ctxs)) {

                sent -= ctxs.size();
                max  -= ctxs.size();

                for (oplog_page *oplog: ctxs) {
                    //TR << "oplog read: id = " << oplog->id;
                    if (oplog->byteswritten > 0) {
                        info.oplog_list[oplog->groupid].insert(std::make_pair(oplog->sequence, std::make_pair(oplog->buf, oplog->byteswritten)));
                    }
                    else {
                        delete oplog;
                    }
                }
            }
        }
    } while (max != 0 || sent != 0);

    return r;
}

void oplog_delete_cb(kv_io_context &op, void* private_data) {
    oplog_remove_ctx* ctx = ((oplog_remove_ctx*)private_data);
    ctx->remained.fetch_sub(1, std::memory_order_relaxed);
    ctx->inflight.fetch_sub(1, std::memory_order_relaxed);
}

int KADI::delete_oplogpages(struct oplog_info &info)
{
    FTRACE
    int r = 0;
    bool finished = false;
    static const int qdepth = 4;

    oplog_remove_ctx remove_ctx(info.oplogpage_keys.size());


    auto it  = info.oplogpage_keys.begin();
    auto end = info.oplogpage_keys.end();

    do {
        kv_key k;

        const unsigned max = qdepth - remove_ctx.inflight.load(std::memory_order_relaxed);

        for (size_t i =0 ; i < max && it != end; ++i, ++it) {
            k.key    = it->first;
            k.length = it->second;

            r = kv_delete_aio(ksid_oplog, &k, { oplog_delete_cb, &remove_ctx});
            if (r != 0) { TR << "oplog page - read error "; return r; }
            remove_ctx.inflight.fetch_add(1, std::memory_order_relaxed);
        }

        finished = (remove_ctx.remained.load(std::memory_order_relaxed) == 0);

    } while (!finished);

    return r;
}



uint64_t KADI::list_oplog(const uint8_t spaceid, const uint32_t prefix, const std::function< void (int, int, uint64_t, const char*, int) > &key_listener)
{
    FTRACE
    int r = 0;
	int opcode;
	void *key;
	int length;
    uint64_t total_keys = 0;
    //static const int oplog_pagesize = 28*1024;

    struct oplog_info info (spaceid, prefix);

    // step 1. read oplog page list
    r = read_oplogpage_dir(info);
    TRI << "oplog dir pages = " << info.oplogpage_dir.size() << ", r = " << r;
    if (r != 0) return 0;

    // step 2. read oplog pages
    r = read_oplogpages(info);
    if (r != 0) return 0;

    TRI << "read is done, updating the index structure, pages read = " << info.oplog_list.size();

    for (const auto &groups: info.oplog_list) {
        const int groupid = groups.first;
        for (const auto &sequences : groups.second) {
            int cur = 0;
            opbuf_reader reader(nullptr, groupid, sequences.second.first, sequences.second.second);
            while (reader.nextkey(&opcode, &key, &length)) {
                TRI << "oplog key " << print_kvssd_key(key, length) ;
                key_listener(opcode, groupid, sequences.first, (const char*)key, length);
                cur++;
            }
            total_keys += reader.numkeys_ret();
        }
    }

    delete_oplogpages(info);

    return total_keys;
}


#if 0
kv_result KADI::iter_readall(kv_iter_context *iter_ctx, buflist_t &buflist, int space_id)
{
    kv_result r = iter_open(iter_ctx, space_id);
    if (r != 0) return r;

    while (!iter_ctx->end) {
        iter_ctx->byteswritten = 0;
        iter_ctx->buf = (char*)malloc(ITER_BUFSIZE); //= malloc(iter_ctx->buflen);
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
            buflist.push_back(std::make_pair(iter_ctx->buf, iter_ctx->byteswritten));
        }

    }

exit:
    r = iter_close(iter_ctx, space_id);
    return r;
}
#endif

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
		ioctx->key.key = 0;
		ioctx->key.length = 0;
		ioctx->value.value = 0;
		ioctx->value.length = 0;
		ioctx->value.offset = 0;

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

	//int num_finished_ios = std::min(ioevent_mgr.poll(timeout_us), MAX_AIO_EVENTS);
	int ret = 0;
	int events = 0;
    int num_finished_ios = MAX_AIO_EVENTS;

    while (num_finished_ios) {
        struct nvme_aioevents aioevents;
        aioevents.nr = num_finished_ios;
        aioevents.ctxid = aioctx_ctxid;

        if (ioctl(fd, NVME_IOCTL_GET_AIOEVENT, &aioevents) < 0) {
            fprintf(stderr, "fail to read IOEVETS \n");
            ret = -1; goto exit;
        }

        if (aioevents.nr == 0) break;

        //TRI << "completed " << aioevents.nr << " IOs, total " << completed_ios.load() << "/" << submitted_ios.load();
        for (int i = 0; i < aioevents.nr; i++) {
            const struct nvme_aioevent &event  = aioevents.events[i];
            aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_pending_cmdctx(event.reqid);
            if (ioctx == 0) {
                exit(1);
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
    memcpy(&ioresult.key, &ioctx.key, sizeof(ioctx.key));
    memcpy(&ioresult.value, &ioctx.value, sizeof(ioctx.value));


    //ioresult.latency = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - ioctx.t1).count();

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

#ifdef ENABLE_IOTRACE
    struct __attribute__((__packed__)) kvs_object_key
    {
        uint8_t          group;                        //1B
        int8_t           shardid;                      //1B
        uint64_t         poolid;                       //8B
        uint32_t         bitwisekey;                   //4B
        uint64_t         snapid;                       //8B
        uint64_t         genid;                        //8B
        uint16_t         blockid;					   //2B - 32+ Bytes
        // followed by name
    };

#endif

    if (ioresult.retcode != 0) {
        return 0;
    }

    switch (ioresult.opcode)
    {

    case nvme_cmd_kv_retrieve:

    	ioresult.value.actual_value_size = event.result;
        ioresult.value.length = std::min(event.result, ioctx.value.length);
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
            memset(ioresult.batch_results, 0, 8 * sizeof(kv_result)); // set all results as success
        }
        break;
    };

    return 0;
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

	////std::cerr << "add store cmd : start = " << subcmd_offset << std::endl;
	////std::cerr << "add store cmd : aligned key    = " << align_64(key_length) << std::endl;
	////std::cerr << "add store cmd : value  length  = " << value_length << std::endl;
	////std::cerr << "add store cmd : aligned value  length  = " << ((511 - 1) / 64 + 1)*64 << std::endl;
	////std::cerr << "add store cmd : aligned value  = " << align_64(value_length) << std::endl;
	////std::cerr << "add store cmd : offset = " << subcmd_offset << std::endl;

	subcmd_index++;
	return true;
}

bool KvBatchCmd::add_store_cmd(int nsid, int option,
		const std::function< uint8_t (char*) > &construct_key,
		const std::function< uint32_t (char*) > &construct_value)
{
	if (isfull()) return false;

	char *body = payload + sizeof(batch_cmd_head);

	subcmd_offset += SUBCMD_HEAD_SIZE;
	const uint8_t key_length = construct_key(body + subcmd_offset);
	subcmd_offset += align_64(key_length);

	uint32_t value_length = construct_value(body + subcmd_offset);
	subcmd_offset += align_64(value_length);

	batch_cmd_head* batch_head = (batch_cmd_head*)payload;
	batch_head->attr[subcmd_index].opcode = nvme_cmd_kv_store;
	batch_head->attr[subcmd_index].keySize = key_length;
	batch_head->attr[subcmd_index].valuseSize = value_length;
	batch_head->attr[subcmd_index].nsid = nsid;
	batch_head->attr[subcmd_index].option = option;

	////std::cerr << "add store cmd : start = " << subcmd_offset << std::endl;
	////std::cerr << "add store cmd : aligned key    = " << align_64(key_length) << std::endl;
	////std::cerr << "add store cmd : value  length  = " << value_length << std::endl;
	////std::cerr << "add store cmd : aligned value  length  = " << ((511 - 1) / 64 + 1)*64 << std::endl;
	////std::cerr << "add store cmd : aligned value  = " << align_64(value_length) << std::endl;
	////std::cerr << "add store cmd : offset = " << subcmd_offset << std::endl;

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
/*
int kv_batch_context::batch_store(int nsid, int option, const std::function< uint8_t (char*) > &construct_key, const std::function< uint32_t (char*) > &construct_value)
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
*/
