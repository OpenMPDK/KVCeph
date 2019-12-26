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
#include "kadi_sort.h"
#include <unistd.h>
#include <limits.h>
#include <math.h>
#include <time.h>
#include <atomic>
#include <vector>
#include <unordered_map>
#include "../../kvsstore_debug.h"
#include "include/ceph_hash.h"
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

    derr << ">> KVSSD is opened: " << devpath.c_str() << ", ctx id = " << aioctx_ctxid << dendl;
    return ret;
}

int KADI::close() {
    if (this->fd > 0) {
    	ioevent_mgr.close(this->fd);
        ::close(fd);

        cmd_ctx_mgr.close();
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
    cmd.cdw4 = (space_id == 0)? OPTION_LOGGING:OPTION_NOLOGGING;
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
#ifdef ENABLE_IOTRACE
    std::string itermsg = (cmd.cdw4 == OPTION_NOLOGGING)? "NOAOL":"AOL";
    if (ret == 0) {
        TRIO << "<kv_store_sync> " << print_kvssd_key(std::string((const char*)key->key, key->length))
           << ", value = " << value->value << ", value offset = " << value->offset << ", value length = " << value->length << ", LOG? " << itermsg << ", hash " << ceph_str_hash_linux((const char*)value->value, value->length) << " OK" ;

    } else {
        TR << "<kv_store_sync> " << print_kvssd_key(std::string((const char*)key->key, key->length))
           << ", value = " << value->value << ", value offset = " << value->offset << ", value length = " << value->length
           << ", retcode = " << ret << ", FAILED" ;
    }
#endif
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
    cmd.cdw4 = (space_id == 0)? OPTION_LOGGING:OPTION_NOLOGGING;
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
    ioctx->cmd.cdw4 = (space_id == 0)? OPTION_LOGGING:OPTION_NOLOGGING;
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

#ifdef ENABLE_IOTRACE
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

int KADI::kv_store_aio(uint8_t space_id, kv_key *key, kv_value *value, const kv_cb& cb) {
	aio_cmd_ctx *ioctx = cmd_ctx_mgr.get_cmd_ctx(cb);

	memset((void*)&ioctx->cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    ioctx->cmd.opcode = nvme_cmd_kv_store;
    ioctx->cmd.nsid = nsid;
    ioctx->cmd.cdw3 = space_id;
    ioctx->cmd.cdw4 = (space_id == 0)? OPTION_LOGGING:OPTION_NOLOGGING;
    ioctx->cmd.cdw5 = value->offset;
    ioctx->cmd.key_length = key->length;
    ioctx->cmd.cdw11 = key->length -1;
    ioctx->cmd.data_addr = (__u64)value->value;
    ioctx->cmd.data_length = value->length;
    ioctx->cmd.cdw10 = (value->length >>  2);
    ioctx->cmd.ctxid = aioctx_ctxid;
    ioctx->cmd.reqid = ioctx->index;
	ioctx->key.key = key->key;
	ioctx->key.length = key->length;
	ioctx->value.value = value->value;
	ioctx->value.length = value->length;
	ioctx->value.offset = value->offset;

    if (key->length > KVCMD_INLINE_KEY_MAX) {
        ioctx->cmd.key_addr = (__u64)key->key;
    } else {
        memcpy((void*)ioctx->cmd.key, (void*)key->key, key->length);
    }

    if (ioctl(fd, NVME_IOCTL_AIO_CMD, &ioctx->cmd) < 0) {
    	cmd_ctx_mgr.release_cmd_ctx(ioctx);
        return -1;
    }

#ifdef ENABLE_IOTRACE
    void *keyptr = 0;
    if (ioctx->cmd.key_length <= KVCMD_INLINE_KEY_MAX) {
        keyptr = (void*)ioctx->cmd.key;
    } else {
        keyptr = (void*)ioctx->cmd.key_addr;
    }
    std::string itermsg = (ioctx->cmd.cdw4 == OPTION_NOLOGGING)? "NOAOL":"AOL";
    TRIO << "{kv_store_aio} " << print_kvssd_key(std::string((const char*)keyptr, ioctx->cmd.key_length))
         << ", value = " << value->value << ", value offset = " << value->offset << ", value length = " << value->length << ", LOG? " << itermsg << ", hash " << ceph_str_hash_linux((const char*)value->value, value->length)<< " OK" ;
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

#ifdef ENABLE_IOTRACE
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

#ifdef ENABLE_IOTRACE
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

#ifdef ENABLE_IOTRACE
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
#ifdef ENABLE_IOTRACE
    if (ret == 0) {
        TRIO << "<ITER_OPEN>  OK" ;
    } else {
        TR << "<ITER_OPEN> , retcode = " << ret << ", FAILED" ;
    }
#endif
    if (ret < 0) {
    	cout << "open failed " << ret << endl;
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


int KADI::iter_read(kv_iter_context *iter_handle, int space_id) {
    struct nvme_passthru_kv_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_iter_read;
    cmd.nsid = nsid;
    cmd.cdw3 = space_id;
    cmd.cdw5 = iter_handle->handle;
    cmd.data_addr = (__u64)iter_handle->buf;
    cmd.data_length = iter_handle->buflen;

    int ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
#ifdef ENABLE_IOTRACE
    if (ret == 0) {
        TRIO << "<ITER_READ> , length = " << iter_handle->byteswritten << ", OK" ;
    } else {
        TR << "<ITER_READ> , retcode = " << ret << ", FAILED" ;
    }
#endif
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
#ifdef ENABLE_IOTRACE
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
	kv_iter_context *ctx = (kv_iter_context *)private_data;
    ctx->end = op.hiter.end;
	ctx->retcode = op.retcode;
	ctx->byteswritten = op.hiter.byteswritten;
	if (ctx->parent) {
        ((KvsAioContext<kv_iter_context> *) ctx->parent)->fire_event(ctx);
    }
}

void oplog_callback(kv_io_context &op, void* private_data)
{
	kv_read_context *ctx = (kv_read_context *)private_data;
	ctx->retcode = op.retcode;
	ctx->byteswritten = op.value.length;

	//std::cerr << "oplog read callback " << ctx->byteswritten << ", retcode = " << ctx->retcode << endl;
	if (ctx->parent)
    	((KvsAioContext<kv_read_context>*)ctx->parent)->fire_event(ctx);
}

int KADI::fill_oplog_info(const uint8_t spaceid, const uint32_t prefix, struct oplog_info &info)
{
	static const int oplog_pagesize = 28*1024;
	int r = 0;
	info.spaceid = spaceid;

	{ // step 1. read oplog key list
		kv_iter_context ctx;
		ctx.prefix = prefix;
		ctx.bitmask = 0xffffffff;
		int ret = iter_readall_aio(&ctx, info.buflist, info.spaceid);	// async
		if (ret != 0) return ret;

		for (const auto &p: info.buflist) {
			int optype;
			void *key;
			int length;
			iterbuf_reader reader(nullptr, p.first, p.second);
			while (reader.nextkey(&optype, &key, &length)) {
				info.oplog_keys.emplace_back(key, length);
			}
		}

		if (info.oplog_keys.empty()) { r = 0; goto clear; }
	}

	{ // step 2. read oplog pages asynchronously
		kv_key k;
		kv_value v;

		int sent = 0;
		std::vector<kv_read_context *> ctxs;
		KvsAioContext<kv_read_context> iter_aioctx;

		iter_aioctx.init(4, [&](int id) {
			return new kv_read_context(id, ksid_oplog, oplog_pagesize, &iter_aioctx);
		});

		const size_t max = iter_aioctx.free_iter_contexts.size();

		auto it  = info.oplog_keys.begin();
		auto end = info.oplog_keys.end();
       // TR << __func__ << "..5" ;
		do {
			for (size_t i =0 ; i < max && it != end; i++) {
				if (info.oplog_keys.empty()) { break; }

				kv_read_context *sub_read_ctx = iter_aioctx.get_free_context();
				if (sub_read_ctx == 0) {  break; }

               // TR << __func__ << "..6" ;
				const auto  &oplogpair = *it;
				const struct oplog_key *oplogkey = ((struct oplog_key*)oplogpair.first);

                sub_read_ctx->buf = (char*)malloc(sub_read_ctx->buflength);
				k.key    = oplogpair.first;
				k.length = oplogpair.second;
				v.value  = sub_read_ctx->buf;
				v.length = sub_read_ctx->buflength;
				v.offset = 0;
				sub_read_ctx->groupid = oplogkey->groupid;
				sub_read_ctx->sequence = oplogkey->sequenceid;

				//cout << "issue oplog read = " << print_key((const char*)k.key, k.length) << ", groupid = " << sub_read_ctx->groupid << endl;

				r = kv_retrieve_aio(ksid_oplog, &k, &v, { oplog_callback, sub_read_ctx });
				if (r != 0) { std::cerr << "read error " << endl; goto clear; }
				sent++;
				it++;
			}

			iter_aioctx.get_events(ctxs);

			if (ctxs.size() > 0) {
				sent -= ctxs.size();

				for (kv_read_context *sub_read_ctx: ctxs) {
					if (sub_read_ctx->retcode != 0) {
						cout << "read failed: retcode =" << sub_read_ctx->retcode << std::endl;
					}
					else if (sub_read_ctx->byteswritten > 0) {
						info.oplog_list[sub_read_ctx->groupid].insert(std::make_pair(sub_read_ctx->sequence, std::make_pair(sub_read_ctx->buf, sub_read_ctx->byteswritten)));
					}
					iter_aioctx.return_free_context(sub_read_ctx);
				}
			}

			ctxs.clear();

		} while (it != end || sent != 0);
	}

	info.spaceid = spaceid;
	info.prefix  = prefix;

	return 0;

clear:
	info.oplog_list.clear();
	info.oplog_keys.clear();
	info.buflist.clear();
	return r;
}

void oplog_delete_cb(kv_io_context &op, void* private_data) {
	((std::atomic<uint64_t> *)private_data)->fetch_add(1, std::memory_order_relaxed);
}

uint64_t KADI::list_oplog(const uint8_t spaceid, const uint32_t prefix, const std::function< void (int, int, uint64_t, const char*, int) > &key_listener)
{
	int opcode;
	void *key;
	int length;
	kv_key del_k;
    uint64_t total_keys = 0;

    struct oplog_info oplog;


    fill_oplog_info(spaceid, prefix, oplog);


    for (const auto &groups: oplog.oplog_list) {
        const int groupid = groups.first;
        for (const auto &sequences : groups.second) {
            int cur = 0;
            opbuf_reader reader(nullptr, groupid, sequences.second.first, sequences.second.second);
            while (reader.nextkey(&opcode, &key, &length)) {
                //TR << "oplog key " << print_kvssd_key(key, length) ;
                key_listener(opcode, groupid, sequences.first, (const char*)key, length);
                cur++;
            }
            total_keys += reader.numkeys_ret();
        }
    }

    //TR << "list_oplog 3" ;
    uint64_t num_complete  = 0, qdepth  = 0;
    uint64_t num_delete = oplog.oplog_keys.size();
    std::atomic<int> completed(0);

    auto it = oplog.oplog_keys.begin();
    while (num_delete > num_complete) {
        while (it != oplog.oplog_keys.end() && qdepth < 16) {
            const auto &p = *it;
            del_k.key = p.first;
            del_k.length = p.second;
            int ret = kv_delete_aio(ksid_oplog, &del_k, { oplog_delete_cb, &completed});
            if (ret != 0) {
                cerr << "oplog: cannot be deleted: " << ret << endl;
                num_delete--;
            }
            else
                qdepth++;
            it++;
        }
        if (qdepth > 0) {
            const int done = completed.exchange(0, std::memory_order_relaxed);
            qdepth       -= done;
            num_complete += done;
            usleep(1);
        }
    }
    //TR << "list_oplog done  ..." << total_keys ;

    return total_keys;
}


int KADI::iter_readall_aio(kv_iter_context *iter_ctx, std::list<std::pair<char*, int> > &buflist, int space_id)
{
    FTRACE

    //std::cerr << __func__ << ": 1" << std::endl;
	int r = iter_open(iter_ctx, space_id);
	if (r != 0) return r;

	KvsAioContext<kv_iter_context> iter_aioctx;

	iter_aioctx.init(4, [&](int id)->kv_iter_context* {
		return new kv_iter_context(id, iter_ctx->handle, &iter_aioctx);
	});

	int sent = 0;

	bool finished = false;
    std::vector<kv_iter_context *> ctxs;
	do {
		const size_t max = iter_aioctx.free_iter_contexts.size();
		if (!finished) {
			for (size_t i =0 ; i < max; i++) {
				kv_iter_context *sub_iter_ctx = iter_aioctx.get_free_context();
				if (sub_iter_ctx == 0) break;

                sub_iter_ctx->buf = (char*)malloc(ITER_BUFSIZE);

				r = iter_read_aio(space_id, iter_ctx->handle,
							sub_iter_ctx->buf, ITER_BUFSIZE,
							{ iter_callback, sub_iter_ctx } );
                //std::cerr << __func__ << ": 6 r = " << r  << std::endl;
				if (r != 0) { goto error_exit; }
                sent++;
			}
		}
        //std::cerr << __func__ << ": 8 sent = " << sent  << std::endl;

		ctxs.clear();

		bool received = iter_aioctx.get_events(ctxs);
		if (!received) continue;

		sent -= ctxs.size();

		for (kv_iter_context *sub_iter_ctx: ctxs) {
            if (sub_iter_ctx->byteswritten > 0) {
				//printf("buffer list: %d bytes\n",  sub_iter_ctx->byteswritten);
				buflist.push_back(std::make_pair(sub_iter_ctx->buf, sub_iter_ctx->byteswritten));
                sub_iter_ctx->buf = 0;
			}

			finished |= sub_iter_ctx->end;
			iter_aioctx.return_free_context(sub_iter_ctx);
            //std::cerr << __func__ << ": 12" << std::endl;
		}
	} while (!finished || sent != 0);

	r = iter_close(iter_ctx, space_id);
	return r;

error_exit:
    std::cerr << __func__ << ": ERROR\n" ;
	buflist.clear();
	if (iter_ctx) iter_close(iter_ctx, space_id);
	return r;

}


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

    switch (ioresult.opcode)
    {
        case nvme_cmd_kv_store:
            if (ioresult.retcode == 0) {
                TRIO << "<STORE AIO> " << print_kvssd_key(std::string((const char*)ioresult.key.key, ioresult.key.length))
                   << ", value length = " << ioctx.value.length
                   << ", hash " << ceph_str_hash_linux((const char*)ioresult.value.value, ioresult.value.length)
                   << " OK" ;
                if (ioctx.value.length == 14) {
                    TRIO << "DEBUG content = " << std::string((const char*)ioresult.value.value, ioresult.value.length);
                }
            } else {
                TR << "<STORE AIO> " << print_kvssd_key(std::string((const char*)ioresult.key.key, ioresult.key.length))
                   << ", retcode = " << ioresult.retcode << ", FAILED" ;
            }
            break;
        case nvme_cmd_kv_retrieve:
            if (ioresult.retcode == 0) {
                    TRIO << "<READ AIO> " << print_kvssd_key(std::string((const char*)ioresult.key.key, ioresult.key.length))
                    << ", value = " << std::min(event.result, ioctx.value.length) << ", actual = " << (event.result & 0xffff) << ", hash " << ceph_str_hash_linux((const char*)ioresult.value.value, ioresult.value.length);
            } else {
                TR << "<READ AIO> " << print_kvssd_key(std::string((const char*)ioresult.key.key, ioresult.key.length))
                   << ", retcode = " << ioresult.retcode << ", FAILED" ;
            }
            break;

        case nvme_cmd_kv_iter_req:
            {
                std::string type = "ITER_CLOSE";
                if ((ioctx.cmd.cdw4 & ITER_OPTION_OPEN) != 0) {
                    type = "ITER_OPEN";
                }
                if (ioresult.retcode == 0) {
                    TRIO << "<" << type << "> id = " <<  (event.result & 0x000000FF) ;
                } else {
                    TR << "<" << type << "> retcode = " << ioresult.retcode << ", FAILED" ;
                }
            }
            break;

        case nvme_cmd_kv_iter_read:
            if (ioresult.retcode == 0) {
                TRIO << "<ITERREAD> bytesread = " <<  (event.result & 0xffff) ;
            } else {
                TR << "<ITERREAD> retcode = " << ioresult.retcode << ", FAILED" ;
            }

            break;
        case nvme_cmd_kv_batch:
        {
            if (ioresult.retcode == 0) {
                TRIO << "<BATCH> OK " ;
            } else {
                TR << "<BATCH> retcode = " << ioresult.retcode << ", FAILED" ;
            }

        }
            break;
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
