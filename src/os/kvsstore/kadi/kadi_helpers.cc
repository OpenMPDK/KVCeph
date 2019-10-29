/*
 * kadi_util.cc
 *
 *  Created on: Jul 25, 2019
 *      Author: ywkang
 */

#include <iostream>
#include <unistd.h>
#include <sys/ioctl.h>

#include "kadi_helpers.h"
#include "kadi_cmds.h"

using namespace std;

#ifndef derr
#define derr std::cerr
#endif

#ifndef dendl
#define dendl std::endl
#endif

///
/// Event Listener
///

#define EPOLL_DEV 1

int ioevent_listener::init(int fd) {
	int efd = eventfd(0,0);
	if (efd < 0) {
		derr << "fail to create an event " << dendl;
		return -1;
	}

	#ifdef EPOLL_DEV
		EpollFD_dev = epoll_create(1024);
		if(EpollFD_dev<0){
			derr << "Unable to create Epoll FD; error = " << EpollFD_dev << dendl;
			return -1;
		}
		watch_events.events = EPOLLIN | EPOLLET;
		watch_events.data.fd = efd;
		int register_event = epoll_ctl(EpollFD_dev, EPOLL_CTL_ADD, efd, &watch_events);
		if (register_event)
			derr << " Failed to add FD = " << efd << ", to epoll FD = " << EpollFD_dev
				<< ", with error code  = " << register_event << dendl;

		#endif

	aioctx.ctxid   = 0;
	aioctx.eventfd = efd;

    if (ioctl(fd, NVME_IOCTL_SET_AIOCTX, &aioctx) < 0) {
        derr <<  "fail to set_aioctx" << dendl;
        return -1;
    }

	return aioctx.ctxid;
}

void ioevent_listener::close(int fd) {
    ioctl(fd, NVME_IOCTL_DEL_AIOCTX, &aioctx);
    ::close((int)aioctx.eventfd);

	#ifdef EPOLL_DEV
		::close(EpollFD_dev);
	#endif
}


int ioevent_listener::poll(uint32_t timeout_us) {
	fd_set rfds;
    FD_ZERO(&rfds);

#ifdef EPOLL_DEV
    int timeout = timeout_us/1000;
    int nr_changed_fds = epoll_wait(EpollFD_dev, list_of_events, 1, timeout);
    if( nr_changed_fds == 0 || nr_changed_fds < 0) { return 0;}
#else
    FD_SET(aioctx.eventfd, &rfds);

    memset(&timeout, 0, sizeof(timeout));
    timeout.tv_usec = timeout_us;

    int nr_changed_fds = select(aioctx.eventfd+1, &rfds, NULL, NULL, &timeout);

    if ( nr_changed_fds == 0 || nr_changed_fds < 0) { return 0; }
#endif

    unsigned long long eftd_ctx = 0;
    int read_s = read(aioctx.eventfd, &eftd_ctx, sizeof(unsigned long long));

    if (read_s != sizeof(unsigned long long)) {
        fprintf(stderr, "failt to read from eventfd ..\n");
        return -1;
    }

    return eftd_ctx;
}

///
/// Iterator buffer reader
///

iterbuf_reader::iterbuf_reader(void *c, void *buf_, int length_):
    cct(c), buf(buf_), bufoffset(0), byteswritten(length_),  numkeys(0)
{

    if (hasnext()) {
        numkeys = *((unsigned int*)buf);
        bufoffset += 4;
    }
}


bool iterbuf_reader::nextkey(void **key, int *length)
{
	if (numkeys == 0) return false;

    int afterKeygap = 0;
    char *current_pos = ((char *)buf) ;

    if (bufoffset + 4 >= byteswritten) return false;

    *length = *((unsigned int*)(current_pos+bufoffset)); bufoffset += 4;

    if (bufoffset + *length > byteswritten) return false;

    *key    = (current_pos+bufoffset);
    afterKeygap = (((*length + 3) >> 2) << 2);
    bufoffset += afterKeygap;

    return true;
}

aio_cmd_ctx* cmd_ctx_manager::get_cmd_ctx(const kv_cb& cb) {
	std::unique_lock<std::mutex> lock (cmdctx_lock);

	while (free_cmdctxs.empty()) {
		derr << "aio cmd queue is empty. wait..." << dendl;
		if (cmdctx_cond.wait_for(lock, std::chrono::seconds(5)) == std::cv_status::timeout) {
			derr << "max queue depth has reached. wait..." << dendl;
		} else {
			derr << "found" << dendl;
		}
	}

	aio_cmd_ctx *p = free_cmdctxs.back();
	free_cmdctxs.pop_back();

	p->post_fn   = cb.post_fn;
	p->post_data = cb.private_data;

	pending_cmdctxs.insert(std::make_pair(p->index, p));
	return p;
}

///
/// Dump
///



void dump_delete_cmd(struct nvme_passthru_kv_cmd *cmd) {
    char buf[2048];
    int offset = sprintf(buf, "[dump delete cmd (%02x)]\n", cmd->opcode);

    offset += sprintf(buf+offset, "\t opcode(%02x)\n", cmd->opcode);
    offset += sprintf(buf+offset, "\t nsid(%04x)\n", cmd->nsid);
    offset += sprintf(buf+offset, "\t cdw3(%04x)\n", cmd->cdw3);
    offset += sprintf(buf+offset, "\t cdw4(%04x)\n", cmd->cdw4);
    offset += sprintf(buf+offset, "\t cdw5(%04x)\n", cmd->cdw5);

    offset += sprintf(buf+offset, "\t cmd.key_length(%02x)\n", cmd->key_length);

    if (cmd->key_length <= KVCMD_INLINE_KEY_MAX) {
        offset += sprintf(buf+offset, "\t cmd.key (%s)\n", print_key((char*)cmd->key, cmd->key_length).c_str());
    }
    else {
        offset += sprintf(buf+offset, "\t cmd.key (%s)\n", print_key((char*)cmd->key_addr, cmd->key_length).c_str());
    }
    offset += sprintf(buf+offset, "\t reqid(%04llu)\n", cmd->reqid);
    offset += sprintf(buf+offset, "\t ctxid(%04d)\n", cmd->ctxid);
    derr << buf << dendl;
}


void dump_retrieve_cmd(struct nvme_passthru_kv_cmd *cmd) {
    char buf[2048];
    int offset = sprintf(buf, "[dump retrieve cmd (%02x)]\n", cmd->opcode);

    offset += sprintf(buf+offset, "\t opcode(%02x)\n", cmd->opcode);
    offset += sprintf(buf+offset, "\t nsid(%04x)\n", cmd->nsid);
    offset += sprintf(buf+offset, "\t cdw3(%04x)\n", cmd->cdw3);
    offset += sprintf(buf+offset, "\t cdw4(%04x)\n", cmd->cdw4);
    offset += sprintf(buf+offset, "\t cdw5(%04x)\n", cmd->cdw5);

    offset += sprintf(buf+offset, "\t cmd.key_length(%02x)\n", cmd->key_length);

    if (cmd->key_length <= KVCMD_INLINE_KEY_MAX) {
        offset += sprintf(buf+offset, "\t cmd.key (%s)\n", print_key((char*)cmd->key, cmd->key_length).c_str());
    }
    else {
        offset += sprintf(buf+offset, "\t cmd.key (%s)\n", print_key((char*)cmd->key_addr, cmd->key_length).c_str());
    }

    offset += sprintf(buf+offset, "\t cmd.data_length(%02x)\n", cmd->data_length);
    offset += sprintf(buf+offset, "\t cmd.data(%p)\n", (void*)cmd->data_addr);
    offset += sprintf(buf+offset, "\t reqid(%04llu)\n", cmd->reqid);
    offset += sprintf(buf+offset, "\t ctxid(%04d)\n", cmd->ctxid);
    derr << buf << dendl;
}


void dump_cmd(struct nvme_passthru_kv_cmd *cmd)
{
    char buf[2048];
    int offset = sprintf(buf, "[dump issued cmd opcode (%02x)]\n", cmd->opcode);
    offset += sprintf(buf+offset, "\t opcode(%02x)\n", cmd->opcode);
    offset += sprintf(buf+offset, "\t flags(%02x)\n", cmd->flags);
    offset += sprintf(buf+offset, "\t rsvd1(%04d)\n", cmd->rsvd1);
    offset += sprintf(buf+offset, "\t nsid(%08x)\n", cmd->nsid);
    offset += sprintf(buf+offset, "\t cdw2(%08x)\n", cmd->cdw2);
    offset += sprintf(buf+offset, "\t cdw3(%08x)\n", cmd->cdw3);
    offset += sprintf(buf+offset, "\t rsvd2(%08x)\n", cmd->cdw4);
    offset += sprintf(buf+offset, "\t cdw5(%08x)\n", cmd->cdw5);
    offset += sprintf(buf+offset, "\t data_addr(%p)\n",(void *)cmd->data_addr);
    offset += sprintf(buf+offset, "\t data_length(%08x)\n", cmd->data_length);
    offset += sprintf(buf+offset, "\t key_length(%08x)\n", cmd->key_length);
    offset += sprintf(buf+offset, "\t cdw10(%08x)\n", cmd->cdw10);
    offset += sprintf(buf+offset, "\t cdw11(%08x)\n", cmd->cdw11);
    offset += sprintf(buf+offset, "\t cdw12(%08x)\n", cmd->cdw12);
    offset += sprintf(buf+offset, "\t cdw13(%08x)\n", cmd->cdw13);
    offset += sprintf(buf+offset, "\t cdw14(%08x)\n", cmd->cdw14);
    offset += sprintf(buf+offset, "\t cdw15(%08x)\n", cmd->cdw15);
    offset += sprintf(buf+offset, "\t timeout_ms(%08x)\n", cmd->timeout_ms);
    offset += sprintf(buf+offset, "\t result(%08x)\n", cmd->result);
    derr << buf << dendl;
}

