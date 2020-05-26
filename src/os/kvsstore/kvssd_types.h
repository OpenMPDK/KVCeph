//
// Created by root on 3/22/20.
//

#ifndef CEPH_KVSSD_H
#define CEPH_KVSSD_H

#include <atomic>
#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include <boost/container/small_vector.hpp>
#include "kadi/kadi_cmds.h"
#include "kadi/kadi_types.h"
#include "include/buffer.h"
#include "kvsstore_debug.h"



///  ====================================================
///  IO Context
///  ====================================================

typedef void (*aio_callback_t)(kv_io_context &op, void *post_data);

struct IoContext;


struct kvaio_t {
    int opcode;
    int spaceid;
    kv_key_t keylength;
    void *value;
    int vallength;
    int valoffset;
    IoContext *parent;
    aio_callback_t cb_func;
    long rval;

    void *db;
    char key[256];
    bufferptr  bp;  ///< read buffer
    bufferlist bl;  ///< write payload (so that it remains stable for duration)
    bufferlist *pbl; /// output bl - will contain bl
    uint64_t caller;
    std::string debug;
    boost::container::small_vector<iovec,4> iov;    // to retrieve an internal address from a bufferlist

    kvaio_t(int opcode_, int spaceid_, aio_callback_t c, IoContext *p, void *db_):
        opcode(opcode_), spaceid(spaceid_), keylength(0), value(0), vallength(0), valoffset(0), parent(p), cb_func(c), rval(-1000), db(db_)
    {
        TR2 << "kvaio_t created: " << (void*)this << ", parent (ioc) = " << (void*)parent;
    }

};

struct IoContext {
private:
    std::mutex lock;
    std::condition_variable cond;
    //ceph::mutex lock = ceph::make_mutex("IOContext::lock");
    //ceph::condition_variable cond;
    int r = 0;
    int keyspace;

    std::list<kvaio_t*> running_aios;    ///< submitting or submitted
    int num_running = 0;
    int num_submitted = 0;

    atomic_bool submitting;
    bool issync;
public:
    void *parent;
    std::string loc;

    //std::list<kvaio_t*> pending_syncios; ///< objects to be synchronously written (no lock contention)
    std::list<kvaio_t*> pending_aios;    ///< not yet submitted



    explicit IoContext(void *parent_, const char *fname)
    : num_running(0), submitting(false), issync(false), parent(parent_), loc(fname) {}

    // no copying
    IoContext(const IoContext& other) = delete;
    IoContext &operator=(const IoContext& other) = delete;
    ~IoContext() {
        release_running_aios();
    }
public:

    bool has_pending_aios() {
        std::unique_lock<std::mutex> l(lock);
        return !pending_aios.empty();
    }

    void aio_wait() {
        std::unique_lock<std::mutex> l(lock);
        if (num_running > 0) {
            cond.wait(l);
        }
    }

    bool _prepare_running_aios() {
        std::unique_lock<std::mutex> l(lock);
        if (!pending_aios.empty()) {
            running_aios.splice(running_aios.begin(), pending_aios);
            num_running = running_aios.size();
            return (num_running > 0);
        }
        return false;
    }

    int _submit_aios(KADI* kadi, bool debug) {
        int r = 0;
        submitting = true;
        for (kvaio_t *aio : running_aios) {
            r = -1;
            switch (aio->opcode) {
                case nvme_cmd_kv_retrieve:
                    //if (debug || 1) TRR << "submit ioc = " << (void*)this << ", op = " << aio->opcode << ", aio key addr = " << (void*)aio->key  << " ," << print_kvssd_key(aio->key, aio->keylength) << ", post data" << (void*)aio << ", ioc = " << (void*)aio->parent;
                    r = kadi->kv_retrieve_aio(aio->spaceid, aio->key, aio->keylength, aio->value, aio->valoffset, aio->vallength, { aio->cb_func,  aio });
                    break;
                case nvme_cmd_kv_delete:
                    //if (debug || 1) TRW << "submit ioc = " << (void*)this << ", op = " << aio->opcode << ", aio key addr = " << (void*)aio->key  << " ," << print_kvssd_key(aio->key, aio->keylength) << ", post data" << (void*)aio << ", ioc = " << (void*)aio->parent;
                    r = kadi->kv_delete_aio(aio->spaceid, aio->key, aio->keylength, { aio->cb_func,  aio });
                    break;
                case nvme_cmd_kv_store:
                    //if (debug || 1) TRW << "submit ioc = " << (void*)this << ", op = " << aio->opcode << ", aio key addr = " << (void*)aio->key  << " ," << print_kvssd_key(aio->key, aio->keylength) << ", post data" << (void*)aio << ", ioc = " << (void*)aio->parent;
                    r = kadi->kv_store_aio(aio->spaceid, aio->key, aio->keylength, aio->value, aio->valoffset, aio->vallength, { aio->cb_func,  aio});
                    break;
            };

            if (r != 0) {
                ceph_abort_msg("IO error in aio_submit");
            }
        }
        submitting = false;
        return r;
    }

    int aio_submit(KADI* kadi, bool _issync = false) {
        issync = _issync;
        if (_prepare_running_aios()) {
            int r= _submit_aios(kadi, false);
            return r;
        }
        return -1;
    }

    int aio_submit_and_wait(KADI* kadi, const char *caller) {
        FTRACE
        int r = aio_submit(kadi, true);
        if (r != 0) {
            return r;
        }

        aio_wait();

        return get_return_value();
    }

    void release_running_aios() {
        FTRACE
        while (submitting.load()) {
            usleep(1);
        }
        {
            std::unique_lock<std::mutex> l(lock);
            for (kvaio_t *p : running_aios) {
                delete p;
            }
            running_aios.clear();
        }
    }

    uint64_t get_num_ios() const { return 0; }

    bool mark_io_complete() {
        FTRACE
        assert(num_running >= 1);

        std::unique_lock<std::mutex> l(lock);
        num_running--;

        if (num_running == 0) {
            if (issync) {
                cond.notify_all();
                return false;
            }
            return true;
        }
        return false;
    }

    void set_return_value(int _r) {
        r = _r;
    }

    int get_return_value() const {
        return r;
    }
};



#endif //CEPH_KVSSD_H

