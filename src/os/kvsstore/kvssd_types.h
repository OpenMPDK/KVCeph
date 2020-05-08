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

public:
    void *parent;
    std::string loc;

    std::mutex running_aio_lock;
    std::list<kvaio_t*> pending_syncios; ///< objects to be synchronously written (no lock contention)
    std::list<kvaio_t*> pending_aios;    ///< not yet submitted
    std::list<kvaio_t*> running_aios;    ///< submitting or submitted

    std::atomic_int num_pending = {0};
    std::atomic_int num_running = {0};

    explicit IoContext(void *parent_, const char *fname)
    : parent(parent_), loc(fname) {
        add_live_object(this);
    }

    // no copying
    IoContext(const IoContext& other) = delete;
    IoContext &operator=(const IoContext& other) = delete;
    ~IoContext() {
        //TR2 << "IOContext destroyed: ptr = " << (void*)this;
        //std::ostringstream oss; oss << BackTrace(1);

        remove_live_object(this, loc);
    }
public:

    bool has_pending_aios() {
        return num_pending.load();
    }

    void aio_wait() {
        std::unique_lock l(lock);

        //TR << "Pending IOs :" << num_running.load();
        while (num_running.load() > 0) {
            cond.wait(l);
        }
        //TR << "IO Finished";
    }

    void release_running_aios() {
        std::unique_lock<std::mutex> lck(running_aio_lock);
        for (const kvaio_t *p : running_aios) {
            delete p;
        }
        running_aios.clear();
    }
    uint64_t get_num_ios() const { return 0; }

    bool try_aio_wake() {
        assert(num_running >= 1);

        std::lock_guard l(lock);
        if (num_running.fetch_sub(1) == 1) {

            // we might have some pending IOs submitted after the check
            // as there is no lock protection for aio_submit.
            // Hence we might have false conditional trigger.
            // aio_wait has to handle that hence do not care here.
            cond.notify_all();
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

