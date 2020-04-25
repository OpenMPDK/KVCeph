//
// Created by root on 3/12/20.
//

#ifndef CEPH_OS_ADAPTER_H
#define CEPH_OS_ADAPTER_H

#include <unistd.h>
#include <memory.h>

#include <atomic>
#include <mutex>
#include <vector>
#include <map>
#include <condition_variable>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/range.hpp>
#include <boost/range/join.hpp>

#include "kadi/kadi_bptree.h"
#include "kvsstore_types.h"
#include "kvsstore_db.h"

#include "include/ceph_assert.h"
#include "include/unordered_map.h"
#include "include/mempool.h"

#include "common/Finisher.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "os/ObjectStore.h"
#include "common/perf_counters.h"
#include "os/fs/FS.h"

class ObjectStoreAdapter: public ObjectStore, public KvsStoreTypes {
    int path_fd = -1;  ///< open handle to $path
    int fsid_fd = -1;  ///< open handle (locked) to $path/fsid
    uuid_d fsid;

protected:
    ObjectStoreAdapter(CephContext *cct, const std::string &path):
        ObjectStore(cct, path) {}

    PerfCounters *logger = nullptr;
    bool mounted = false;

public:
    //
    // abstract functions
    //

    virtual int read_sb() = 0;
    virtual int write_sb() = 0;

    virtual int open_db(bool create) = 0;
    virtual void close_db() = 0;

    virtual int mkfs_kvsstore() = 0;
    virtual int mount_kvsstore() = 0;
    virtual int umount_kvsstore() = 0;

    //virtual int flush_cache() = 0;
    virtual void osr_drain_all() = 0;

    virtual int open_collections() = 0;
    virtual void reap_collections() = 0;
    virtual int fsck_impl() = 0;
    virtual int flush_cache_impl(bool collmap_clear = false) = 0;
    virtual int fiemap_impl(CollectionHandle &c_, const ghobject_t &oid, uint64_t offset, size_t len, map<uint64_t, uint64_t> &destmap)  = 0;

    virtual CollectionRef get_collection(const coll_t& cid) = 0;
    virtual int omap_get_impl( Collection *c, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out) = 0;

public:
    //
    // Objectstore Adapter provides the implementations of the following functions
    // utility functions

    int _open_fsid(bool create);
    int _read_fsid(uuid_d *uuid);
    int _write_fsid();
    void _close_fsid();
    int _lock_fsid();
    int _open_path();
    void _close_path();
    int _fsck_with_mount();

    // ObjectStore interface

    int mount() override;
    int umount() override;
    int mkfs() override;
    bool test_mount_in_use() override;
    int fiemap(CollectionHandle &c_, const ghobject_t &oid, uint64_t offset, size_t length, bufferlist &bl) override;
    int fiemap(CollectionHandle &c_, const ghobject_t &oid, uint64_t offset, size_t length, map<uint64_t, uint64_t> &destmap) override;
    int collection_empty(CollectionHandle &ch, bool *empty) override;
    int collection_bits(CollectionHandle &ch) override;
    int set_alloc_hint( TransContext *txc, CollectionRef& c, OnodeRef& o, uint64_t expected_object_size,uint64_t expected_write_size, uint32_t flags) { return 0; }
    string get_type() override { return "kvsstore"; }
    bool needs_journal() override { return false; }
    bool wants_journal() override { return false; }
    bool allows_journal() override { return false;}
    bool is_rotational() override { return false; }
    bool is_journal_rotational() override { return false; }
    string get_default_device_class() override { return "ssd"; }
    int validate_hobject_key(const hobject_t &obj) const override { return 0; }
    unsigned get_max_attr_name_length() override { return 256; }
    int mkjournal() override { return 0;   }
    objectstore_perf_stat_t get_cur_stats() override { return objectstore_perf_stat_t(); }
    void set_fsid(uuid_d u) override { fsid = u;    }
    uuid_d get_fsid() override { return fsid;    }
    const PerfCounters* get_perf_counters() const override { return logger; }
    int fsck(bool deep) override { return _fsck_with_mount();    }
    int repair(bool deep) override { return _fsck_with_mount();    }
    int flush_cache(ostream *os = NULL) override {  return flush_cache_impl(false); }
    int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf, bool *per_pool_omap) override { return -ENOTSUP; }
    ObjectStore::CollectionHandle open_collection(const coll_t &cid) override { return get_collection(cid);    }
    void set_collection_commit_queue(const coll_t &cid, ContextQueue *commit_queue) override {}
    uint64_t estimate_objects_overhead(uint64_t num_objects) override { return num_objects * 300; }
    int omap_get(CollectionHandle &c_, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out ) override {
        Collection *c = static_cast<Collection *>(c_.get());
        return omap_get_impl(c, oid, header, out);
    }
    void dump_perf_counters(Formatter *f) override {
        f->open_object_section("perf_counters");
        logger->dump_formatted(f, false);
        f->close_section();
    }
};

#endif //CEPH_OS_ADAPTER_H
