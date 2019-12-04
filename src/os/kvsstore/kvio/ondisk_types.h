//
// Created by root on 10/12/18.
//

#ifndef CEPH_KVSSTORE_ONDISK_H
#define CEPH_KVSSTORE_ONDISK_H

#include <sstream>
#include <cstdlib>
#include <string>
#include <cstdint>
#include <algorithm>
#include <map>
#include <ostream>

#include "include/ceph_assert.h"
#include "include/unordered_map.h"
//#include "include/memory.h"
#include "include/mempool.h"
#include <memory.h>
#include "common/Finisher.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "os/ObjectStore.h"
#include "os/fs/FS.h"
#include "kvio_options.h"


/// superblock
struct kvsstore_sb_t {
    uint64_t lid_last;
    uint64_t is_uptodate;

    explicit kvsstore_sb_t(): lid_last(0), is_uptodate(0) {}

    DENC(kvsstore_sb_t, v, p) {
        DENC_START(1, 1, p);
            denc(v.lid_last, p);
            denc(v.is_uptodate, p);
        DENC_FINISH(p);
    }
    void dump(Formatter *f) const{
        f->dump_unsigned("lid_last", lid_last);
        f->dump_unsigned("is_uptodate", is_uptodate);
    }
    static void generate_test_instances(list<kvsstore_sb_t*>& o){}

};
WRITE_CLASS_DENC(kvsstore_sb_t)

/// collection metadata
struct kvsstore_cnode_t {
    uint32_t bits;   ///< how many bits of coll pgid are significant

    explicit kvsstore_cnode_t(int b=0) : bits(b) {}

    DENC(kvsstore_cnode_t, v, p) {
        DENC_START(1, 1, p);
            denc(v.bits, p);
        DENC_FINISH(p);
    }
    void dump(Formatter *f) const{f->dump_unsigned("bits", bits);}
    static void generate_test_instances(list<kvsstore_cnode_t*>& o){}

};
WRITE_CLASS_DENC(kvsstore_cnode_t)


/// onode: per-object metadata
struct kvsstore_onode_t {
    uint64_t lid = 0;
    uint64_t size = 0;                   ///< object size
    bufferlist omap_header;
    map<mempool::kvsstore_cache_other::string, bufferptr>  attrs;        ///< attrs
    set<string>             omaps;

    template<typename Functor>
    void fill_omap_values(map<string, bufferlist> *out, Functor &&read_omap_value) {
    	for (auto &p: omaps) {
    		if (read_omap_value(p, (*out)[p]) != 0) {
    			throw "fill_omap_values: key is not found";
    		}
    	}
    	return;
    }

    bool has_omap() const {

    	return omaps.size() > 0;
    }

    DENC(kvsstore_onode_t, v, p) {
        DENC_START(1, 1, p);
            denc_varint(v.lid, p);
            denc_varint(v.size, p);
            denc(v.attrs, p);
            denc(v.omaps, p);
            denc(v.omap_header, p);
            //denc(v.flags, p);
        DENC_FINISH(p);
    }

    void dump(Formatter *f) const {}
    static void generate_test_instances(list<kvsstore_onode_t*>& o) {}
};
WRITE_CLASS_DENC(kvsstore_onode_t)




#endif //CEPH_KVSSTORE_ONDISK_H
