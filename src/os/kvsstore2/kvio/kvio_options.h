/*
 * kvsstore_options.h
 *
 *  Created on: Nov 18, 2019
 *      Author: root
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_OPTIONS_H_
#define SRC_OS_KVSSTORE_KVSSTORE_OPTIONS_H_

#include <string>
typedef unsigned __int128 uint128_t;

//# BATCH
//-------

#define DISABLE_BATCH true

//# KEYSPACE
//  0 - enable indexing, 1 - disable indexing
//--------------------------

#define KEYSPACE_ONODE 		0
#define KEYSPACE_ONODE_TEMP 0
#define KEYSPACE_COLLECTION 0
#define KEYSPACE_OMAP 		1
#define KEYSPACE_OMAP_TEMP 	1
#define KEYSPACE_DATA 		1
#define KEYSPACE_SB 		1
#define KEYSPACE_JOURNAL	1


//# GROUP
//--------------------------

#define GROUP_PREFIX_ONODE    0x0
#define GROUP_PREFIX_COLL  	  0x1
#define GROUP_PREFIX_DATA  	  0x2
#define GROUP_PREFIX_JOURNAL  0x3
#define GROUP_PREFIX_OMAP     0x4

inline std::string prefix_to_str(uint32_t prefix) {
    static const char *strs[] = { "ONODE", "COLL", "DATA", "JOURNAL", "OMAP" };
    if (prefix < 5)
        return strs[prefix];
    else
        return "DATA";
}


//# SIZE
//--------------------------

#define KVS_OBJECT_MAX_SIZE 		2*1024*1024UL
#define KVSSTORE_POOL_VALUE_SIZE 	8192
#define DEFAULT_READBUF_SIZE 		8192
#define KVS_OBJECT_SPLIT_SIZE       DEFAULT_READBUF_SIZE
#define KVS_OBJECT_SPLIT_SHIFT      13
#define KVKEY_MAX_SIZE				255
#define OMAP_KEY_MAX_SIZE 			241
#define MAX_BATCH_VALUE_SIZE 		8192

//# CACHE
//-------------------------
#define ONODE_LRUCACHE_TRIM_MAX_SKIP_PINNED 128
#define KVS_CACHE_MAX_ONODES (3*1024*1024*1024UL / 4096)
#define KVS_CACHE_MAX_DATA_SIZE 1*1024*1024*1024

#endif /* SRC_OS_KVSSTORE_KVSSTORE_OPTIONS_H_ */
