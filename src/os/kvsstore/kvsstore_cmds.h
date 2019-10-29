/*
 * kvsstore_cmds.h
 *
 *  Created on: Aug 13, 2019
 *      Author: root
 */

#ifndef OS_KVSSTORE_KVSSTORE_CMDS_H_
#define OS_KVSSTORE_KVSSTORE_CMDS_H_

#include "kvsstore_types.h"
#include "kadi/kadi_cmds.h"
#include "kvsstore_keyencoder.h"

class KvsStoreKVCommands
{
	CephContext *cct;


public:
	KvsStoreKVCommands(CephContext *cct_): cct(cct_) {}
	std::tuple<uint8_t, kv_key *, kv_value *> write_sb(bufferlist &bl);
    std::tuple<uint8_t, kv_key *, kv_value *> read_sb();
    std::tuple<uint8_t, kv_key *, kv_value *> read_onode(const ghobject_t &oid);
    std::tuple<uint8_t, kv_key *, kv_value *> read_data(const ghobject_t &oid);
    std::tuple<uint8_t, kv_key *, kv_value *> read_coll(const char *name, const int namelen);
    std::tuple<uint8_t, kv_key *, kv_value *> read_omap(const ghobject_t& oid, uint64_t index, const std::string &strkey);

    void add_prefetch(KvsPrefetchContext *ctx, KvsCollection *c, const ghobject_t &oid);

    void add_coll(KvsIoContext *ctx, const coll_t &cid, bufferlist &bl);
    void rm_coll(KvsIoContext *ctx,const coll_t &cid);

    void add_onode(KvsIoContext *ctx,const ghobject_t &oid, bufferlist &bl);
    void rm_onode(KvsIoContext *ctx,const ghobject_t& oid);
    void add_userdata(KvsIoContext *ctx,const ghobject_t& oid, bufferlist &bl);
    void add_userdata(KvsIoContext *ctx,const ghobject_t& oid, char *data, int length);
    void rm_data(KvsIoContext *ctx,const ghobject_t& oid);

    void add_omap(KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, const std::string &strkey, bufferlist &bl);
    void rm_omap (KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, const std::string &strkey);
    void add_omapheader(KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, bufferlist &bl);

    void assert_keylength(const char *str, const int len, const int max);


    void omap_iterator_init(CephContext *cct, uint64_t lid, const ghobject_t &oid, kv_iter_context *iter_ctx);

    inline kv_value *to_kv_value(const char *data, const int length, const bool avoidcopy, CephContext * cct = 0) {
        kv_value *value = KvsMemPool::Alloc_value(length, !avoidcopy);
        if (avoidcopy) {
            value->value = (void*)data;
            value->length = length;
        }
        else {
            memcpy((void*)value->value, data, length);

        }
        return value;
    }

    inline kv_value *to_kv_value(bufferlist &bl, bool avoidcopy = false, CephContext * cct = 0) {
        return to_kv_value(bl.c_str(), bl.length(), avoidcopy, cct);
    }

    template<typename KeyGen>
    inline void queue_store_kv(KvsIoContext *ctx, const int space_id, const int journal_spaceid, const char* data, const int datalength, const bool persistent, const KeyGen& keygen) {
        kv_key *key = KvsMemPool::Alloc_key();
        key->length = keygen(key->key);
        ctx->add(space_id , key, to_kv_value(data, datalength, persistent)); // KEYSPACE_ONODE

       if (persistent || datalength > MAX_BATCH_VALUE_SIZE) {
    		kv_key *key = KvsMemPool::Alloc_key();
    		key->length = keygen(key->key);
    		ctx->add(space_id , key, to_kv_value(data, datalength, persistent)); // KEYSPACE_ONODE

       } else {
    	   ctx->batchctx.batch_store(space_id, 0, data, datalength, [&] (void *buffer)->uint8_t {
    			return keygen(buffer);
    		});

    		if (journal_spaceid != -1) {
    			ctx->journal.batch_store(KEYSPACE_JOURNAL, 0, data, datalength, [&] (void *buffer)->uint8_t {
    				int length = keygen(buffer);
    				((struct kvs_var_object_key*)buffer)->group = journal_spaceid; //GROUP_PREFIX_JOURNAL_ONODE;
    				return length;
    			});
    		}
       }
    }

    template<typename KeyGen>
    inline void queue_store_kv(KvsIoContext *ctx, const int space_id, const int journal_spaceid, bufferlist &bl, const KeyGen& keygen) {
    	queue_store_kv(ctx, space_id, journal_spaceid, bl.c_str(), bl.length(), false, keygen);
    }

    template<typename KeyGen>
    inline void queue_delete_kv(KvsIoContext *ctx, const int space_id, const KeyGen& keygen) {
        kv_key *key = KvsMemPool::Alloc_key();
        key->length = keygen(key->key);
        ctx->del(space_id, key);
    }

};




#endif /* OS_KVSSTORE_KVSSTORE_CMDS_H_ */
