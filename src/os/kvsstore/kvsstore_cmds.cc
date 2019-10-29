#include "kvsstore_cmds.h"
#include "KvsStore.h"
#include "kvs_debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_kvs
#define DEFAULT_READBUF_SIZE 8192


std::tuple<uint8_t, kv_key *, kv_value *> KvsStoreKVCommands::write_sb(bufferlist &bl) {
	kv_key   *key = KvsMemPool::Alloc_key();
	kv_value *val = to_kv_value(bl);
    construct_sb_key(key);
    return std::make_tuple(KEYSPACE_SB, key, val);
}

std::tuple<uint8_t, kv_key *, kv_value *> KvsStoreKVCommands::read_sb() {
	kv_key   *key = KvsMemPool::Alloc_key();
	kv_value *val = KvsMemPool::Alloc_value(DEFAULT_READBUF_SIZE);
    construct_sb_key(key);
    return std::make_tuple(KEYSPACE_SB, key, val);
}

std::tuple<uint8_t, kv_key *, kv_value *> KvsStoreKVCommands::read_onode(const ghobject_t &oid) {
	kv_key   *key = KvsMemPool::Alloc_key();
	kv_value *val = KvsMemPool::Alloc_value(DEFAULT_READBUF_SIZE);

	uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_ONODE_TEMP:KEYSPACE_ONODE;
    key->length = construct_var_onode_key(cct, GROUP_PREFIX_ONODE, oid, key->key, space_id);

    return std::make_tuple(space_id, key, val);
}

std::tuple<uint8_t, kv_key *, kv_value *> KvsStoreKVCommands::read_data(const ghobject_t &oid){
	kv_key   *key = KvsMemPool::Alloc_key();
	kv_value *val = KvsMemPool::Alloc_value(DEFAULT_READBUF_SIZE);

    key->length = construct_var_object_key(cct, GROUP_PREFIX_DATA, oid, key->key, KEYSPACE_DATA);
    return std::make_tuple(KEYSPACE_DATA, key, val);
}

std::tuple<uint8_t, kv_key *, kv_value *> KvsStoreKVCommands::read_coll(const char *name, const int namelen) {
	assert_keylength("collection name is too long", namelen, COLL_NAME_MAX_SIZE);

	kv_key   *key = KvsMemPool::Alloc_key();
	kv_value *val = KvsMemPool::Alloc_value(DEFAULT_READBUF_SIZE);
	key->length = construct_collkey_impl(key->key, name, namelen);
    return std::make_tuple(KEYSPACE_COLLECTION, key, val);
}

std::tuple<uint8_t, kv_key *, kv_value *> KvsStoreKVCommands::read_omap(const ghobject_t& oid, uint64_t index, const std::string &strkey) {
	const uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_OMAP_TEMP:KEYSPACE_OMAP;
	assert_keylength("omap name is too long", strkey.length(), OMAP_KEY_MAX_SIZE);

	kv_key   *key = KvsMemPool::Alloc_key();
	kv_value *val = KvsMemPool::Alloc_value(DEFAULT_READBUF_SIZE);
	key->length = construct_omap_key(cct, index, strkey.c_str(), strkey.length(), key->key, space_id);

	return std::make_tuple(space_id, key, val);
}


void KvsStoreKVCommands::add_prefetch(KvsPrefetchContext *ctx, KvsCollection *c, const ghobject_t &oid)
{
	ctx->onode = new KvsOnode(c, oid);
    ctx->command = std::move(read_onode(oid));
    c->add_to_prefetch_map(oid, ctx->onode);
}

void KvsStoreKVCommands::add_coll(KvsIoContext *ctx, const coll_t &cid, bufferlist &bl) {
	const char *cidkey_str = cid.c_str();
	const int   cidkey_len = (int)strlen(cidkey_str);
	assert_keylength("collection name is too long", cidkey_len, COLL_NAME_MAX_SIZE);

	queue_store_kv(ctx, KEYSPACE_COLLECTION, -1, bl, [&] (void *buffer)->uint8_t {
	return construct_collkey_impl(buffer, cidkey_str, cidkey_len);
	});
}

void KvsStoreKVCommands::rm_coll(KvsIoContext *ctx,const coll_t &cid) {
    const char *cidkey_str = cid.c_str();
    const int   cidkey_len = (int)strlen(cidkey_str);
    assert_keylength("collection name is too long", cidkey_len, COLL_NAME_MAX_SIZE);

    queue_delete_kv(ctx, KEYSPACE_COLLECTION,  [&] (void *buffer)->uint8_t {
		return construct_collkey_impl(buffer, cidkey_str, cidkey_len);
	});
}

void KvsStoreKVCommands::add_onode(KvsIoContext *ctx,const ghobject_t &oid, bufferlist &bl) {
	const uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_ONODE_TEMP:KEYSPACE_ONODE;

	queue_store_kv(ctx, space_id, GROUP_PREFIX_JOURNAL_ONODE, bl, [&] (void *buffer)->uint8_t {
		return construct_var_onode_key(cct, GROUP_PREFIX_ONODE, oid, buffer, space_id);
	});
}

void KvsStoreKVCommands::rm_onode(KvsIoContext *ctx,const ghobject_t& oid){
	const uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_ONODE_TEMP:KEYSPACE_ONODE;

    queue_delete_kv(ctx, space_id, [&] (void *buffer)->uint8_t {
		return construct_var_onode_key(cct, GROUP_PREFIX_ONODE, oid, buffer, space_id);
	});
}

void KvsStoreKVCommands::add_userdata(KvsIoContext *ctx,const ghobject_t& oid, bufferlist &bl){
	return add_userdata(ctx, oid, bl.c_str(), bl.length());
}

void KvsStoreKVCommands::add_userdata(KvsIoContext *ctx,const ghobject_t& oid, char *data, int length){
	queue_store_kv(ctx, KEYSPACE_DATA, -1, data, length, true, [&] (void *buffer)->uint8_t {
		return construct_var_object_key(cct, GROUP_PREFIX_DATA, oid, buffer, KEYSPACE_DATA);
	});
}

void KvsStoreKVCommands::rm_data(KvsIoContext *ctx,const ghobject_t& oid){
	queue_delete_kv(ctx, KEYSPACE_DATA, [&] (void *buffer)->uint8_t {
		return construct_var_object_key(cct, GROUP_PREFIX_DATA, oid, buffer, KEYSPACE_DATA);
	});
}

void KvsStoreKVCommands::add_omap(KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, const std::string &strkey, bufferlist &bl){
	const uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_OMAP_TEMP:KEYSPACE_OMAP;
	assert_keylength("omap name is too long", strkey.length(), OMAP_KEY_MAX_SIZE);

	queue_store_kv(ctx, space_id, GROUP_PREFIX_JOURNAL_OMAP, bl, [&] (void *buffer)->uint8_t {
		return construct_omap_key(cct, index, strkey.c_str(), strkey.length(), buffer, space_id);
	});
}



void KvsStoreKVCommands::rm_omap (KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, const std::string &strkey){
	const uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_OMAP_TEMP:KEYSPACE_OMAP;
	assert_keylength("omap name is too long", strkey.length(), OMAP_KEY_MAX_SIZE);

	queue_delete_kv(ctx, space_id, [&] (void *buffer)->uint8_t {
		return construct_omap_key(cct, index, strkey.c_str(), strkey.length(), buffer, space_id);
	});
}

void KvsStoreKVCommands::add_omapheader(KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, bufferlist &bl){
	const uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_OMAP_TEMP:KEYSPACE_OMAP;

	queue_store_kv(ctx, space_id, GROUP_PREFIX_JOURNAL_OMAP, bl, [&] (void *buffer)->uint8_t {
		return construct_omap_key(cct, index, 0, 0, buffer, space_id);
	});

}

void KvsStoreKVCommands::assert_keylength(const char *str, const int len, const int max) {
	if (len >= max) {
		std::cerr << str << ": len = " << len << ", max = " << max << std::endl;
		ceph_abort();
	}
}

void KvsStoreKVCommands::omap_iterator_init(CephContext *cct, uint64_t lid, const ghobject_t &oid, kv_iter_context *iter_ctx) {

	kvs_omap_key_header hdr = { GROUP_PREFIX_OMAP, lid};
    iter_ctx->prefix =  ceph_str_hash_linux((char*)&hdr, sizeof(struct kvs_omap_key_header));
    iter_ctx->bitmask = 0xFFFFFFFF;
    iter_ctx->buflen = ITER_BUFSIZE;
    iter_ctx->spaceid = (oid.hobj.is_temp())? KEYSPACE_OMAP_TEMP:KEYSPACE_OMAP;
}



