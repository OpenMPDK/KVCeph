#include "indexer_hint.h"

#include <stdint.h>
#include "kvsstore_db.h"
#include "kvsstore_debug.h"
#include "kvio/kvio_options.h"
#include "kvio/kadi/kadi_types.h"
#include "kvsstore_iterator.h"
#include "KvsStore.h"

// set up dout_context and dout_prefix here
// -----------------------------------------
#define dout_context cct
#define dout_subsys ceph_subsys_kvs
#undef dout_prefix
#define dout_prefix *_dout << "[kvsstore] "



static inline int make_align_4B(int size) {	return ((size -1) / 4 + 1); }

static inline void assert_keylength(const int len) {
	if (len > KVKEY_MAX_SIZE) {
		std::cerr << "key is too long, len = " << len << std::endl;
		std::cerr <<  BackTrace(1) << std::endl;
		ceph_abort();
	}
}

// -------------------------------------
//  Conversion: Bufferlist -> kv_value
// -------------------------------------

// for reads
static inline void to_kv_value(kv_value *value, const int offset, const int length, bufferlist &bl) {
	bl.clear();
	value->offset = offset;
	value->length = make_align_4B(length);
	bl.reserve(value->length);
	value->value  = bl.c_str();
}

static inline void to_kv_value(kv_value *value, const int offset, const int length, char *data) {
	value->offset = offset;
	value->length = length;
	value->value  = data;
}
inline void set_kv_value(kv_value *value, char *data, const int len) {
	value->value  = data;
	value->length = len;
	value->offset = 0;
}

inline void set_kv_value(kv_value *value, bufferlist *bl) {
	value->offset = 0;
	value->length = bl->length();
	value->value  = (void*)bl->c_str();
}

int KvsStoreDB::_async_write_impl(uint8_t space_id, char *data, int len, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill, const kv_cb &cb) {
	kv_value value;
	value.length = len;
	value.value = data;
	value.offset = 0;

	return kadi.async_write(space_id, &value, cb, fill);
}

bool KvsStoreDB::rm_journal(int index) {
	return kadi.sync_delete(KEYSPACE_JOURNAL, [index] (struct nvme_passthru_kv_cmd& cmd) {
		cmd.key_length = construct_journalkey_impl(cmd.key, index);
	});
}

bool KvsStoreDB::read_journal(int index, kv_value *value) {
	bool ispartial;
	return (0 == kadi.sync_read(KEYSPACE_JOURNAL, value, ispartial, [index] (struct nvme_passthru_kv_cmd& cmd) {
		cmd.key_length = construct_journalkey_impl(cmd.key, index);
	}));
}



// -------------------------------------------------
//  Synchronous Ceph Object IO
// -------------------------------------------------


int KvsStoreDB::_read_impl(uint8_t space_id, int offset, int length, bufferlist &bl, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill, bool &ispartial, bool retry) {

	kv_value value;
	to_kv_value(&value, offset, length, bl);

	int ret = kadi.sync_read(space_id, &value, ispartial, fill);

	if (retry && ret ==0 && value.offset == 0 && value.actual_value_size > (unsigned)length) {
		// if buffer size was small, retry with the actual value size
		to_kv_value(&value, 0, value.actual_value_size, bl);
		ret = kadi.sync_read(space_id, &value, ispartial, fill);
	}

	return ret;
}

int KvsStoreDB::_read_impl(uint8_t space_id, bufferlist &bl, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill, bool &ispartial, bool retry) {
	return _read_impl(space_id, 0, DEFAULT_READBUF_SIZE, bl, fill, ispartial, retry );
}


int KvsStoreDB::write_sb(bufferlist &bl) {
	kv_value value;
	set_kv_value(&value, &bl);

	return kadi.sync_write(KEYSPACE_SB, &value, [&] (struct nvme_passthru_kv_cmd &cmd) {
	    cmd.key_length = construct_kvsbkey_impl(cmd.key);
	});
}

int KvsStoreDB::read_sb(bufferlist &bl) {
	bool ispartial;
	return _read_impl(KEYSPACE_SB, bl, [&] (struct nvme_passthru_kv_cmd &cmd) {
		cmd.key_length = construct_kvsbkey_impl(cmd.key);
	}, ispartial);
}


int KvsStoreDB::read_onode(const ghobject_t &oid, bufferlist &bl) {
	bool ispartial;
	auto keymem = make_malloc_unique<char>(256);
	return _read_impl(KEYSPACE_ONODE, bl, [&] (struct nvme_passthru_kv_cmd &cmd) {
		void *long_keyaddr = keymem.get();
		cmd.key_addr = (__u64)long_keyaddr;
		cmd.key_length = construct_onode_key(cct, oid, long_keyaddr);
	},ispartial);
}

int KvsStoreDB::read_data(const ghobject_t &oid, int offset, int length, bufferlist &bl, bool &ispartial) {
	auto keymem = make_malloc_unique<char>(256);

	return _read_impl(KEYSPACE_DATA, offset, length, bl, [&] (struct nvme_passthru_kv_cmd &cmd) {
		void *long_keyaddr = keymem.get();
		cmd.key_addr = (__u64)long_keyaddr;
		cmd.key_length = construct_object_key(cct, oid, long_keyaddr);
	}, ispartial, true /* retry */);
}

int KvsStoreDB::read_data(const ghobject_t &oid, bufferlist &bl, bool &ispartial) {
	return read_data(oid, 0, DEFAULT_READBUF_SIZE, bl, ispartial);
}

int KvsStoreDB::read_block(const ghobject_t &oid, const int blockindex, bufferlist &bl, uint32_t &nread) {
	bool ispartial;
	auto keymem = make_malloc_unique<char>(256);

	kv_value value;
	to_kv_value(&value, 0, KVS_OBJECT_SPLIT_SIZE, bl);

	int ret = kadi.sync_read(KEYSPACE_DATA, &value, ispartial, [&] (struct nvme_passthru_kv_cmd &cmd) {
		void *long_keyaddr = keymem.get();
		cmd.key_addr = (__u64)long_keyaddr;
		cmd.key_length = construct_object_key(cct, oid, long_keyaddr, blockindex);
	});

	if (ret == 0)
		nread = value.length;

	return ret;
}


int KvsStoreDB::read_block(const ghobject_t &oid, const int blockindex, char *data, uint32_t &nread) {
	bool ispartial;
	auto keymem = make_malloc_unique<char>(256);

	kv_value value;
	to_kv_value(&value, 0, KVS_OBJECT_SPLIT_SIZE, data);

	int ret = kadi.sync_read(KEYSPACE_DATA, &value, ispartial, [&] (struct nvme_passthru_kv_cmd &cmd) {
		void *long_keyaddr = keymem.get();
		cmd.key_addr = (__u64)long_keyaddr;
		cmd.key_length = construct_object_key(cct, oid, long_keyaddr, blockindex);
	});

	if (ret == 0)
		nread = value.length;

	return ret;
}

int KvsStoreDB::read_coll(const char *name, const int namelen, bufferlist &bl) {
	bool ispartial;
	const int coll_keylength = calculate_collkey_length(namelen);
	if (coll_keylength > KVCMD_INLINE_KEY_MAX) {
		auto keymem = make_malloc_unique<char>(256);
		return _read_impl(KEYSPACE_COLLECTION, bl, [&] (struct nvme_passthru_kv_cmd &cmd) {
			void *long_keyaddr = keymem.get();
			cmd.key_addr = (__u64)long_keyaddr;
			cmd.key_length = construct_collkey_impl(long_keyaddr, name, namelen);
		}, ispartial);
	} else {
		return _read_impl(KEYSPACE_COLLECTION, bl, [&] (struct nvme_passthru_kv_cmd &cmd) {
			cmd.key_length = construct_collkey_impl(cmd.key, name, namelen);
		}, ispartial);
	}
}

int KvsStoreDB::read_omap(const ghobject_t& oid, const uint64_t index, const std::string &strkey, bufferlist &bl) {
	bool ispartial;
	const int omap_keylength = calculate_omapkey_length(strkey.length());
	if (omap_keylength > KVCMD_INLINE_KEY_MAX) {
		auto keymem = make_malloc_unique<char>(256);
		return _read_impl(KEYSPACE_OMAP, bl, [&] (struct nvme_passthru_kv_cmd &cmd) {
			void *long_keyaddr = keymem.get();
			cmd.key_addr = (__u64)long_keyaddr;
			cmd.key_length = construct_omapkey_impl(long_keyaddr, index, strkey.c_str(), strkey.length(), KEYSPACE_OMAP);
		}, ispartial, true /* retry */);
	} else {
		return _read_impl(KEYSPACE_OMAP, bl, [&] (struct nvme_passthru_kv_cmd &cmd) {
			cmd.key_length = construct_omapkey_impl(cmd.key, index, strkey.c_str(), strkey.length(), KEYSPACE_OMAP);
		}, ispartial, true /* retry */);
	}
}

// -------------------------------------------------
//  Asynchronous Transaction I/O (write / delete)
// -------------------------------------------------


void KvsStoreDB::add_coll(KvsIoContext *ctx, const coll_t &cid, bufferlist &bl) {
	const char *cidkey_str = cid.c_str();
	const int   cidkey_len = (int)strlen(cidkey_str);
	assert_keylength(sizeof(kvs_coll_key) + cidkey_len );
	auto keyfunc = [&] (void *buffer)->uint8_t {
		return construct_collkey_impl(buffer, cidkey_str, cidkey_len);
	};
	ctx->add_to_journal(KEYSPACE_COLLECTION, KVS_JOURNAL_ENTRY_COLL, &bl, keyfunc);
	ctx->add_pending_meta(KEYSPACE_COLLECTION, bl, keyfunc);
}

void KvsStoreDB::rm_coll(KvsIoContext *ctx, const coll_t &cid) {
	const char *cidkey_str = cid.c_str();
	const int   cidkey_len = (int)strlen(cidkey_str);

	assert_keylength(sizeof(kvs_coll_key) + cidkey_len );

	auto keyfunc = [&] (void *buffer)->uint8_t {
			return construct_collkey_impl(buffer, cidkey_str, cidkey_len);
	};

	ctx->add_to_journal(KEYSPACE_COLLECTION, KVS_JOURNAL_ENTRY_COLL, 0, keyfunc);

	ctx->add_pending_remove(KEYSPACE_COLLECTION, keyfunc);
}


void KvsStoreDB::add_onode(KvsIoContext *ctx,const ghobject_t &oid, bufferlist &bl) {
	const uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_ONODE_TEMP:KEYSPACE_ONODE;
	const auto keygen = [&] (void *buffer)->uint8_t {
		return construct_onode_key(cct, oid, buffer);
	};

	ctx->add_to_journal(space_id, KVS_JOURNAL_ENTRY_ONODE, &bl, keygen);

	ctx->add_pending_meta(space_id, bl, keygen);
}

void KvsStoreDB::rm_onode(KvsIoContext *ctx,const ghobject_t& oid){
	const uint8_t space_id = (oid.hobj.is_temp())? KEYSPACE_ONODE_TEMP:KEYSPACE_ONODE;
	const auto keygen = [&] (void *buffer)->uint8_t {
		return construct_onode_key(cct, oid, buffer);
	};

	ctx->add_to_journal(space_id, KVS_JOURNAL_ENTRY_ONODE, 0, keygen);
	ctx->add_pending_remove(space_id, keygen);
}

void KvsStoreDB::add_userdata(KvsIoContext *ctx,const ghobject_t& oid, char *page, int length, int pageid){
	ctx->add_pending_data(KEYSPACE_DATA, page, length, [&] (void *buffer)->uint8_t {
		return construct_object_key(cct, oid, buffer, pageid);
	});
}

void KvsStoreDB::rm_data(KvsIoContext *ctx,const ghobject_t& oid, int blockid){
	ctx->add_pending_remove(KEYSPACE_DATA, [&] (void *buffer)->uint8_t {
		return construct_object_key(cct, oid, buffer, blockid);
	});
}

void KvsStoreDB::add_omap(KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, const std::string &strkey, bufferlist &bl)
{
	ctx->add_pending_meta(KEYSPACE_DATA, bl, [&] (void *buffer)->uint8_t {
		return construct_omapkey_impl(buffer, index, strkey.c_str(), strkey.length(), KEYSPACE_OMAP);
	});
}

void KvsStoreDB::rm_omap(KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, const std::string &strkey)
{
	ctx->add_pending_remove(KEYSPACE_OMAP, [&] (void *buffer)->uint8_t {
		return construct_omapkey_impl(buffer, index, strkey.c_str(), strkey.length(), KEYSPACE_OMAP);
	});
}

/// ------------------------------------------------------------------------------------------------
/// Asynchronous write functions
/// ------------------------------------------------------------------------------------------------

void txc_data_callback(kv_io_context &op, void* private_data) {
    KvsTransContext *txc= (KvsTransContext *)private_data;
    txc->aio_finish(&op);
}

void txc_journal_callback(kv_io_context &op, void* private_data) {
    KvsTransContext *txc= (KvsTransContext *)private_data;
    txc->journal_finish(&op);
}

// sync write
int KvsStoreDB::write_journal(KvsTransContext *txc) {
	int ret;
	kv_value value;
	for (const KvsJournal *p : txc->ioc.journal) {
		value.offset = 0;
		value.length = p->journal_buffer_pos;
		value.value  = p->journal_buffer;

		ret = kadi.sync_write(KEYSPACE_JOURNAL, &value, [] (struct nvme_passthru_kv_cmd& cmd) {
			const uint64_t id = KvsJournal::journal_index++;
			cmd.key_length = construct_journalkey_impl(cmd.key, id);
		});

		if (ret != 0) return ret;

		delete p;	// delete journal object
	}

	txc->ioc.journal.clear();

	return 0;
}

int KvsStoreDB::aio_submit(KvsTransContext *txc)
{
   	kv_value   value;
   	int res = 0;

    const int num_batch_cmds = txc->ioc.batchctx.size();

    txc->ioc.running_ios.splice(txc->ioc.running_ios.begin(), txc->ioc.pending_ios);
    txc->ioc.num_running = txc->ioc.running_ios.size() + num_batch_cmds;

	std::unique_lock lk(txc->ioc.running_aio_lock);
	for (auto ior : txc->ioc.running_ios) {

		if (ior->data == 0 && ior->raw_data == 0) { // delete
			res = kadi.kv_delete_aio(ior->spaceid, ior->key, { txc_data_callback, static_cast<void*>(txc) });
		}
		else {
			if (ior->data) {
				set_kv_value(&value, ior->data);
			} else {
				set_kv_value(&value, ior->raw_data, ior->raw_data_length);
			}

			res = kadi.kv_store_aio(ior->spaceid, ior->key, &value, { txc_data_callback, static_cast<void*>(txc) });
		}
		if (res != 0) return res;
	}


    res = kadi.batch_submit_aio(&txc->ioc.batchctx, 0, { txc_data_callback, static_cast<void*>(txc) });

    return res;
}

KvsIterator *KvsStoreDB::get_iterator(uint32_t prefix)
{
	return new KvsBptreeIterator(&kadi.adi, skip_skp, prefix);
}

uint64_t KvsStoreDB::compact() {
	FTRACE
	if (1) return 0;
	uint64_t processed_keys = 0;
	bptree onode_tree(&kadi.adi, 6, GROUP_PREFIX_ONODE);
	bptree  coll_tree(&kadi.adi, 6, GROUP_PREFIX_COLL);
	bptree *tree;

	//derr << "compact 1" << dendl;
	processed_keys = list_oplog(KEYSPACE_ONODE, 0xffffffff,
			[&] (int opcode, int groupid, uint64_t sequence, const char* key, int length) {
			const uint32_t prefix = *(uint32_t*)key;

			if (prefix == GROUP_PREFIX_ONODE) {
				tree = &onode_tree;
			} else if (prefix == GROUP_PREFIX_COLL) {
				tree = &coll_tree;
			} else {
				return;
			}

			//derr << "opcode = " << opcode << ", key = " << print_key((const char*)key, length) << ", length = " << length << dendl;

			if (opcode == 0) {
				onode_tree.insert((char*)key, length);
			} else if (opcode == 1) {
				onode_tree.remove((char*)key, length);
			}
			//cout << "read: group"  << groupid << ", seq " << sequence << ", " << print_key((const char*)key, length) << ", length = " << length << endl;
	});
	//derr << "compact 2" << dendl;
	if (processed_keys > 0) {
		onode_tree.flush();
		coll_tree.flush();
	}
	//derr << "compact 3" << dendl;

	return processed_keys;
}
