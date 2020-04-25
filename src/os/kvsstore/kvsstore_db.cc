
#include <stdint.h>
#include "kvsstore_db.h"
#include "kvsstore_debug.h"
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

// called when each I/O operation finishes
void aio_callback(kv_io_context &op, void *post_data)
{
    FTRACE
    kvaio_t *aio = static_cast<kvaio_t*>(post_data);
    IoContext *ioc = static_cast<IoContext*>(aio->parent);

    int r = op.retcode;

    if (op.opcode == nvme_cmd_kv_retrieve && r == 0 && op.value.actual_value_size > op.value.length) {
        TR << "read retry";
        aio->pbl->clear();
        aio->bp = buffer::create_small_page_aligned(op.value.actual_value_size);
        aio->value     = aio->bp.c_str();
        aio->vallength = aio->bp.length();
        aio->valoffset = 0;

        KvsStoreDB* db = static_cast<KvsStoreDB*>(aio->db);

        db->kadi.kv_retrieve_aio(aio->spaceid, aio->key, aio->keylength, aio->value, aio->valoffset, aio->vallength, { aio->cb_func,  aio });
    }

    if ( r != 0 ) {
        ioc->set_return_value(op.retcode);
    }

    if (op.opcode == nvme_cmd_kv_retrieve) {

        aio->bp.set_length(op.value.length);
        aio->pbl->append(std::move(aio->bp));
    }

    print_value(r, op.opcode, op.key.key, op.key.length, op.value.value,op.value.length);

    bool last = ioc->try_aio_wake();
    if (last && ioc->parent) {
        KvsStore::TransContext* txc = static_cast<KvsStore::TransContext*>(ioc->parent);
        KvsStore* store = static_cast<KvsStore*>(txc->parent);

        txc->aio_finish(store);
    }
}

KvsStoreDB::KvsStoreDB(CephContext *cct_): cct(cct_), kadi(cct), compaction_started(false) {

    keyspace_sorted     = cct->_conf->kvsstore_keyspace_sorted;
    keyspace_notsorted  = cct->_conf->kvsstore_keyspace_notsorted;
}


kvaio_t* KvsStoreDB::_aio_write(int keyspaceid, bufferlist &bl, IoContext *ioc)
{
    boost::container::small_vector<iovec,4> iov;
    bl.prepare_iov(&iov);

    return _aio_write(keyspaceid, (void*)iov[0].iov_base, iov[0].iov_len, ioc);
}


kvaio_t* KvsStoreDB::_aio_write(int keyspaceid, void *addr, uint32_t len, IoContext *ioc)
{
    ioc->pending_aios.push_back(kvaio_t(nvme_cmd_kv_store, keyspaceid, aio_callback, ioc, this));
    ++ioc->num_pending;

    kvaio_t& aio = ioc->pending_aios.back();
    aio.value     = addr;
    aio.vallength = len;
    aio.valoffset = 0;
    return &aio;
}

kvaio_t* KvsStoreDB::_syncio_write(int keyspaceid, bufferlist &bl, IoContext *ioc)
{
    boost::container::small_vector<iovec,16> iov;
    bl.prepare_iov(&iov);
    return _syncio_write(keyspaceid, (void*)iov[0].iov_base, iov[0].iov_len, ioc);
}

kvaio_t* KvsStoreDB::_syncio_write(int keyspaceid, void *addr, uint32_t len, IoContext *ioc)
{
    ioc->pending_syncios.push_back(kvaio_t(nvme_cmd_kv_store, keyspaceid, aio_callback, ioc, this));

    kvaio_t& aio = ioc->pending_syncios.back();
    aio.value     = addr;
    aio.vallength = len;
    aio.valoffset = 0;
    return &aio;
}

kvaio_t* KvsStoreDB::_aio_read(int keyspaceid, uint32_t len, bufferlist *pbl, IoContext *ioc)
{
    ioc->pending_aios.push_back(kvaio_t(nvme_cmd_kv_retrieve, keyspaceid, aio_callback, ioc, this));
    ++ioc->num_pending;
    kvaio_t& aio = ioc->pending_aios.back();

    aio.bp = buffer::create_small_page_aligned(len);
    aio.pbl = pbl; // out bl - needs to be cleared when retrying

    aio.value     = aio.bp.c_str();
    aio.vallength = aio.bp.length();
    aio.valoffset = 0;

    return &aio;
}

kvaio_t* KvsStoreDB::_syncio_remove(int keyspaceid, IoContext *ioc)
{
    ioc->pending_syncios.push_back(kvaio_t(nvme_cmd_kv_delete, keyspaceid, aio_callback, ioc, this));

    kvaio_t& aio = ioc->pending_syncios.back();
    aio.value     = 0;
    aio.vallength = 0;
    aio.valoffset = 0;
    return &aio;
}


kvaio_t* KvsStoreDB::_aio_remove(int keyspaceid, IoContext *ioc)
{
    ioc->pending_aios.push_back(kvaio_t(nvme_cmd_kv_delete, keyspaceid, aio_callback, ioc, this));
    ++ioc->num_pending;

    kvaio_t& aio = ioc->pending_aios.back();
    aio.value     = 0;
    aio.vallength = 0;
    aio.valoffset = 0;
    return &aio;
}

void KvsStoreDB::aio_read_chunk(const ghobject_t &oid, uint16_t chunkid, uint32_t len, bufferlist &bl, IoContext *ioc)
{
    kvaio_t *aio = _aio_read(keyspace_notsorted, len, &bl, ioc);
    aio->keylength = construct_object_key(cct, oid, aio->key, chunkid);
}

void KvsStoreDB::aio_write_chunk(const ghobject_t &oid, uint16_t chunkid, void *addr, uint32_t len, IoContext *ioc)
{
    kvaio_t *aio = _aio_write(keyspace_notsorted, addr, len, ioc);
    aio->keylength = construct_object_key(cct, oid, aio->key, chunkid);

}

void KvsStoreDB::aio_remove_chunk(const ghobject_t &oid, uint16_t chunkid, IoContext *ioc)
{
    kvaio_t *aio = _aio_remove(keyspace_notsorted, ioc);
    aio->keylength = construct_object_key(cct, oid, aio->key, chunkid);
}

void KvsStoreDB::aio_read_onode(const ghobject_t &oid, bufferlist &bl, IoContext *ioc)
{
    kvaio_t *aio = _aio_read(keyspace_sorted, KVS_OBJECT_SPLIT_SIZE, &bl, ioc);
    aio->keylength = construct_onode_key(cct, oid, aio->key);
    // TR << "read onode: oid = " << oid  << " key = " << print_kvssd_key((const char*)aio->key, aio->keylength) ;

}

void KvsStoreDB::aio_write_onode(const ghobject_t &oid, bufferlist &bl, IoContext *ioc, bool sync)
{
    kvaio_t *aio;
    if (sync) {
        aio = _syncio_write(keyspace_sorted,  bl, ioc);
    } else {
        aio = _aio_write(keyspace_sorted,  bl, ioc);
    }
    aio->keylength = construct_onode_key(cct, oid, aio->key);
    // TR << "write onode: oid = " << oid << ", key = " << print_kvssd_key((const char*)aio->key, aio->keylength) ;
}

void KvsStoreDB::aio_remove_onode(const ghobject_t &oid, IoContext *ioc, bool sync)
{
    kvaio_t *aio;
    if (sync) {
        aio = _syncio_remove(keyspace_sorted,  ioc);
    } else {
        aio = _aio_remove(keyspace_sorted, ioc);
    }

    aio->keylength = construct_onode_key(cct, oid, aio->key);
}

void KvsStoreDB::aio_read_omap(const uint64_t index, const std::string &strkey, bufferlist &bl, IoContext *ioc)
{
    kvaio_t *aio = _aio_read(keyspace_notsorted, KVS_OBJECT_SPLIT_SIZE, &bl, ioc);
    aio->keylength = construct_omapkey_impl( aio->key, index, strkey.c_str(), strkey.length());
}

void KvsStoreDB::aio_write_omap(uint64_t index, const std::string &strkey, bufferlist &bl, IoContext *ioc, bool sync)
{
    FTRACE
    kvaio_t *aio;
    if (sync) {
        aio = _syncio_write(keyspace_notsorted, bl, ioc);
    } else {
        aio = _aio_write(keyspace_notsorted, bl, ioc);
    }
    aio->keylength = construct_omapkey_impl( aio->key, index, strkey.c_str(), strkey.length());
}

void KvsStoreDB::aio_remove_omap(uint64_t index, const std::string &strkey, IoContext *ioc, bool sync)
{
    kvaio_t *aio;
    if (sync) {
        aio = _syncio_remove(keyspace_notsorted, ioc);
    } else {
        aio = _aio_remove(keyspace_notsorted, ioc);
    };
    aio->keylength = construct_omapkey_impl( aio->key, index, strkey.c_str(), strkey.length());
}

void KvsStoreDB::aio_read_journal(int index, bufferlist &bl, IoContext *ioc)
{
    kvaio_t *aio = _aio_read(keyspace_notsorted, KVS_OBJECT_SPLIT_SIZE, &bl, ioc);
    aio->keylength = construct_journalkey_impl(aio->key, index);
}
void KvsStoreDB::aio_write_journal(int index, void *addr, uint32_t len, IoContext *ioc)
{
    kvaio_t *aio = _aio_write(keyspace_notsorted, addr, len, ioc);
    aio->keylength = construct_journalkey_impl(aio->key, index);
}
void KvsStoreDB::aio_remove_journal(int index, IoContext *ioc)
{
    kvaio_t *aio = _aio_remove(keyspace_notsorted, ioc);
    aio->keylength = construct_journalkey_impl(aio->key, index);
}

void KvsStoreDB::aio_read_coll(const coll_t &cid, bufferlist &bl, IoContext *ioc)
{
    const char *cidkey_str = cid.c_str();
    const int   cidkey_len = (int)strlen(cidkey_str);

    kvaio_t *aio = _aio_read(keyspace_sorted, KVS_OBJECT_SPLIT_SIZE, &bl, ioc);
    aio->keylength = construct_collkey_impl(aio->key, cidkey_str, cidkey_len);
}

void KvsStoreDB::aio_write_coll(const coll_t &cid, bufferlist &bl, IoContext *ioc) {
    const char *cidkey_str = cid.c_str();
    const int cidkey_len = (int) strlen(cidkey_str);

    kvaio_t *aio = _aio_write(keyspace_sorted, bl, ioc);
    aio->keylength = construct_collkey_impl(aio->key, cidkey_str, cidkey_len);

}

void KvsStoreDB::aio_remove_coll(const coll_t &cid, IoContext *ioc)
{
    TRBACKTRACE;

    const char *cidkey_str = cid.c_str();
    const int   cidkey_len = (int)strlen(cidkey_str);

    kvaio_t *aio = _aio_remove(keyspace_sorted, ioc);
    aio->keylength = construct_collkey_impl(aio->key, cidkey_str, cidkey_len);

    //TR << "trying to delete COLL - " << print_kvssd_key((char *) aio->key, aio->keylength);
}

// low level sync read function
int KvsStoreDB::read_kvkey(kv_key *key, bufferlist &bl, bool sorted)
{
    IoContext ioc( 0);
    const int keyspaceid = (sorted)? keyspace_sorted:keyspace_notsorted;
    boost::container::small_vector<iovec,4> iov;    // to retrieve an internal address from a bufferlist
    kv_value value;

    bufferptr bp = buffer::create_small_page_aligned(8192);
    value.value  = bp.c_str();
    value.length = bp.length();
    value.offset = 0;

    int r =  this->kadi.kv_retrieve_sync(keyspaceid, key, &value);
    if (r == 0) {
        print_value(r, 0x90, key->key, key->length, value.value, value.length);
        bp.set_length(value.length);
        bl.append(std::move(bp));
    }
    return r;
}

int KvsStoreDB::read_sb(bufferlist &bl) {
    IoContext ioc(0);
    kvaio_t *aio = _aio_read(keyspace_notsorted, KVS_OBJECT_SPLIT_SIZE, &bl, &ioc);
    aio->keylength =  construct_kvsbkey_impl(aio->key);

    if (ioc.has_pending_aios()) {
        aio_submit(&ioc);
        ioc.aio_wait();
        int r = ioc.get_return_value();
        if (r != 0) {
            return r;
        }
    }
    return 0;
}

int KvsStoreDB::write_sb(bufferlist &bl) {
    IoContext ioc(0);

    kvaio_t *aio = _syncio_write(keyspace_notsorted, bl, &ioc);
    aio->keylength =  construct_kvsbkey_impl(aio->key);

    print_value(0, -1, aio->key, aio->keylength, bl.c_str(), bl.length());

    return syncio_submit_and_wait(&ioc);
}

int KvsStoreDB::aio_write_sb(bufferlist &bl, IoContext *ioc) {
    kvaio_t *aio = _aio_write(keyspace_notsorted, bl, ioc);
    aio->keylength =  construct_kvsbkey_impl(aio->key);
    return 0;
}


int KvsStoreDB::aio_submit_and_wait(IoContext *ioc) {
    int r = 0;
    if (ioc->has_pending_aios()) {
        aio_submit(ioc);
        ioc->aio_wait();
        r = ioc->get_return_value();
        if (r != 0) {
            return r;
        }
    }
    return r;
}

int KvsStoreDB::syncio_submit_and_wait(IoContext *ioc) {
    int r = 0;
    if (ioc->pending_syncios.size()) {
        syncio_submit(ioc);
        ioc->aio_wait();
        r = ioc->get_return_value();
        if (r != 0) {
            return r;
        }
    }
    return r;
}

int KvsStoreDB::aio_submit(IoContext *ioc)
{
    if (ioc->num_pending.load() == 0) {
        return 0;
    }

    list<kvaio_t>::iterator e = ioc->running_aios.begin();
    ioc->running_aios.splice(e, ioc->pending_aios);

    int pending = ioc->num_pending.load();
    ioc->num_running += pending;
    ioc->num_pending -= pending;

    int r= 0;

    auto cur = ioc->running_aios.begin();
    auto end = e;

    //TR << "aio " << (void*) &(*cur) << ", parent = " << (void*)cur->parent;


    while (cur != end) {
        switch (cur->opcode) {
            case nvme_cmd_kv_retrieve:
                r = kadi.kv_retrieve_aio(cur->spaceid, cur->key, cur->keylength, cur->value, cur->valoffset, cur->vallength, { cur->cb_func,  &(*cur) });
                break;
            case nvme_cmd_kv_delete:
                //TR << "kvdelete - " << print_kvssd_key((char *) cur->key, cur->keylength);
                r = kadi.kv_delete_aio(cur->spaceid, cur->key, cur->keylength, { cur->cb_func,  &(*cur) });
                break;
            case nvme_cmd_kv_store:
                //TR << "kvstore - " << print_kvssd_key((char *) cur->key, cur->keylength) << ", hash = " << ceph_str_hash_linux((const char *) cur->value, cur->vallength) << ", val length " << cur->vallength << ", addr = " << (void*)cur->value;
                r = kadi.kv_store_aio(cur->spaceid, cur->key, cur->keylength, cur->value, cur->valoffset, cur->vallength, { cur->cb_func,  &(*cur)});
                break;
        };

        if (r != 0) {
            derr << "IO error: ret = " << r << dendl;
            ceph_abort_msg("IO error in aio_submit");
        }

        cur++;
    }

    return r;
}


int KvsStoreDB::syncio_submit(IoContext *ioc)
{
    if (ioc->pending_syncios.size() == 0) {
        return 0;
    }
    int r;

    ioc->num_running = ioc->pending_syncios.size();

    auto cur = ioc->pending_syncios.begin();
    auto end = ioc->pending_syncios.end();



    while (cur != end) {
        switch (cur->opcode) {
            case nvme_cmd_kv_retrieve:
                r = kadi.kv_retrieve_aio(cur->spaceid, cur->key, cur->keylength, cur->value, cur->valoffset, cur->vallength, { cur->cb_func,  &(*cur) });
                break;
            case nvme_cmd_kv_delete:
                r = kadi.kv_delete_aio(cur->spaceid, cur->key, cur->keylength, { cur->cb_func,  &(*cur) });
                break;
            case nvme_cmd_kv_store:
                r = kadi.kv_store_aio(cur->spaceid, cur->key, cur->keylength, cur->value, cur->valoffset, cur->vallength, { cur->cb_func,  &(*cur)});
                break;
        };

        if (r != 0) {
            derr << "IO error: ret = " << r << dendl;
            ceph_abort_msg("IO error in aio_submit");
        }

        cur++;
    }

    return r;
}










/// ------------------------------------------------------------------------------------------------
/// Asynchronous write functions
/// ------------------------------------------------------------------------------------------------

KvsIterator *KvsStoreDB::get_iterator(uint32_t prefix)
{
	return new KvsBptreeIterator(&kadi, skip_skp, prefix);
}

uint64_t KvsStoreDB::compact() {
	FTRACE
    {
        std::unique_lock<std::mutex> cl (compact_lock);
        if (compaction_started) {
            compact_cond.wait(cl);
            return 0;
        } else {
            compaction_started = true;
        }
    };
	uint64_t processed_keys = 0;

	bptree onode_tree(&kadi, 1, GROUP_PREFIX_ONODE);
	bptree  coll_tree(&kadi, 1, GROUP_PREFIX_COLL);
	bptree *tree;

	processed_keys = kadi.list_oplog(keyspace_sorted, 0xffffffff,
			[&] (int opcode, int groupid, uint64_t sequence, const char* key, int length) {

			const uint32_t prefix = *(uint32_t*)key;
            std::string treename = "";
			if (prefix == GROUP_PREFIX_ONODE) {
				tree = &onode_tree;
                treename = "onode tree: ";
			} else if (prefix == GROUP_PREFIX_COLL) {
				tree = &coll_tree;
                treename = "coll tree: ";
			} else {
				return;
			}
            //TR  << "found: opcode = " << opcode << ", key = " << print_key((const char*)key, length) << ", length = " << length ;
			if (opcode == nvme_cmd_kv_store) {
                //TR << "tree-insert " << treename << " key: " << print_kvssd_key(std::string((char*)key,length)) ;
                tree->insert((char*)key, length);
			} else if (opcode == nvme_cmd_kv_delete) {
                //TR << "tree-remove " << treename << " key: " << print_kvssd_key(std::string((char*)key,length)) ;
                tree->remove((char*)key, length);

			}

			//cout << "read: group"  << groupid << ", seq " << sequence << ", " << print_key((const char*)key, length) << ", length = " << length << endl;
	});

    //TR << "compaction 1: found and read " << processed_keys << " oplog pages" ;

    onode_tree.flush();

    coll_tree.flush();

    //TR << "compaction 2: updated the index structure " ;

    {
        std::unique_lock<std::mutex> cl (compact_lock);
        compaction_started = false;
        compact_cond.notify_all();
    };

	return processed_keys;
}


KvsBptreeIterator::KvsBptreeIterator(KADI *adi, int ksid_skp, uint32_t prefix):
        tree(adi,ksid_skp, prefix)
{
    iter = tree.get_iterator();
}

int KvsBptreeIterator::begin() {
    iter->begin();
    return 0;
}
int KvsBptreeIterator::end() {
    iter->end();
    return 0;
}

int KvsBptreeIterator::upper_bound(const kv_key &key) {
    iter->upper_bound((char*)key.key, key.length);
    return 0;

}
int KvsBptreeIterator::lower_bound(const kv_key &key) {
    iter->lower_bound((char*)key.key, key.length);
    return 0;
}

bool KvsBptreeIterator::valid() {
    return !iter->is_end();
}

int KvsBptreeIterator::next() {
    iter->move_next(1);
    return 0;
}
int KvsBptreeIterator::prev() {
    iter->move_next(-1);
    return 0;
}

kv_key KvsBptreeIterator::key() {
    char *key = 0;
    int   len = 0;
    if (!iter->get_key((char**)&key, len)) {
        end();
        //TR << "current key not found" ;
        return { 0, 0 };
    };
    //TR << print_kvssd_key(key, len) ;
    return { key, (uint8_t)len };
}

