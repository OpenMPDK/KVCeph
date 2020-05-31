
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
    IoContext *ioc = aio->parent;

    int r = op.retcode;

    if (op.opcode == nvme_cmd_kv_retrieve && r == 0 && op.value.actual_value_size > op.value.length) {
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
        //TRU << "read " << op.value.length << " bytes";
    }

    //TRU << "opcode " << op.opcode << ", key=" << print_kvssd_key(op.key.key, op.key.length) << ", valuelength = " << op.value.length << " bytes";

    //TR << print_value(r, op.opcode, op.key.key, op.key.length, op.value.value,op.value.length);

    bool islastio_in_tr = ioc->mark_io_complete();
    if (islastio_in_tr)
    {
        KvsStore::TransContext* txc = static_cast<KvsStore::TransContext*>(ioc->parent);
        KvsStore* store = static_cast<KvsStore*>(txc->parent);
        txc->aio_finish(store);
    }

}

KvsStoreDB::KvsStoreDB(CephContext *cct_): cct(cct_), kadi(cct), compaction_started(false) {
    FTRACE
    if (cct) {
        keyspace_sorted = cct->_conf->kvsstore_keyspace_sorted;
        keyspace_notsorted = cct->_conf->kvsstore_keyspace_notsorted;
    }

    omap_readfunc = [&] (uint64_t nid, int startid, int endid, std::vector<bufferlist*>& iolist) -> bool {
        return this->aio_read_omap_keyblock(nid, startid, endid, iolist) == 0;
    };

    omap_writefunc =[&](uint64_t nid, bufferlist* buffer, int start_id, IoContext &ctx) {
        this->aio_write_omap_keyblock(nid, buffer, start_id, ctx);
    };

    omap_removefunc = [&] (uint64_t nid, int start_id, int end_id, IoContext &ctx) -> bool {
        return this->aio_delete_omap_keyblock(nid, start_id, end_id, ctx) == 0;
    };
}
int KvsStoreDB::aio_write_omap_keyblock(uint64_t nid, bufferlist *bl, int start_id, IoContext &ioc) {
    TRU << "writing keyblock #" << start_id << ", length = " << bl->length();
    kvaio_t *aio = _aio_write(keyspace_notsorted, *bl, &ioc);
    aio->keylength = construct_omapblockkey_impl( aio->key, nid, start_id);
    return 0;
}

int KvsStoreDB::aio_delete_omap_keyblock(uint64_t nid, uint32_t startid, uint32_t endid, IoContext &ioc) {
    for (unsigned id = startid ; id < endid; id++) {
        kvaio_t *aio= _aio_remove(keyspace_notsorted, &ioc);
        aio->keylength = construct_omapblockkey_impl( aio->key, nid, id);
    }
}

int KvsStoreDB::aio_read_omap_keyblock(uint64_t nid,uint32_t start_id, uint32_t end_id, std::vector<bufferlist*> &bls) {
    IoContext ioc(0, __func__);
    TRU << "omap read " << end_id - start_id << " blocks";
    for (unsigned id = start_id; id < end_id; id++) {
        bufferlist *bl = new bufferlist();
        kvaio_t *aio = _aio_read(keyspace_notsorted, DEFAULT_OMAPBUF_SIZE, bl, &ioc);
        aio->keylength = construct_omapblockkey_impl(aio->key, nid, start_id);

        bls.push_back(bl);
    }

    int r = ioc.aio_submit_and_wait(&kadi, __func__);

    TRU << "finished: r = " << r << ", data read = " << bls[0]->length();

    if (r != 0) {
        for (bufferlist *bl: bls) { delete bl; }
        bls.clear();
        return r;
    }
    return r;
}

void KvsStoreDB::aio_read_omap(const uint64_t index, const std::string &strkey, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_read(keyspace_notsorted, KVS_OBJECT_SPLIT_SIZE, &bl, ioc);
    aio->keylength = construct_omapkey_impl( aio->key, index, strkey.c_str(), strkey.length());
}

void KvsStoreDB::aio_write_omap(uint64_t index, const std::string &strkey, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_write(keyspace_notsorted, bl, ioc);
    aio->keylength = construct_omapkey_impl( aio->key, index, strkey.c_str(), strkey.length());
}

void KvsStoreDB::aio_remove_omap(uint64_t index, const std::string &strkey, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio= _aio_remove(keyspace_notsorted, ioc);
    aio->keylength = construct_omapkey_impl( aio->key, index, strkey.c_str(), strkey.length());
}

kvaio_t* KvsStoreDB::_aio_write(int keyspaceid, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    bl.c_str();
    boost::container::small_vector<iovec,4> iov;
    bl.prepare_iov(&iov);

    return _aio_write(keyspaceid, (void*)iov[0].iov_base, iov[0].iov_len, ioc);
}


kvaio_t* KvsStoreDB::_aio_write(int keyspaceid, void *addr, uint32_t len, IoContext *ioc)
{
    FTRACE
    ioc->pending_aios.push_back(new kvaio_t(nvme_cmd_kv_store, keyspaceid, aio_callback, ioc, this));

    kvaio_t* aio = ioc->pending_aios.back();
    aio->value     = addr;
    aio->vallength = len;
    aio->valoffset = 0;
    return aio;
}


kvaio_t* KvsStoreDB::_aio_read(int keyspaceid, uint32_t len, bufferlist *pbl, IoContext *ioc)
{
    FTRACE
    ioc->pending_aios.push_back(new kvaio_t(nvme_cmd_kv_retrieve, keyspaceid, aio_callback, ioc, this));
    kvaio_t* aio = ioc->pending_aios.back();

    aio->bp = buffer::create_small_page_aligned(len);
    aio->pbl = pbl; // out bl - needs to be cleared when retrying

    aio->value     = aio->bp.c_str();
    aio->vallength = aio->bp.length();
    aio->valoffset = 0;

    return aio;
}



kvaio_t* KvsStoreDB::_aio_remove(int keyspaceid, IoContext *ioc)
{
    FTRACE
    ioc->pending_aios.push_back(new kvaio_t(nvme_cmd_kv_delete, keyspaceid, aio_callback, ioc, this));

    kvaio_t* aio = ioc->pending_aios.back();
    aio->value     = 0;
    aio->vallength = 0;
    aio->valoffset = 0;
    return aio;
}

void KvsStoreDB::aio_read_chunk(const ghobject_t &oid, uint16_t chunkid, uint32_t len, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_read(keyspace_notsorted, len, &bl, ioc);
    aio->keylength = construct_object_key(cct, oid, aio->key, chunkid);
}

void KvsStoreDB::aio_write_chunk(const ghobject_t &oid, uint16_t chunkid, void *addr, uint32_t len, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_write(keyspace_notsorted, addr, len, ioc);
    aio->keylength = construct_object_key(cct, oid, aio->key, chunkid);

}

void KvsStoreDB::aio_remove_chunk(const ghobject_t &oid, uint16_t chunkid, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_remove(keyspace_notsorted, ioc);
    aio->keylength = construct_object_key(cct, oid, aio->key, chunkid);
}

void KvsStoreDB::aio_read_onode(const ghobject_t &oid, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_read(keyspace_sorted, KVS_OBJECT_SPLIT_SIZE, &bl, ioc);
    aio->keylength = construct_onode_key(cct, oid, aio->key);
    // TR << "read onode: oid = " << oid  << " key = " << print_kvssd_key((const char*)aio->key, aio->keylength) ;

}

void KvsStoreDB::aio_write_onode(const ghobject_t &oid, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    //TR << "onode " << oid << ", length = " << bl.length();
    kvaio_t *aio = _aio_write(keyspace_sorted,  bl, ioc);
    aio->keylength = construct_onode_key(cct, oid, aio->key);
    // TR << "write onode: oid = " << oid << ", key = " << print_kvssd_key((const char*)aio->key, aio->keylength) ;
}

void KvsStoreDB::aio_remove_onode(const ghobject_t &oid, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_remove(keyspace_sorted, ioc);
    aio->keylength = construct_onode_key(cct, oid, aio->key);
}


void KvsStoreDB::aio_read_journal(int index, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_read(keyspace_notsorted, KVS_OBJECT_SPLIT_SIZE, &bl, ioc);
    aio->keylength = construct_journalkey_impl(aio->key, index);
}
void KvsStoreDB::aio_write_journal(int index, void *addr, uint32_t len, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_write(keyspace_notsorted, addr, len, ioc);
    aio->keylength = construct_journalkey_impl(aio->key, index);
}
void KvsStoreDB::aio_remove_journal(int index, IoContext *ioc)
{
    FTRACE
    kvaio_t *aio = _aio_remove(keyspace_notsorted, ioc);
   aio->keylength = construct_journalkey_impl(aio->key, index);
}

void KvsStoreDB::aio_read_coll(const coll_t &cid, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    const char *cidkey_str = cid.c_str();
    const int   cidkey_len = (int)strlen(cidkey_str);

    kvaio_t *aio = _aio_read(keyspace_sorted, KVS_OBJECT_SPLIT_SIZE, &bl, ioc);
    aio->keylength = construct_collkey_impl(aio->key, cidkey_str, cidkey_len);
}

void KvsStoreDB::aio_write_coll(const coll_t &cid, bufferlist &bl, IoContext *ioc) {
    FTRACE
    const char *cidkey_str = cid.c_str();
    const int cidkey_len = (int) strlen(cidkey_str);

    kvaio_t *aio = _aio_write(keyspace_sorted, bl, ioc);
    aio->keylength = construct_collkey_impl(aio->key, cidkey_str, cidkey_len);

}

void KvsStoreDB::aio_remove_coll(const coll_t &cid, IoContext *ioc)
{
    FTRACE

    const char *cidkey_str = cid.c_str();
    const int   cidkey_len = (int)strlen(cidkey_str);

    kvaio_t *aio = _aio_remove(keyspace_sorted, ioc);
    aio->keylength = construct_collkey_impl(aio->key, cidkey_str, cidkey_len);

    //TR << "trying to delete COLL - " << print_kvssd_key((char *) aio->key, aio->keylength);
}
int KvsStoreDB::read_onode(const ghobject_t &oid, bufferlist &bl)
{
    FTRACE
    char keybuffer[256];
    kv_key key;
    key.key    = keybuffer;
    key.length = construct_onode_key(cct, oid, keybuffer);

    kv_value value;
    bufferptr bp = buffer::create_small_page_aligned(8192);
    value.value  = bp.c_str();
    value.length = bp.length();
    value.offset = 0;

    int r =  this->kadi.kv_retrieve_sync(keyspace_sorted, &key, &value);
    if (r == 0) {
        bp.set_length(value.length);
        bl.append(std::move(bp));
    }
    return r;
}
// low level sync read function
int KvsStoreDB::read_kvkey(kv_key *key, bufferlist &bl, bool sorted)
{
    FTRACE
    const int keyspaceid = (sorted)? keyspace_sorted:keyspace_notsorted;
    kv_value value;

    bufferptr bp = buffer::create_small_page_aligned(8192);
    value.value  = bp.c_str();
    value.length = bp.length();
    value.offset = 0;

    int r =  this->kadi.kv_retrieve_sync(keyspaceid, key, &value);
    if (r == 0) {
        //TR << print_value(r, 0x90, key->key, key->length, value.value, value.length);
        bp.set_length(value.length);
        bl.append(std::move(bp));
    }
    return r;
}

int KvsStoreDB::read_sb(bufferlist &bl) {
    FTRACE
    IoContext ioc(0, __func__ );
    kvaio_t *aio = _aio_read(keyspace_notsorted, KVS_OBJECT_SPLIT_SIZE, &bl, &ioc);
    aio->keylength =  construct_kvsbkey_impl(aio->key);

    return ioc.aio_submit_and_wait(&kadi, __func__);
}

int KvsStoreDB::write_sb(bufferlist &bl) {
    FTRACE
    char keybuffer[256];
    bl.c_str();
    boost::container::small_vector<iovec,4> iov;
    bl.prepare_iov(&iov);

    kv_value v;
    v.value     = (void*)iov[0].iov_base;
    v.length = iov[0].iov_len;
    v.offset = 0;
    kv_key k;
    k.key = keybuffer;
    k.length = construct_kvsbkey_impl(keybuffer);

    return this->kadi.kv_store_sync(keyspace_notsorted, &k, &v);
}

int KvsStoreDB::aio_write_sb(bufferlist &bl, IoContext *ioc) {
    FTRACE
    kvaio_t *aio = _aio_write(keyspace_notsorted, bl, ioc);
    aio->keylength =  construct_kvsbkey_impl(aio->key);
    return 0;
}
#if 0

int KvsStoreDB::syncio_submit_and_wait(IoContext *ioc) {
    FTRACE
    TR << "syncio_submit_and_wait entered";
    int r = 0;
    if (ioc->pending_syncios.size()) {
        syncio_submit(ioc);
        ioc->aio_wait();
        r = ioc->get_return_value();
        if (r != 0) {
            TR << "syncio_submit_and_wait exited r = " << r;
            return r;
        }
    }
    TR << "syncio_submit_and_wait exited r = " << r;
    return r;
}

#include <sstream>
int KvsStoreDB::aio_submit(IoContext *ioc)
{
    FTRACE
    if (ioc->num_pending.load() == 0) {
        return 0;
    }

    ioc->running_aios.clear();
    list<kvaio_t*>::iterator e = ioc->running_aios.begin();
    ioc->running_aios.splice(e, ioc->pending_aios);

    int pending = ioc->num_pending.load();
    ioc->num_running += pending;
    ioc->num_pending -= pending;

    int r= 0;

    std::unique_lock<std::mutex> lck(ioc->running_aio_lock);
    auto cur = ioc->running_aios.begin();
    auto end = ioc->running_aios.end();
#if 0
    //TR << "aio " << (void*) &(*cur) << ", parent = " << (void*)cur->parent;
    TR2 << "AIO SUBMIT - dry run ioc = " << (void*)ioc << ", num running = " << ioc->num_running.load();
    while (cur != end) {
        kvaio_t *aio = (*cur);
        aio->caller  = (uint64_t)pthread_self();



        std::stringstream  ss;
        ss << "DRYRUN op = : " << aio->opcode << " kvaio_t = " << (void *) aio << ", aio parent = "
            << (void *) aio->parent << "== ioc = " << (void *) ioc << ", txc(ioc->parent) = " << (void*)ioc->parent;
        aio->debug   = ss.str();
        cur++;
    }

    cur = ioc->running_aios.begin();
    end = ioc->running_aios.end();

    int num =0 ;
    TR2 << "AIO SUBMIT - run ioc = " << (void*)ioc;
#endif
    while (cur != end) {
        //TR2 << "LOOP " << num++ << "/" << pending;
        kvaio_t *aio = (*cur);
#if 0
        if (aio) {
            if (aio->opcode > 200) {
                TR2 << "ERROR AIO op contains a wrong pointer";
                TRBACKTRACE;
            }
            TR2 << "SEND AIO op = : "<< aio->opcode <<  " kvaio_t = " << (void*)aio << ", aio parent = " << (void*) aio->parent << ", ioc = " << (void*)ioc << ", txc = " << ioc->parent;
        }
#endif

        switch (aio->opcode) {
            case nvme_cmd_kv_retrieve:

                r = kadi.kv_retrieve_aio(aio->spaceid, aio->key, aio->keylength, aio->value, aio->valoffset, aio->vallength, { aio->cb_func,  aio });
                break;
            case nvme_cmd_kv_delete:
                //TR << "kvdelete - " << print_kvssd_key((char *) cur->key, cur->keylength);
                r = kadi.kv_delete_aio(aio->spaceid, aio->key, aio->keylength, { aio->cb_func,  aio });
                break;
            case nvme_cmd_kv_store:
                //TR << "kvstore - " << print_kvssd_key((char *) cur->key, cur->keylength) << ", hash = " << ceph_str_hash_linux((const char *) cur->value, cur->vallength) << ", val length " << cur->vallength << ", addr = " << (void*)cur->value;
                r = kadi.kv_store_aio(aio->spaceid, aio->key, aio->keylength, aio->value, aio->valoffset, aio->vallength, { aio->cb_func,  aio});
                break;
        };

        if (r != 0) {
            derr << "IO error: ret = " << r << dendl;
            ceph_abort_msg("IO error in aio_submit");
        }

        cur++;
    }

    TR2 << "AIO SUBMIT DONE - ioc = " << (void*)ioc;

    return r;
}


int KvsStoreDB::syncio_submit(IoContext *ioc)
{
    FTRACE
    if (ioc->pending_syncios.size() == 0) {
        return 0;
    }
    int r;

    ioc->num_running = ioc->pending_syncios.size();

    auto cur = ioc->pending_syncios.begin();
    auto end = ioc->pending_syncios.end();



    while (cur != end) {
        kvaio_t *aio = (*cur);
        if (aio) {
            TR2 << "SEND SYNC op = : " << aio->opcode << " aio = " << (void *) aio << ", aio parent = "
                << (void *) aio->parent << ", ioc = " << (void *) ioc << ", txc = " << ioc->parent;
        } else {
            TR2 << "ERROR AIO is null";
            TRBACKTRACE;
        }

        switch (aio->opcode) {
            case nvme_cmd_kv_retrieve:
                r = kadi.kv_retrieve_aio(aio->spaceid, aio->key, aio->keylength, aio->value, aio->valoffset, aio->vallength, { aio->cb_func,  aio });
                break;
            case nvme_cmd_kv_delete:
                r = kadi.kv_delete_aio(aio->spaceid, aio->key, aio->keylength, { aio->cb_func,  aio });
                break;
            case nvme_cmd_kv_store:
                r = kadi.kv_store_aio(aio->spaceid, aio->key, aio->keylength, aio->value, aio->valoffset, aio->vallength, { aio->cb_func,  aio});
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




#endif





/// ------------------------------------------------------------------------------------------------
/// Asynchronous write functions
/// ------------------------------------------------------------------------------------------------

KvsIterator *KvsStoreDB::get_iterator(uint32_t prefix)
{
    FTRACE
	return new KvsBptreeIterator(&kadi, keyspace_notsorted, prefix);
}

uint64_t KvsStoreDB::compact() {
	FTRACE
    {
        std::unique_lock<std::mutex> cl (compact_lock);
        if (compaction_started) {
            TRI << "wait...";
            compact_cond.wait(cl);
            return 0;
        } else {
            compaction_started = true;
        }
    };

	TRI << "started";
	uint64_t processed_keys = 0;
    uint64_t keys = 0;
	bptree onode_tree(&kadi, keyspace_notsorted, GROUP_PREFIX_ONODE);
	bptree  coll_tree(&kadi, keyspace_notsorted, GROUP_PREFIX_COLL);
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

                keys++;
			//cout << "read: group"  << groupid << ", seq " << sequence << ", " << print_key((const char*)key, length) << ", length = " << length << endl;
	});

    TRI << "compaction 1: found and read " << processed_keys << " oplog pages, inserted/removed keys = " << keys ;

    onode_tree.flush();

    coll_tree.flush();

    TRI << "compaction 2: updated the index structure " ;

    {
        std::unique_lock<std::mutex> cl (compact_lock);
        compaction_started = false;
        compact_cond.notify_all();
    };
    TRI << "finished";
	return processed_keys;
}


KvsBptreeIterator::KvsBptreeIterator(KADI *adi, int ksid_skp, uint32_t prefix):
        tree(adi,ksid_skp, prefix)
{
    FTRACE
    iter = tree.get_iterator();
}

int KvsBptreeIterator::begin() {
    FTRACE
    iter->begin();
    return 0;
}
int KvsBptreeIterator::end() {
    FTRACE
    iter->end();
    return 0;
}

int KvsBptreeIterator::upper_bound(const kv_key &key) {
    FTRACE
    iter->upper_bound((char*)key.key, key.length);
    return 0;

}
int KvsBptreeIterator::lower_bound(const kv_key &key) {
    FTRACE
    iter->lower_bound((char*)key.key, key.length);
    return 0;
}

bool KvsBptreeIterator::valid() {
    FTRACE
    return !iter->is_end();
}

int KvsBptreeIterator::next() {
    FTRACE
    iter->move_next(1);
    return 0;
}
int KvsBptreeIterator::prev() {
    FTRACE
    iter->move_next(-1);
    return 0;
}

kv_key KvsBptreeIterator::key() {
    FTRACE
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


#if 0
kvaio_t* KvsStoreDB::_syncio_write(int keyspaceid, bufferlist &bl, IoContext *ioc)
{
    FTRACE
    boost::container::small_vector<iovec,16> iov;
    bl.prepare_iov(&iov);
    return _syncio_write(keyspaceid, (void*)iov[0].iov_base, iov[0].iov_len, ioc);
}

kvaio_t* KvsStoreDB::_syncio_write(int keyspaceid, void *addr, uint32_t len, IoContext *ioc)
{
    FTRACE
    ioc->pending_syncios.push_back(new kvaio_t(nvme_cmd_kv_store, keyspaceid, aio_callback, ioc, this));

    kvaio_t* aio = ioc->pending_syncios.back();
    TR << "aio.ioc = " << (void*) aio->parent;
    aio->value     = addr;
    aio->vallength = len;
    aio->valoffset = 0;
    return aio;
}
kvaio_t* KvsStoreDB::_syncio_remove(int keyspaceid, IoContext *ioc)
{
    FTRACE
    ioc->pending_syncios.push_back(new kvaio_t(nvme_cmd_kv_delete, keyspaceid, aio_callback, ioc, this));

    kvaio_t* aio = ioc->pending_syncios.back();
    aio->value     = 0;
    aio->vallength = 0;
    aio->valoffset = 0;
    return aio;
}

#endif