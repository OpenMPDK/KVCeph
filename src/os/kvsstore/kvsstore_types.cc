
//
// Created by root on 10/12/18.
//
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <bitset>

#include "KvsStore.h"
#include "kvs_debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_kvs

KvsOpSequencer::KvsOpSequencer(KvsStore *store, const coll_t &c)
    : RefCountedObject(store->cct), store(store), cid(c)
{
   store->register_osr(this);
}


void KvsOpSequencer::_unregister() {
    FTRACE
    if (registered) {
        store->unregister_osr(this);
        registered = false;
    }
}

KvsOpSequencer::~KvsOpSequencer() {
    FTRACE
    assert(q.empty());
    _unregister();
}

#if 0
void KvsOpSequencer::discard() {
    FTRACE
// Note that we may have txc's in flight when the parent Sequencer
// goes away.  Reflect this with zombie==registered==true and let
// _osr_drain_all clean up later.

    assert(!zombie);
    zombie = true;
   // parent = nullptr;
    bool empty;
    {
        std::lock_guard<std::mutex> l(qlock);
        empty = q.empty();
    }
    if (empty) {
        _unregister();
    }
}
#endif

void KvsOpSequencer::drain() {
    FTRACE
    //lderr(store->cct) << __func__ << " ### draining acquiring qlock " << dendl;
    std::unique_lock l{qlock};
    //lderr(store->cct) << __func__ << " ### acquired qlock wait if q.empty = " << q.empty() << dendl;
 // debug layout 
    if (!q.empty()){
        KvsTransContext *txc = &q.back();
        //lderr(store->cct) << __func__ << " q.size() = " << q.size() 
        //    << " txc->state = " << txc->get_state_name() 
        //    << " oncommits.size() = " << txc->oncommits.size() << dendl;
    }
// debug layout remove 
    while (!q.empty())
        qcond.wait(l);
    //lderr(store->cct) << __func__ << " ### released qlock " << dendl;

}

void KvsOpSequencer::drain_preceding(KvsTransContext *txc) {
    FTRACE
    //lderr(store->cct) << __func__ << " ### drain_preceding acquiring qlock " << dendl;
    std::unique_lock l{qlock};
    while (!q.empty() && &q.front() != txc)
        qcond.wait(l);
}

bool KvsOpSequencer::_is_all_kv_submitted() {
    FTRACE
    // caller must hold qlock
    //lderr(store->cct) << __func__ << " ### all qlock " << dendl;
    if (q.empty()) {
        return true;
    }
    KvsTransContext *txc = &q.back();
    if (txc->state >= KvsTransContext::STATE_AIO_WAIT) {
        return true;
    }
    return false;
}

void KvsOpSequencer::flush() {
    FTRACE
    std::unique_lock l{qlock};
    while (true) {
// set flag before the check because the condition
// may become true outside qlock, and we need to make
// sure those threads see waiters and signal qcond.
        ++kv_submitted_waiters;
        if (_is_all_kv_submitted()) {
            return;
        }
        qcond.wait(l);
        --kv_submitted_waiters;
    }
}

#if 0
void flush_all_but_last()
{
    std::unique_lock l(qlock);
    assert(q.size() >= 1);
    while (true)
    {
        // set flag before the check because the condition
        // may become true outside qlock, and we need to make
        // sure those threads see waiters and signal qcond.
        ++kv_submitted_waiters;
        if (q.size() <= 1)
        {
            --kv_submitted_waiters;
            return;
        }
        else
        {
            auto it = q.rbegin();
            it++;
            if (it->state >= KvsTransContext::STATE_KV_SUBMITTED)
            {
                return;
            }
        }
        qcond.wait(l);
        --kv_submitted_waiters;
    }
}
#endif

bool KvsOpSequencer::flush_commit(Context *c) {
    FTRACE
    std::lock_guard l(qlock);
    if (q.empty()) {
        return true;
    }
    KvsTransContext *txc = &q.back();
    if (txc->state >= KvsTransContext::STATE_IO_DONE) {
        return true;
    }
    txc->oncommits.push_back(c);
    return false;
}

KvsCache *KvsCache::create(CephContext* cct, PerfCounters *logger)
{
    KvsCache *c = new KvsLRUCache(cct);
    c->logger = logger;
    c->max_readcache = cct->_conf->kvsstore_readcache_bytes;
    c->max_onodes    = cct->_conf->kvsstore_max_cached_onodes;

    // = 1024*1024*1024
    return c;
}

void KvsCache::trim_all()
{
    std::lock_guard<std::recursive_mutex> l(lock);
    _trim(0, 0);
}

void KvsCache::trim()
{
    std::lock_guard<std::recursive_mutex> l(lock);
    _trim(max_onodes, max_readcache);
}

// LRUCache
#undef dout_prefix
#define dout_prefix *_dout << "KvsStore.LRUCache(" << this << ") "

void KvsLRUCache::_touch_onode(OnodeRef& o)
{
    auto p = onode_lru.iterator_to(*o);
    onode_lru.erase(p);
    onode_lru.push_front(*o);
}

void KvsLRUCache::_touch_data(ReadCacheBufferRef& o)
{
    auto p = buffer_lru.iterator_to(*o);
    buffer_lru.erase(p);
    buffer_lru.push_front(*o);
}

void KvsLRUCache::_trim(uint64_t onode_max, uint64_t buffer_max)
{
    /*derr << __func__ << " onodes " << onode_lru.size() << " / " << onode_max
                    << ", buffers" << buffer_size << "/" << buffer_max << dendl;
    */
    int skipped = 0;
    int max_skipped = 64;

    // free buffers
    auto i = buffer_lru.end();
    if (i != buffer_lru.begin()) {
        --i;
        bool needexit = false;
        // buffers
        while ((buffer_size > buffer_max || buffer_max == 0) && !needexit) {
            ReadCacheBuffer *b = &*i;

            if (b->nref.load() > 1) {
                if (++skipped >= max_skipped) {
                    break;
                }

                if (i == buffer_lru.begin()) {
                    break;
                } else {
                    i--;
                    continue;
                }
            }


            if (i != buffer_lru.begin()) {
                buffer_lru.erase(i--);
            } else {
                buffer_lru.erase(i);
                needexit = true;
            }
            buffer_size -= b->length();

            b->get();  // paranoia
            b->space->remove_data(b->oid);
            b->put();
        }

    }

    // onodes
    int num = onode_lru.size() - onode_max;
    if (num <= 0)
        return; // don't even try

    auto p = onode_lru.end();
    assert(p != onode_lru.begin());
    --p;

    skipped = 0;
    while (num > 0) {
        KvsOnode *o = &*p;
        int refs = o->nref.load();
        if (refs > 1) {
            dout(20) << __func__ << "  " << o->oid << " has " << refs
                     << " refs, skipping" << dendl;
            if (++skipped >= max_skipped) {
                dout(20) << __func__ << " maximum skip pinned reached; stopping with "
                         << num << " left to trim" << dendl;
                break;
            }

            if (p == onode_lru.begin()) {
                break;
            } else {
                p--;
                num--;
                continue;
            }
        }
        dout(20) << __func__ << "  rm " << o->oid << dendl;
        if (p != onode_lru.begin()) {
            onode_lru.erase(p--);
        } else {
            onode_lru.erase(p);
            assert(num == 1);
        }
        o->get();  // paranoia
        o->c->onode_map.remove(o->oid);
        o->put();
        --num;
    }
}




// OnodeSpace

#undef dout_prefix
#define dout_prefix *_dout << "kvsstore.OnodeSpace(" << this << " in " << cache << ") "

OnodeRef KvsOnodeSpace::add(const ghobject_t& oid, OnodeRef o)
{
    std::lock_guard<std::recursive_mutex> l(cache->lock);
    auto p = onode_map.find(oid);
    if (p != onode_map.end()) {
        ldout(cache->cct, 20) << __func__ << " " << oid << "(" << &oid << ") " << o
                              << " raced, returning existing " << p->second
                              << dendl;
        return p->second;
    }
    ldout(cache->cct, 20) << __func__ << " " << oid << "(" << &oid << ") " << o << dendl;
    onode_map[oid] = o;
    cache->_add_onode(o, 1);


    return o;
}

void KvsOnodeSpace::remove(const ghobject_t& oid) { 
    onode_map.erase(oid); 
    //ldout(cache->cct, 20) << __func__ << " " << oid << "(" << &oid << ") " << dendl;
}

void KvsOnodeSpace::add_data(const ghobject_t &oid, ReadCacheBufferRef b)
{
    std::lock_guard<std::recursive_mutex> l(cache->lock);
    auto p = data_map.find(oid);
    if (p != data_map.end()) {
        ldout(cache->cct, 30) << __func__ << " " << oid  << " raced" << dendl;
        return;
    }

    ldout(cache->cct, 30) << __func__ << " " << oid << " " << dendl;

    data_map[oid] = b;
    cache->_add_data(b, 1);
}

bool KvsOnodeSpace::invalidate_data(const ghobject_t &oid)
{
    std::lock_guard<std::recursive_mutex> l(cache->lock);
    auto p = data_map.find(oid);
    if (p != data_map.end()) {
        cache->_rm_data(p->second);
        data_map.erase(p);
        return true;
    }
    return false;
}

bool KvsOnodeSpace::invalidate_onode(const ghobject_t &oid)
{
    std::lock_guard<std::recursive_mutex> l(cache->lock);
    ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
    if (p == onode_map.end()) {
        cache->_rm_onode(p->second);
        return true;
    }
    return false;
}

ReadCacheBufferRef KvsOnodeSpace::lookup_data(const ghobject_t &oid)
{
    ldout(cache->cct, 20) << __func__ << dendl;
    ReadCacheBufferRef o;
    {
        std::lock_guard<std::recursive_mutex> l(cache->lock);
        ceph::unordered_map<ghobject_t,ReadCacheBufferRef>::iterator p = data_map.find(oid);
        if (p == data_map.end()) {
            ldout(cache->cct, 20) << __func__ << " " << oid << " miss" << dendl;
        } else {
            ldout(cache->cct, 20) << __func__ << " " << oid << " hit " << p->second
                                  << dendl;
            cache->_touch_data(p->second);
            o = p->second;
        }
    }

    return o;
}

OnodeRef KvsOnodeSpace::lookup(const ghobject_t& oid)
{
    ldout(cache->cct, 20) << __func__ << dendl;
    OnodeRef o;
    {
        std::lock_guard<std::recursive_mutex> l(cache->lock);
        ceph::unordered_map<ghobject_t,OnodeRef>::iterator p = onode_map.find(oid);
        if (p == onode_map.end()) {
            ldout(cache->cct, 20) << __func__ << " " << oid << " miss, mapsize = " << onode_map.size() << dendl;
        } else {
            ldout(cache->cct, 20) << __func__ << " " << oid << " hit " << p->second
                                  << dendl;
            cache->_touch_onode(p->second);
            o = p->second;
        }
    }

    return o;
}

void KvsOnodeSpace::clear() {
    {
        std::lock_guard<std::recursive_mutex> l(cache->lock);
        ldout(cache->cct, 10) << __func__ << dendl;
        for (auto &p : onode_map) {
            cache->_rm_onode(p.second);
        }
        onode_map.clear();
    }

    {
        std::lock_guard<std::recursive_mutex> l(cache->lock);
        ldout(cache->cct, 10) << __func__ << dendl;
        for (auto &p : data_map) {
            cache->_rm_data(p.second);
        }
        data_map.clear();
    }
}

#undef dout_prefix
#define dout_prefix *_dout << "kvsstore.KvsLRUCache "

void KvsLRUCache::_rm_data(ReadCacheBufferRef& o) {
    auto q = buffer_lru.iterator_to(*o);
    buffer_lru.erase(q);
    buffer_size -= o->length();
}


bool KvsOnodeSpace::empty()
{
    bool b = false;
    {
        std::lock_guard<std::recursive_mutex> l(cache->lock);
        b = onode_map.empty();
    }
    if (b)
    {
        std::lock_guard<std::recursive_mutex> l(cache->lock);
        b = b & data_map.empty();
    }

    return b;
}

bool KvsOnodeSpace::map_any(std::function<bool(OnodeRef)> f)
{
    std::lock_guard<std::recursive_mutex> l(cache->lock);
    ldout(cache->cct, 20) << __func__ << dendl;
    for (auto& i : onode_map) {
        if (f(i.second)) {
            return true;
        }
    }
    return false;
}

void KvsOnodeSpace::dump(CephContext *cct, int lvl)
{
    for (auto& i : onode_map) {
       // ldout(cct, lvl) << i.first << " : " << i.second << dendl;
       ldout(cct, 20) << i.first << " : " << i.second << dendl;
    }
}


#undef dout_prefix
#define dout_prefix *_dout << "kvsstore.onode(" << this << ")." << __func__ << " "

void KvsOnode::flush()
{
    if (flushing_count.load()) {
        //lderr(c->store->cct) << __func__ << " cnt:" << flushing_count << dendl;
        std::unique_lock l(flush_lock);
        while (flushing_count.load()) {
            flush_cond.wait(l);
        }
    }
}


#undef dout_prefix
#define dout_prefix *_dout << "kvsstore.OmapIteratorImpl "

/*

void omap_iterator_init(CephContext *cct, uint64_t lid, kv_iter_context *iter_ctx){

    kvs_omap_key_header hdr = { GROUP_PREFIX_OMAP, lid};
    iter_ctx->prefix =  ceph_str_hash_linux((char*)&hdr, sizeof(struct kvs_omap_key_header));
    iter_ctx->bitmask = 0xFFFFFFFF;
    iter_ctx->buflen = ITER_BUFSIZE;
}

void print_iterKeys(CephContext *cct, std::map<string, int> &keylist){
	for (std::map<string, int> ::iterator it=keylist.begin(); it!=keylist.end(); ++it){
                derr << __func__ << " Iter keylist: Key = " << (it->first) <<
			" => " << (uint32_t)it->second  << dendl;}
}
*/

///
/// KvsOmapIteratorImpl
///


KvsOmapIterator::KvsOmapIterator(
        CollectionRef c, OnodeRef o, KvsStore *s)
        : c(c), o(o), store(s)
{
}
void KvsOmapIterator::makeready()
{
    it = keylist.begin();
    // remove header from the list

    if (valid() && (*it).length() == 0) {
        keylist.erase(it);
        it = keylist.begin();
    }

}
int KvsOmapIterator::seek_to_first()
{
   // lderr(store->cct) << __func__ << " before keylist " << dendl;
    it = keylist.begin();
    if (o->onode.has_omap()){ 
       // lderr(store->cct) << __func__ << " has omap " << dendl;
        it = keylist.begin();
        }
    else{ //lderr(store->cct) << __func__ << " does not contain omaps " << dendl;
     it = std::set<string>::iterator(); }
    return 0;
}

 bool KvsOmapIterator::header(bufferlist &hdr)
{
    kv_result res = c->store->db.sync_read(c->store->kvcmds.read_omap(o->oid, o->onode.lid, ""), hdr);

    return (res == 0);

}

int KvsOmapIterator::upper_bound(const string& after)
{
    if (o->onode.has_omap()){
        it = std::upper_bound(keylist.begin(), keylist.end(), after);
    }
    else{ it = std::set<string>::iterator();}
    return 0;
}

int KvsOmapIterator::lower_bound(const string& to)
{
    if(o->onode.has_omap()){
        it = std::lower_bound(keylist.begin(), keylist.end(), to);
    }
    else{
        it = std::set<string>::iterator();
    }
    return 0;
}

bool KvsOmapIterator::valid()
{
    const bool iter = (it != keylist.end() && it != std::set<string>::iterator());
    return o->onode.has_omap() && iter;
}

int KvsOmapIterator::next()
{
    if (!o->onode.has_omap())
        return -1;

    if (it != keylist.end()) {
        ++it;
        return 0;
    }

    return -1;
}

string KvsOmapIterator::key()
{
    if (it != keylist.end() || it != std::set<string>::iterator())
    	 return *it;
    else
         return  "";
}

bufferlist KvsOmapIterator::value()
{
    bufferlist output;

    if(!(it != keylist.end() || it != std::set<string>::iterator())) {
        return output;
    }

    const string user_key = *it;

    kv_result res = c->store->db.sync_read(c->store->kvcmds.read_omap(o->oid, o->onode.lid, user_key), output);

	if (res != 0) {
        lderr(c->store->cct) << __func__ << ": sync_read failed res = " << res << ", bufferlist length = " << output.length()  << dendl;
        output.clear();
    }

    return output;
}


KvsOmapIteratorImpl::KvsOmapIteratorImpl(
        CollectionRef c, KvsOmapIterator *it_)
        : c(c), it(it_)
{

}

int KvsOmapIteratorImpl::seek_to_first()
{
    //lderr(c->store->cct) << __func__ << "  seek_to_first  before collection lock "<< dendl;
    RWLock::RLocker l(c->lock);
    //lderr(c->store->cct) << __func__ << "  seek_to_first collection lock "<< dendl;
    return it->seek_to_first();
}

int KvsOmapIteratorImpl::upper_bound(const string& after)
{
    RWLock::RLocker l(c->lock);
    return it->upper_bound(after);

}

int KvsOmapIteratorImpl::lower_bound(const string& to)
{
    RWLock::RLocker l(c->lock);
    return it->lower_bound(to);
}

bool KvsOmapIteratorImpl::valid()
{
    RWLock::RLocker l(c->lock);
    return it->valid();
}

int KvsOmapIteratorImpl::next()
{
    RWLock::RLocker l(c->lock);
    return it->next();
}

string KvsOmapIteratorImpl::key()
{
    RWLock::RLocker l(c->lock);
    return it->key();
}
 
bufferlist KvsOmapIteratorImpl::value()
{
    RWLock::RLocker l(c->lock);
    return it->value();
}

#undef dout_prefix
#define dout_prefix *_dout << "kvsstore "


///
/// Transaction Contexts
///


void KvsTransContext::aio_finish(kv_io_context *op) {
    store->txc_aio_finish(op, this);
}



