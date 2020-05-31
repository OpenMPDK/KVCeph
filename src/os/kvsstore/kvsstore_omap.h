//
// Created by root on 5/28/20.
//

#ifndef CEPH_KVSSTORE_OMAP_H
#define CEPH_KVSSTORE_OMAP_H

#include "include/buffer.h"
#include "compressor/Compressor.h"
#include <set>
#include <functional>

#define DEFAULT_OMAPBUF_SIZE 8192U

struct IoContext;

struct kvsstore_omap_list {
private:
    struct kvsstore_onode_t *onode;
    CompressorRef cp;
    typedef std::function< void (uint64_t, bufferlist*, int, IoContext &ctx)> writefunc_t;
    typedef std::function< bool (uint64_t, int, int, std::vector<bufferlist*>&)> readfunc_t;
    typedef std::function< void (uint64_t, int, int, IoContext &ctx)> removefunc_t;
    typedef std::function< void (const std::string &key)> listenerfunc_t;

public:

    kvsstore_omap_list(struct kvsstore_onode_t *onode_, CompressorRef cp_): onode(onode_) {
        cp = cp_;
    }

    void insert(CephContext* cct, const std::string &key, const readfunc_t &reader);
    void load_omap(CephContext* cct); //, const readfunc_t &reader);
    void flush(CephContext* cct, IoContext &ioc, const writefunc_t &writer, std::vector<bufferlist*> &tempbuffers);
    int  compress(CephContext* cct, bufferlist &in, bufferlist &out);
    int  decompress(CephContext* cct, bufferlist &in, bufferlist &out);
    bool erase (CephContext* cct, const std::string &key, const readfunc_t &readfunc);
    bool erase(CephContext* cct, const std::string &first, const std::string &last, const readfunc_t &readfunc, const listenerfunc_t &listener);
    void clear(IoContext &ioc, const removefunc_t &removefunc);
    int list(CephContext* cct,std::set<std::string> *out, const readfunc_t &readfunc);
    int lookup(CephContext* cct,const std::set<std::string> &keys, std::set<std::string> *out, const readfunc_t &readfunc);
    void insert_keys_from_buffer(char *buf, uint32_t endpos, std::set<std::string> &to);
    void serialize_key(const std::string &key, bufferptr &buffer);

/*
    int lookup(const std::set<std::string> &keys, std::set<std::string> *out, const readfunc_t &readfunc){ return 0; }
    int list(std::set<std::string> *out, const readfunc_t &readfunc){ return 0; }
    void prepare_encoding(const std::function< void (uint64_t, std::vector<bufferlist*>&, int , IoContext &ctx)> &writefunc, const removefunc_t &removefunc){ return; }
    bool erase (const std::string &key, const readfunc_t &readfunc) { return true; }
    bool erase(const std::string &first, const std::string &last, const readfunc_t &readfunc, const listenerfunc_t &listener){ return true; }

    kvsstore_omap_list(struct kvsstore_onode_t *onode_):onode(onode_) {

    }
    void insert( const std::string &key) {
        return;
    }
    void clear(const removefunc_t &removefunc) {
        return;
    }
    */
};



#if 0
struct kvsstore_omap_list {
private:
    struct kvsstore_onode_t *onode;
    typedef std::function< bool (uint64_t, int, int, std::vector<bufferlist*>&)> readfunc_t;
    typedef std::function< void (uint64_t, std::vector<bufferlist*>&, int )> writefunc_t;
    typedef std::function< void (uint64_t, int,int )> removefunc_t;
    typedef std::function< void (const std::string &key)> listenerfunc_t;
public:
    const uint32_t max_cache_size = 15;

    kvsstore_omap_list(struct kvsstore_onode_t *onode_): onode(onode_) {}

    int lookup(const std::set<std::string> &keys, std::set<std::string> *out, const readfunc_t &readfunc);
    int list(std::set<std::string> *out, const readfunc_t &readfunc);
    void prepare_encoding(const writefunc_t &writefunc, const removefunc_t &removefunc);
    bool erase(const std::string &first, const std::string &last, const readfunc_t &readfunc, const listenerfunc_t &listener);
    bool erase (const std::string &key, const readfunc_t &readfunc);
    void insert( const std::string &key);
    void clear(const removefunc_t &removefunc);
private:
    const std::set<std::string>* load(const readfunc_t &readfunc);
    void prepare_writebuffer(std::set<std::string> &keylist, std::vector<bufferlist*>& iolist);
    void populate_omaps(std::vector<bufferlist*>& iolist);
    void flush_keylist(std::set<std::string> &keylist, const writefunc_t &writefunc);
};
#endif


#endif //CEPH_KVSSTORE_OMAP_H

