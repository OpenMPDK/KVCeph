#include "kvsstore_omap.h"
#include "kvsstore_types.h"
#include "kvsstore_debug.h"
// 5K
#define OMAP_WRITEBUFER_LEN 8192
#define KVS_COMP

int kvsstore_omap_list::compress(CephContext* cct, bufferlist &in, bufferlist &out) {
    out.clear();
    return cp->compress(in, out);
}

int kvsstore_omap_list::decompress(CephContext* cct, bufferlist &in, bufferlist &out) {
    out.clear();
    return cp->decompress(in, out);
}



void kvsstore_omap_list::load_omap(CephContext* cct, const readfunc_t &reader) {
    FTRACE

    if (onode->omap_loaded) return;

//    TRU << "loading omap: buffer length " << onode->omap_bp.length() << ", omaps = " << onode->omaps.size();
    if (onode->num_omap_extents > 0) {

        std::vector<bufferlist*> bls;
        onode->omap_loaded = reader(onode->nid, 0, onode->num_omap_extents, bls);

        if (!onode->omap_loaded) {
            TRERR << "omap load failed: nid = " << onode->nid << ", num extents = " << onode->num_omap_extents;
            throw std::runtime_error("omap load failed");
        };

        bufferlist data;

        for (bufferlist *bl : bls) {
#ifdef KVS_COMP
            decompress(cct,*bl, data);
            TRU << "decompress: before " << bl->length() << ", after " << data.length();

            insert_keys_from_buffer(data.c_str(), data.length(), onode->omaps);
#else
            insert_keys_from_buffer(bl->c_str(), bl->length(), onode->omaps);
#endif
            delete bl;
        }
    }

    if (onode->omap_wb.have_raw() && onode->omap_wb.length() > 0) {
        insert_keys_from_buffer(onode->omap_wb.c_str(), onode->omap_wb.length(), onode->omaps);
        bufferlist l;
        l.push_back(std::move(onode->omap_wb));
    }

    onode->omap_loaded = true;
}

void kvsstore_omap_list::flush(CephContext* cct, IoContext &ioc, const writefunc_t &writer, std::vector<bufferlist*> &tempbuffers)
{
    FTRACE
    if (onode->omap_dirty) {
        if (onode->omaps.size() > 0) {
            uint32_t max_buffersize = onode->omaps.size() * 256;
            bufferptr buffer = buffer::create_small_page_aligned(max_buffersize);
            buffer.set_length(0);

            for (const std::string &k : onode->omaps) {
                serialize_key(k, buffer);
            }

            bufferlist *out = new bufferlist();
#ifdef KVS_COMP
            bufferlist in;
            in.push_back(std::move(buffer));
            compress (cct,in, *out);
            tempbuffers.push_back(out);
            TRC << "compressed data: " << in.length() << "->" << out->length() << ", number of keys " << onode->omaps.size();
#else
            out->push_back(std::move(buffer));
            TRC << "not compressed data: " << out->length() << ", number of keys " << onode->omaps.size();
#endif
            writer(onode->nid, out, onode->num_omap_extents, ioc);

            onode->num_omap_extents = 1;

        }
        onode->omap_dirty = false;
    }
}

void kvsstore_omap_list::insert(CephContext* cct, const std::string &key, const readfunc_t &reader) {
    FTRACE
    if (!onode->omap_loaded) {
        if (!onode->omap_wb.get_raw()) {
            onode->omap_wb = buffer::create_small_page_aligned(OMAP_WRITEBUFER_LEN);
            onode->omap_wb.set_length(0);
        }

        if (onode->omap_wb.length() + key.length() + 1 <= OMAP_WRITEBUFER_LEN) {
            serialize_key(key, onode->omap_wb);
            return;
        }
        else
            load_omap(cct, reader);
    }

    if (onode->omap_loaded) {
        onode->omaps.insert(key);
        onode->omap_dirty = true;
    }
};

void kvsstore_omap_list::clear(IoContext &ioc, const removefunc_t &removefunc)
{
    FTRACE
    // clear write buffer
    //if (onode->omap_keys.length() > 0 && onode->omap_keys.get_num_buffers() > 0)
    //    onode->omap_keys.clear();
    if (onode->omap_wb.have_raw()) {
        bufferlist l;
        l.push_back(std::move(onode->omap_wb));
    }
    onode->omaps.clear();
    onode->omap_dirty = false;
    if (onode->num_omap_extents > 0) {
        removefunc(onode->nid, 0, onode->num_omap_extents, ioc);
        onode->num_omap_extents = 0;
    }

}

bool kvsstore_omap_list::erase (CephContext *cct, const std::string &key, const readfunc_t &reader) {
    FTRACE

    if (!onode->omap_loaded) {
        load_omap(cct, reader);
    }

    const bool erased = (onode->omaps.erase(key) > 0);
    onode->omap_dirty |= erased;

    return erased;
};

bool kvsstore_omap_list::erase(CephContext *cct, const std::string &first, const std::string &last, const readfunc_t &reader, const listenerfunc_t &listener) {
    FTRACE

    if (!onode->omap_loaded) {
        load_omap(cct, reader);
    }

    auto before = onode->omaps.size();
    std::set<std::string>::iterator p = onode->omaps.lower_bound(first);
    std::set<std::string>::iterator e = onode->omaps.lower_bound(last);

    while (p != e) {
        listener(*p);
        onode->omaps.erase(p++);
    }
    auto erased = (onode->omaps.size() - before > 0);
    onode->omap_dirty |= erased;
    return erased;
}

int kvsstore_omap_list::list(CephContext *cct,std::set<std::string> *out, const readfunc_t &reader){
    FTRACE
    if (!onode->omap_loaded) {
        load_omap(cct, reader);
    }
    out->insert(onode->omaps.begin(), onode->omaps.end());
    TRU << "list done: output = " << out->size();
    return 0;
}

int kvsstore_omap_list::lookup(CephContext *cct,const std::set<std::string> &keys, std::set<std::string> *out, const readfunc_t &reader)
{
    FTRACE
    if (!onode->omap_loaded) {
        load_omap(cct, reader);
    }
    for (const std::string &key : keys) {
        TRU << "lookup " << key;
        if (onode->omaps.find(key) != onode->omaps.end())
            out->insert(key);
    }
    TRU << "lookup done: output = " << out->size();
    return 0;
}

void kvsstore_omap_list::insert_keys_from_buffer(char *buf, uint32_t endpos, std::set<std::string> &to) {
    FTRACE
    uint32_t curpos = 0;
    while (curpos < endpos) {
        int stringlen = *(uint8_t*) (buf + curpos); curpos += sizeof(uint8_t);
        std::string str(buf + curpos, stringlen); curpos += stringlen;
        //TR3 << "found " << str;

        to.insert(std::move(str));
    }
}

void kvsstore_omap_list::serialize_key(const std::string &key, bufferptr &buffer) {
    FTRACE
    const uint8_t klen= key.length();
    buffer.append((const char*)&klen, sizeof(uint8_t));
    buffer.append(key.c_str(), klen);
    //TRU << "serialize key: " << key << ", buffer length = " << buffer.length();
}