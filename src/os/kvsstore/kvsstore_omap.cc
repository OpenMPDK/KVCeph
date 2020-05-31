#include "kvsstore_omap.h"
#include "kvsstore_types.h"

// 5K
#define OMAP_WRITEBUFER_LEN 5120


void kvsstore_omap_list::insert_keys_from_buffer(char *buf, uint32_t endpos, std::set<std::string> &to) {
    uint32_t curpos = 0;
    while (curpos < endpos) {
        int stringlen = *(uint8_t*) (buf + curpos); curpos += sizeof(uint8_t);
        std::string str(buf + curpos, stringlen); curpos += stringlen;
        //TR3 << "found " << str;

        to.insert(std::move(str));
    }
}

void kvsstore_omap_list::serialize_key(const std::string &key, bufferptr &buffer) {
    const uint8_t klen= key.length();
    buffer.append((const char*)&klen, sizeof(uint8_t));
    buffer.append(key.c_str(), klen);
}


void kvsstore_omap_list::load_omap(CephContext* cct, const readfunc_t &reader) {

    //TRU << "loading omap: buffer length " << onode->omap_bp.length() << ", omaps = " << onode->omaps.size();

    if (!onode->omap_loaded && onode->num_omap_extents > 0) {
        TRU << "loading omap: num extents = " << (int)onode->num_omap_extents;

        std::vector<bufferlist*> bls;
        onode->omap_loaded = reader(onode->nid, 0, onode->num_omap_extents, bls);

        if (!onode->omap_loaded) {
            TRERR << "omap load failed: nid = " << onode->nid << ", num extents = " << onode->num_omap_extents;
            throw std::runtime_error("omap load failed");
        };

        bufferlist data;

        for (bufferlist *bl : bls) {
            decompress(cct,*bl, data);
            TRU << "decompress: before " << bl->length() << ", after " << data.length();

            insert_keys_from_buffer(data.c_str(), data.length(), onode->omaps);
            delete bl;
        }

    }

    if (onode->omap_bp.get_raw() != nullptr) {

        TRU << "remaining data = " << onode->omap_bp.length();
        insert_keys_from_buffer(onode->omap_bp.c_str(), onode->omap_bp.length(), onode->omaps);
        onode->omap_bp.set_length(0);
    }
    TRU << "omap size = " << onode->omaps.size();

    // clear the buffer
    onode->omap_loaded = true;
}

// { "snappy",	COMP_ALG_SNAPPY },
// { "zlib",	COMP_ALG_ZLIB },
// { "zstd",	COMP_ALG_ZSTD },
int kvsstore_omap_list::compress(CephContext* cct, bufferlist &in, bufferlist &out) {
    return cp->compress(in, out);
}

int kvsstore_omap_list::decompress(CephContext* cct, bufferlist &in, bufferlist &out) {
    return cp->decompress(in, out);
}

void kvsstore_omap_list::flush(CephContext* cct, IoContext &ioc, const writefunc_t &writer, std::vector<bufferlist*> &tempbuffers)
{
    if (onode->omap_dirty) {
        TRU << "omap is dirty " << onode->omap_dirty;
        uint32_t max_buffersize = onode->omaps.size() * 256;
        bufferptr buffer  = buffer::create_small_page_aligned(max_buffersize);
        buffer.set_length(0);

        for (const std::string &k : onode->omaps) {
            serialize_key(k, buffer);
        }

        bufferlist in;
        in.push_back(std::move(buffer));

        bufferlist *out = new bufferlist();
        compress (cct,in, *out);
        tempbuffers.push_back(out);

        TRU << "compressed data: " << in.length() << "->" << out->length();

        writer(onode->nid, out, onode->num_omap_extents, ioc);

        onode->num_omap_extents = 1;
        onode->omaps.clear();
        onode->omap_loaded = false;
    }
}

void kvsstore_omap_list::insert(CephContext* cct, const std::string &key, const readfunc_t &reader) {
    if (onode->omap_loaded) {
        TRU << "insert to loaded map";
        onode->omaps.insert(key);
        onode->omap_dirty = true;
    }
    else {
        // create a write buffer if needed
        if (onode->omap_bp.get_raw() == nullptr) {
            onode->omap_bp = buffer::create_small_page_aligned(OMAP_WRITEBUFER_LEN);
            onode->omap_bp.set_length(0);
        }

        if (onode->omap_bp.length() + key.length() + sizeof(uint8_t) > OMAP_WRITEBUFER_LEN) {
            TRU << "write buffer is full";
            // case 1. write buffer is full -> load the map and write to the map, and reset write buffer
            load_omap(cct, reader);
            onode->omaps.insert(key);
            onode->omap_dirty = true;
        } else {
            // case 2. write to a buffer
            //TRU << "WB add:" << key << ", klen " << (int)key.length();
            serialize_key(key, onode->omap_bp);
        }
    }
};

void kvsstore_omap_list::clear(IoContext &ioc, const removefunc_t &removefunc)
{
    TRU << "clear";
    // clear write buffer
    onode->omap_bp.set_length(0);
    onode->omaps.clear();
    onode->omap_dirty = false;
    if (onode->num_omap_extents > 0) {
        removefunc(onode->nid, 0, onode->num_omap_extents, ioc);
        onode->num_omap_extents = 0;
    }
}

bool kvsstore_omap_list::erase (CephContext *cct, const std::string &key, const readfunc_t &reader) {
    TRU << "dirty due to erase";
    if (!onode->omap_loaded) {
        load_omap(cct, reader);
    }

    const bool erased = (onode->omaps.erase(key) > 0);
    onode->omap_dirty |= erased;

    return erased;
};

bool kvsstore_omap_list::erase(CephContext *cct, const std::string &first, const std::string &last, const readfunc_t &reader, const listenerfunc_t &listener) {
    TRU << "dirty due to erase";
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
    if (!onode->omap_loaded) {
        load_omap(cct, reader);
    }
    out->insert(onode->omaps.begin(), onode->omaps.end());
    return 0;
}

int kvsstore_omap_list::lookup(CephContext *cct,const std::set<std::string> &keys, std::set<std::string> *out, const readfunc_t &reader)
{
    if (!onode->omap_loaded) {
        load_omap(cct, reader);
    }
    for (const std::string &key : keys) {
        if (onode->omaps.find(key) != onode->omaps.end())
            out->insert(key);
    }
    return 0;
}
