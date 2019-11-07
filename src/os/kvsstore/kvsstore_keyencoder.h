/*
 * keyencoder.h
 *
 *  Created on: Aug 14, 2019
 *      Author: root
 */

#ifndef OS_KVSSTORE_KVSSTORE_KEYENCODER_H_
#define OS_KVSSTORE_KVSSTORE_KEYENCODER_H_


inline void append_escaped(const string &in, string *out)
{
    char hexbyte[8];
    for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
        if (*i <= '#') {
            snprintf(hexbyte, sizeof(hexbyte), "#%02x", (uint8_t)*i);
            out->append(hexbyte);
        } else if (*i >= '~') {
            snprintf(hexbyte, sizeof(hexbyte), "~%02x", (uint8_t)*i);
            out->append(hexbyte);
        } else {
            out->push_back(*i);
        }
    }
    out->push_back('!');
}


inline char *append_escaped(const string &in, char *key)
{
    char *pos = key;
    for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
        if (*i <= '#') {
            pos += snprintf(pos, 8, "#%02x", (uint8_t)*i);

        } else if (*i >= '~') {
            pos += snprintf(pos, 8, "~%02x", (uint8_t)*i);
        } else {
            *pos = *i;
            pos += 1;
        }
    }
    *pos = '!';
    pos += 1;
    return pos;
}

inline int decode_escaped(const char *p, string *out)
{
    const char *orig_p = p;
    while (*p && *p != '!') {
        if (*p == '#' || *p == '~') {
            unsigned hex;
            int r = sscanf(++p, "%2x", &hex);
            if (r < 1)
                return -EINVAL;
            out->push_back((char)hex);
            p += 2;
        } else {
            out->push_back(*p++);
        }
    }
    return p - orig_p;
}

#if 0
void encode_nspace_oid(const std::string &nspace, const std::string &oidkey, const std::string &oidname, std::string *out)
{
    out->clear();
    append_escaped(nspace, out);

    if (oidkey.length()) {
        // is a key... could be < = or >.
        // (ASCII chars < = and > sort in that order, yay)
        if (oidkey < oidname) {
            out->append("<");
            append_escaped(oidkey, out);
            append_escaped(oidname, out);
        } else if (oidkey > oidname) {
            out->append(">");
             append_escaped(oidkey, out);
            append_escaped(oidname, out);
        } else {
            // same as no key
            out->append("=");
            append_escaped(oidname, out);
        }
    } else {
        // no key
        out->append("=");
        append_escaped(oidname, out);
    }
}
/*
bool construct_ghobject_t(CephContext* cct, const char *key, int keylength, ghobject_t* oid);
void print_kvskey(char *s, int length, const char *header, std::string &out);
void encode_nspace_oid(const std::string &nspace, const std::string &oidkey, const std::string &oidname, std::string *out);
uint32_t get_object_group_id(const uint8_t  isonode,const int8_t shardid, const uint64_t poolid);
// OMAP Iterator helpers
uint8_t construct_omap_key(CephContext* cct, uint64_t lid, const char *name, const int name_len, void *keybuffer, uint8_t space_id);
bool belongs_toOmap(void *key, uint64_t lid);
void omap_iterator_init(CephContext *cct, uint64_t lid, kv_iter_context *iter_ctx);
void print_iterKeys(CephContext *cct, std::map<string, int> &keylist);
int populate_keylist(CephContext *cct, uint64_t lid, std::list<std::pair<malloc_unique_ptr<char>, int> > &buflist, std::set<string> &keylist);
*/

#endif

inline char *encode_nspace_oid(const std::string &nspace, const std::string &oidkey, const std::string &oidname, char *pos)
{
    pos = append_escaped(nspace, pos);

    if (oidkey.length()) {
        // is a key... could be < = or >.
        // (ASCII chars < = and > sort in that order, yay)
        if (oidkey < oidname) {
            *pos++ = '<';
            pos = append_escaped(oidkey, pos);
            pos = append_escaped(oidname, pos);
        } else if (oidkey > oidname) {
            *pos++ = '>';
            pos = append_escaped(oidkey, pos);
            pos = append_escaped(oidname, pos);
        } else {
            // same as no key
            *pos++ = '=';
            pos = append_escaped(oidname, pos);
        }
    } else {
        // no key
        *pos++ = '=';
        pos = append_escaped(oidname, pos);
    }

    return pos;
}

inline int decode_nspace_oid(char *p, ghobject_t* oid)
{
    int r;
    p += decode_escaped(p, &oid->hobj.nspace) + 1;

    if (*p == '=') {
        // no key
        ++p;
        r = decode_escaped(p, &oid->hobj.oid.name);
        if (r < 0)
            return -7;
        p += r + 1;
    } else if (*p == '<' || *p == '>') {
        // key + name
        ++p;
        string okey;
        r = decode_escaped(p, &okey);
        if (r < 0)
            return -8;
        p += r + 1;
        r = decode_escaped(p, &oid->hobj.oid.name);
        if (r < 0)
            return -9;
        p += r + 1;
        oid->hobj.set_key(okey);
    } else {
        // malformed
        return -10;
    }

    return 0;

}


struct kvs_key_header
{
    uint8_t  hdr;
    uint8_t  isonode;
    int8_t  shardid;
    uint64_t poolid;
};


inline bool construct_ghobject_t(CephContext* cct, const char *key, int keylength, ghobject_t* oid) {

    struct kvs_var_object_key* kvskey = (struct kvs_var_object_key*)key;
    if (kvskey->group != GROUP_PREFIX_DATA && kvskey->group != GROUP_PREFIX_ONODE) return false;

    oid->shard_id.id = kvskey->shardid;
    if (oid->hobj.is_temp())

    oid->hobj.pool = kvskey->poolid - 0x8000000000000000ull;
    oid->hobj.set_bitwise_key_u32(kvskey->bitwisekey);
    oid->hobj.snap.val = kvskey->snapid;
    oid->generation = kvskey->genid;

    if (decode_nspace_oid(&kvskey->name[0], oid) < 0) {
        // panic: name is too long
    	std::cerr << "name is too long:" << print_key((char*)key, keylength) << std::endl;
        ceph_abort();
    }
    return true;
}



inline void construct_sb_key(kv_key *key) {
    memset((void*)key->key, 0, 16);
    struct kvs_sb_key* kvskey = (struct kvs_sb_key*)key->key;
    kvskey->prefix = GROUP_PREFIX_SUPER;
    kvskey->group  = GROUP_PREFIX_SUPER;
    sprintf(kvskey->name, "%s", "kvsb");
    key->length = 16;
}

inline uint8_t construct_collkey_impl(void *buffer, const char *name, const int namelen)
{
    struct kvs_coll_key *collkey = (struct kvs_coll_key *)buffer;

    collkey->hash  = GROUP_PREFIX_COLL;
    collkey->group = GROUP_PREFIX_COLL;

    memcpy(collkey->name, name, namelen);

    return (5 + namelen);
}

inline uint8_t construct_omap_key(CephContext* cct, uint64_t lid, const char *name, const int name_len, void *keybuffer, uint8_t space_id){
    kvs_omap_key_header hdr = { GROUP_PREFIX_OMAP, lid};

    struct kvs_omap_key* kvskey = (struct kvs_omap_key*)keybuffer;
    kvskey->hash = ceph_str_hash_linux((char*)&hdr, sizeof(struct kvs_omap_key_header));
    kvskey->group= GROUP_PREFIX_OMAP;
    kvskey->lid  = lid;
// TODO: remove space id
    kvskey->spaceid = space_id;

    if (name_len == 0) {
        kvskey->isheader = 1;
        return 15;
    } else {
        kvskey->isheader = 0;
        memcpy(kvskey->name, name, name_len);
        return (15 + name_len);
    }
}

inline uint32_t get_object_group_id(const uint8_t  isonode,const int8_t shardid, const uint64_t poolid) {
	struct kvs_key_header hdr = { GROUP_PREFIX_DATA, isonode, shardid, poolid };
	return ceph_str_hash_linux((char*)&hdr, sizeof(struct kvs_key_header));
}

//#undef dout_prefix
//#define dout_prefix *_dout << "kvsstore "

inline uint8_t _construct_var_object_key_impl(CephContext* cct, const uint8_t keyprefix, const bool isonode, const ghobject_t& oid, void *keybuffer, uint8_t space_id) {
    struct kvs_var_object_key* kvskey = (struct kvs_var_object_key*)keybuffer;

    kvskey->group = (isonode)? GROUP_PREFIX_ONODE:GROUP_PREFIX_DATA;
    kvskey->shardid = int8_t(oid.shard_id);
    kvskey->poolid  = oid.hobj.pool + 0x8000000000000000ull;
// TODO: remove space id
    kvskey->spaceid = space_id;

    kvskey->bitwisekey = oid.hobj.get_bitwise_key_u32();
    kvskey->snapid = uint64_t(oid.hobj.snap);
    kvskey->genid = (uint64_t)oid.generation;
    kvskey->grouphash = get_object_group_id (kvskey->group, kvskey->shardid, kvskey->poolid);
  
    char *pos = encode_nspace_oid(oid.hobj.nspace, oid.hobj.get_key(), oid.hobj.oid.name, &kvskey->name[0]);

    const int namelen = (pos - &kvskey->name[0]);

   // if (isonode){
  //      std::cerr << " onode keylength, " << (35 + namelen) << std::endl;
  //      lderr(cct) << " onode keylength, " << (35 + namelen) << dendl;
   //  }
   
    if (namelen > 220) {
        // panic: name is too long
    	std::cerr << "name is too long" << std::endl;
        ceph_abort();
    }
    return (35 + namelen);
}

inline uint8_t construct_var_object_key(CephContext* cct, const uint8_t keyprefix, const ghobject_t& oid, void *keybuffer, uint8_t space_id) {
	return _construct_var_object_key_impl(cct, keyprefix, false, oid, keybuffer, space_id);
}

inline uint8_t construct_var_onode_key(CephContext* cct, const uint8_t keyprefix, const ghobject_t& oid, void *keybuffer, uint8_t space_id) {
    return  _construct_var_object_key_impl(cct, keyprefix, true, oid, keybuffer, space_id);
}


#endif /* OS_KVSSTORE_KVSSTORE_KEYENCODER_H_ */
