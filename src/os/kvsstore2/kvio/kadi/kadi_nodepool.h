/*
 * kadi_nodepool.h
 *
 *  Created on: Nov 9, 2019
 *      Author: root
 */

#ifndef SRC_KADI_KADI_NODEPOOL_H_
#define SRC_KADI_KADI_NODEPOOL_H_

#include <atomic>
#include <unordered_map>
#include "kadi_cmds.h"
#include "kadi_types.h"
#include "../../kvsstore_debug.h"

#define DATANODE_BITMAP_CNT 8
typedef uint64_t bp_addr_t;

enum {
	NODE_OP_NOP = -1,
	NODE_OP_WRITE = 0,
	NODE_OP_DELETE = 1,
    NODE_TYPE_INVALID = UINT16_MAX,
	NODE_TYPE_TREE = UINT16_MAX -1,
	NODE_TYPE_META = UINT16_MAX -2,
	NODE_TYPE_DATA = UINT16_MAX -3,

};

struct _bp_addr_t
{
	uint64_t pageid:48;
	uint64_t offset:16;
};


struct __attribute__((packed)) datanode_header {
	uint64_t bitmap[DATANODE_BITMAP_CNT];
};

struct __attribute__((packed)) bptree_node_header {
	bp_addr_t self;
	bp_addr_t parent;
	bp_addr_t prev;
	bp_addr_t next;
	uint32_t type;
	uint32_t children;
};

struct bptree_param
{
	int treenode_block_size;
	int datanode_block_size;
	int max_order;
	int max_entries;

	const static int fragment_size  = 64;
	const static int fragment_shift = 6;
	const static int datanode_header_size = sizeof(struct datanode_header);

	bptree_param(int block_size_):
		treenode_block_size(block_size_),
		datanode_block_size(block_size_)
	{
		const int tree_node_body_size = (treenode_block_size - sizeof(bptree_node_header));
		max_order   = tree_node_body_size / (sizeof(bp_addr_t) + sizeof(bp_addr_t));
		max_entries = tree_node_body_size / (sizeof(bp_addr_t));	// no value
	}
};

static inline uint64_t bpaddr_pageid(const bp_addr_t &addr)  {
	return (addr >> 16);
}

static inline uint16_t bpaddr_slotid(const bp_addr_t &addr)  {
	return (addr & 0xFFFF);
}

static inline uint16_t fragid_to_offset(const uint16_t fragid)  {
	return (fragid << bptree_param::fragment_shift);
}

static inline int keylength_to_fragments(const int length) {
	return ((length -1) >> bptree_param::fragment_shift) + 1;
}

static inline bp_addr_t create_key_addr(const uint64_t pageid, const uint16_t slotid)  {
    bp_addr_t addr = (slotid & 0xFFFF);
    addr |= (pageid & 0xFFFFFFFFFFFFFFFFull ) << 16;
    return addr;
}



static inline bp_addr_t create_metanode_addr(int treeindex)  {
	return create_key_addr(treeindex, NODE_TYPE_META);
}

static inline bp_addr_t create_treenode_addr(const uint64_t pageid)  {
	return create_key_addr(pageid,NODE_TYPE_TREE);
}

static inline bp_addr_t create_datanode_addr(const uint64_t pageid)  {
	return create_key_addr(pageid, NODE_TYPE_DATA);
}

static inline std::string bpaddr_slottype(const bp_addr_t &addr) {
    const int slotid = bpaddr_slotid(addr);
    if (slotid == NODE_TYPE_INVALID) return "INVALID";
    if (slotid == NODE_TYPE_DATA) return "DATA NODE";
    if (slotid == NODE_TYPE_TREE) return "TREE NODE";
    if (slotid == NODE_TYPE_META) return "META NODE";
    else {
        return " key slot # " + std::to_string(slotid);
    }
}

static inline std::string desc(const bp_addr_t &addr)  {
    std::stringstream ss;
    ss << "addr = " << addr << ": pageid " << bpaddr_pageid(addr) << ", slot type " << bpaddr_slottype(addr);
    return ss.str();
}

const static bp_addr_t invalid_key_addr = create_key_addr(0, NODE_TYPE_INVALID);


class kv_indexnode {
public:
	int op = -1;	// -1: do nothing, 0: write , 1 delete
	bp_addr_t addr;
	char *buffer;
	int buffer_size;

    kv_indexnode(const bp_addr_t &addr_):
            addr(addr_), buffer(0), buffer_size(0) {
    }

	kv_indexnode(const bp_addr_t &addr_, char *buffer_, int buffer_size_):
		addr(addr_), buffer(buffer_), buffer_size(buffer_size_) {
	}

	void set_dirty() {
		op = NODE_OP_WRITE;
	}

	void set_invalid() {
		op = NODE_OP_DELETE;
	}

	virtual ~kv_indexnode() {
		free(buffer);
	}

	inline bool is_treenode() {
		return bpaddr_slotid(addr) == NODE_TYPE_TREE;
	}

	inline bool is_datanode() {
		return bpaddr_slotid(addr) == NODE_TYPE_DATA;
	}

	inline bool is_metanode() {
		return bpaddr_slotid(addr) == NODE_TYPE_META;
	}

	inline bool is_invalid() {
		return op == NODE_OP_DELETE;
	}

	int get_type() { return bpaddr_slotid(addr); }

	virtual int size() {
		return buffer_size;
	}

	virtual void dump() = 0;
};

class bptree_meta;
class bptree_node;
class KvsSlottedPage;

class bptree_pool {
public:
	std::unordered_map<bp_addr_t, kv_indexnode*> pool;

	KADI *adi;
	int ksid_skp;
	uint32_t prefix;
	bptree_param *param;
	bptree_meta *meta;

	uint64_t   next_pgid;	// NODE ID 0 is dedicated for meta

public:
	bptree_pool(KADI *adi_, int ksid_skp_, uint32_t prefix_, bptree_param *param_);



	~bptree_pool() {
		for (auto &p : pool) {
			delete p.second;
		}
		pool.clear();
	}

	bptree_meta *get_meta() { return meta; }
public:
	//// =============================================
	//// Node Getters and Setters
	//// =============================================

	void remove_treenode(bptree_node *node);
	bptree_node *fetch_tree_node(const bp_addr_t &addr);
	bptree_node *create_tree_node(bool leaf);
	KvsSlottedPage *fetch_data_node(const bp_addr_t &addr, bool create);
	KvsSlottedPage *create_data_node();
	bool fetch_key(const bp_addr_t &addr, char **key, int &length);
	void flush(const bp_addr_t &newrootaddr);
    void remove_all();

private:

	//// =============================================
	//// POOL METADATA FETCH
	//// =============================================

	bptree_meta *_fetch_meta();
	kv_indexnode *_fetch_node(const bp_addr_t &addr);



private:

	//// =============================================
	//// KVSSD I/O
	//// =============================================

	struct __attribute__((__packed__)) kvs_page_key
	{
	   uint32_t	     prefix;
	   uint64_t      pageid;
	};

	struct flush_ctx {
		std::atomic<uint32_t> completed {0};
		std::atomic<uint32_t> errors {0};

	};

	static void kv_indexnode_flush_cb(kv_io_context &op, void* private_data) {
		struct flush_ctx* ctx = (struct flush_ctx*)private_data;
		if (ctx) {
			if (op.retcode != 0) {
				ctx->errors.fetch_add(1, std::memory_order_relaxed);
			}
			ctx->completed.fetch_add(1, std::memory_order_relaxed);
		}
	}

	int read_page(const bp_addr_t &addr, void *buffer, uint32_t buffersize) {
	    FTRACE
        //TR << "read_page: addr " << desc(addr)  << ", buffer " << buffer << ", buffersize = " << buffersize  ;
		kv_value page;
		page.length = buffersize;
		page.offset = 0;
		page.value  = buffer;

		int ret = adi->kv_retrieve_sync(ksid_skp, &page, [&] (struct nvme_passthru_kv_cmd& cmd){
		    cmd.key_length = fill_cmdkey_for_index_nodes(cmd.key, addr);

			//TR << "read_page: key = " << print_kvssd_key((char*)cmd.key, cmd.key_length) << ", " << desc(addr) ;
		});

		if (ret == 0) {
			return page.length;
		}
		return 0;
	}

	inline int fill_cmdkey_for_index_nodes(void *key, const bp_addr_t &addr) {
        kvs_page_key* k = (kvs_page_key*)key;
        k->prefix = bpaddr_slotid(addr);
        k->pageid = bpaddr_pageid(addr);
        return sizeof(kvs_page_key);
	}

	bool _flush_dirtylist() {
		int ret = 0;
		kv_value value;
		uint32_t qdepth  = 0, num_completed = 0;
		struct flush_ctx flushctx;
		uint32_t num_ios = pool.size();
		value.offset = 0;

		auto it = pool.begin();
		while (num_ios > num_completed) {

            while (it != pool.end() && qdepth < 16) {
                const auto &p = it->second;

                switch (p->op) {
                    case NODE_OP_NOP:
                        ret = 1;
                        break;
                    case NODE_OP_WRITE:
                        value.length = p->size();
                        value.value  = p->buffer;

                        ret = adi->kv_store_aio(ksid_skp, &value, {kv_indexnode_flush_cb, &flushctx},
                                                [&] (struct nvme_passthru_kv_cmd& cmd){
                                                    cmd.key_length = fill_cmdkey_for_index_nodes(cmd.key, p->addr);
                                                    //TR << "store node:  key = " << print_kvssd_key((char*)cmd.key, cmd.key_length) << ", " << desc (p->addr) ;
                                                });

                        break;
                    case NODE_OP_DELETE:
                        ret = adi->kv_delete_aio(ksid_skp, {kv_indexnode_flush_cb, &flushctx},
                                                 [&] (struct nvme_passthru_kv_cmd& cmd){
                                                     cmd.key_length = fill_cmdkey_for_index_nodes(cmd.key, p->addr);
                                                 });
                        break;
                    default:
                        //TR << "ERR: wrong op code " << p->op ;
                        return false;

                };


                if (ret != 0) { // I/O not issued
                    num_ios--;
                    if (p->op != NODE_OP_NOP) {
                        //TR << "ERR: I/O failed: op = "  << p->op ;
                        return false;
                    }
                }
                else {
                    qdepth++;
                }

                it++;
            }

			if (qdepth > 0) {
				const int done = flushctx.completed.exchange(0, std::memory_order_relaxed);
				qdepth        -= done;
				num_completed += done;
			}
		}
		return true;
	}


private:



};



#endif /* SRC_KADI_KADI_NODEPOOL_H_ */
