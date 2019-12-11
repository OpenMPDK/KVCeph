/*
 * kadi_nodepool.cc
 *
 *  Created on: Nov 9, 2019
 *      Author: root
 */
#include <iostream>
#include "kadi_nodepool.h"
#include "kadi_bptree.h"

bptree_pool::bptree_pool(KADI *adi_, int ksid_skp_, uint32_t prefix_, bptree_param *param_):
adi(adi_), ksid_skp(ksid_skp_), prefix(prefix_), param(param_)
{
    FTRACE
	meta = _fetch_meta();
	TR << "meta = " << meta << TREND;
	//next_pgid = meta->get_last_pgid();
}


void bptree_pool::remove_treenode(bptree_node *node) {
	node->set_invalid();
}
bptree_node *bptree_pool::fetch_tree_node(const bp_addr_t &addr) {
	return (bptree_node*)_fetch_node(addr);
}

bptree_node *bptree_pool::create_tree_node(bool leaf) {
	bp_addr_t addr = create_treenode_addr(next_pgid++);

	bptree_node *n = new bptree_node(addr,
			(char*)malloc(param->treenode_block_size),
			param->treenode_block_size, true, leaf,
			param->max_order, param->max_entries);

	pool[addr] = n;
	return n;
}

bool bptree_pool::fetch_key(const bp_addr_t &addr, char **key, int &length) {
	KvsSlottedPage* dn = (KvsSlottedPage*)_fetch_node(create_datanode_addr(bpaddr_pageid(addr)));
	if (dn) {
		return dn->lookup(addr, key, length);
	}
	return false;
}

KvsSlottedPage *bptree_pool::fetch_data_node(const bp_addr_t &addr, bool create = false) {

	return (KvsSlottedPage*)_fetch_node(addr);
}

KvsSlottedPage *bptree_pool::create_data_node() {
	bp_addr_t addr = create_datanode_addr(next_pgid++);
	KvsSlottedPage *n = new KvsSlottedPage(addr, (char*)malloc(param->datanode_block_size), param->datanode_block_size, true);
	pool[addr] = n;
	return n;
}

void bptree_pool::flush(const bp_addr_t &newrootaddr) {
    if (newrootaddr == invalid_key_addr) return;

	meta->set_next_pgid(next_pgid);
	meta->set_root_addr(newrootaddr);
    TRITER << "FLUSH meta root->addr = " << desc(meta->get_root_addr()) << " next pgid  = " << next_pgid << TREND;

	meta->set_dirty();


    TRITER << "Nodepool Flush - set meta - rootaddr = " << newrootaddr << ", get_root_addr = " << meta->get_root_addr() << ", meta addr = " << meta->addr<< TREND;
	_flush_dirtylist();
}

bptree_meta *bptree_pool::_fetch_meta() {
    FTRACE
    TR << ">> 1. fetching metadata: prefix = " << prefix << TREND;

	bp_addr_t addr = create_metanode_addr(prefix);
    bptree_meta *n = 0;
    TR << ">> 2. addr = " << addr << TREND;

	auto it = pool.find(addr);
	if (it != pool.end()) {
		return (bptree_meta*)it->second;
	}

    n = new bptree_meta(addr);

	int nread = read_page(addr, n->get_raw_buffer(), bptree_meta::META_SIZE);
	if (nread == bptree_meta::META_SIZE) {
		// load meta from data
		TRITER << "metadata fetched " << desc(addr) << TREND;
	} else {
		// new meta
        TRITER << "create new metadata " << desc(addr) << TREND;
        n->init(prefix);
	}

	pool[addr] = n;

	return n;
}

kv_indexnode *bptree_pool::_fetch_node(const bp_addr_t &addr) {
    FTRACE
	// search the cache
	if (addr == invalid_key_addr) return 0;

	TRITER << "fetch node " << TREND;
	auto it = pool.find(addr);
	if (it != pool.end()) {
		if (it->second->is_invalid()) return 0; // deleted
		return it->second;
	}

	// load from the disk
	if (bpaddr_pageid(addr) < meta->get_last_pgid() && !meta->isnew) {

        TR << "malloc large block " << param->datanode_block_size << TREND;
		void *data = malloc(param->datanode_block_size);
		TR << "done" << TREND;
		int nread = read_page(addr, data, param->datanode_block_size);
		if (nread > 0) {
			kv_indexnode *n;
			const int off = bpaddr_slotid(addr);
			if (off == NODE_TYPE_TREE) {
			    TR << "fetch bptree node " << TREND;
				n = new bptree_node(addr, (char*)data, param->treenode_block_size, false, true, param->max_order, param->max_entries);
                if (((bptree_node*)n)->header()->self == 0) {
                    TR << "self addr == 0 " << TREND;
                }
                if (((bptree_node*)n)->header()->next == 0) {
                    TR << "next addr == 0 " << TREND;
                }

            }
			else if (off == NODE_TYPE_DATA){
                TR << "fetch data node " << TREND;
                n = new KvsSlottedPage(addr, (char *) data, param->datanode_block_size, false);
            } else {
			    free(data);
			    return 0;
			}


			pool[addr] = n;
			return n;
		}
		free(data);
	}

	return 0;
}
