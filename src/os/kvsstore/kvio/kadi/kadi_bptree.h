/*
 * kadi_bptree.h
 *
 *  Created on: Oct 31, 2019
 *      Author: yangwook.k
 */

#ifndef SRC_KADI_KADI_BPTREE_H_
#define SRC_KADI_KADI_BPTREE_H_

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>
#include <atomic>
#include <map>
#include <memory>
#include <cstring>

#include "kadi_nodepool.h"
#include "kadi_slottedpage.h"
#include "../../kvsstore_debug.h"
enum {
        BPLUS_TREE_LEAF,
        BPLUS_TREE_NON_LEAF = 1,
};

enum {
        LEFT_SIBLING,
        RIGHT_SIBLING = 1,
};

static inline void set_dirty(kv_indexnode *sub_node) {
		if (sub_node)
        	sub_node->set_dirty();
}


class bptree_meta: public kv_indexnode {
public:
    virtual void dump() {
        //TR << "bpmeta" ;
    }
	const static int META_SIZE = 16;
    bool isnew;

    bptree_meta(const bp_addr_t &addr): kv_indexnode(addr), isnew(false) {
        buffer = (char*)calloc(1, META_SIZE);
        buffer_size = META_SIZE;
    }

    char *get_raw_buffer() { return buffer; }


    void init(int treeid)
	{
        set_root_addr( create_metanode_addr(treeid) );
        set_next_pgid(2);
        isnew = true;
        //TR << "  new meta: root addr = " << desc(get_root_addr())  ;
        //TR << "  new meta: last pgid = " << get_last_pgid() ;
	}

	inline uint64_t get_last_pgid() {
		uint64_t* values = (uint64_t*)buffer;
		return values[0];
	}

	inline void set_next_pgid(uint64_t pgid) {
		uint64_t* values = (uint64_t*)buffer;
		values[0] = pgid;
        //TR << "updated buffer = " << print_kvssd_key(buffer, META_SIZE) << ", 0th " << values[0] << ", 1th" << values[1] << ", next pageid =" << pgid ;
	}

	inline bp_addr_t get_root_addr() {
		bp_addr_t* values = (bp_addr_t*)buffer;
		return values[1];
	}

	inline void set_root_addr(bp_addr_t addr) {
		bp_addr_t* values = (bp_addr_t*)buffer;
		values[1] = addr;
        //TR << "updated buffer = " << print_kvssd_key(buffer, META_SIZE) << ", 0th " << values[0] << ", 1th" << values[1] << ", root addr =" << addr << ", get root addr = " << get_root_addr() ;

	}
};

class bptree;

class bptree_iterator {
protected:
	bptree *tree;
	int keyindex;
	bp_addr_t nodeaddr;
public:
	virtual bool get_key(char **key, int &length) { return false; }
	virtual void move_next(const long int movement) {};
	virtual void begin() { std::cout << "begin" << std::endl; };
	virtual void end() {};
	virtual void lower_bound(const char *key, int length) {};
	virtual void upper_bound(const char *key, int length) {};

public:
	using iterator_category = std::random_access_iterator_tag;
	using value_type = bp_addr_t;
	using difference_type = ptrdiff_t;
	using pointer = bp_addr_t*;
	using reference = bp_addr_t&;

	bptree_iterator(bptree *tree_, int keyindex_, bp_addr_t nodeaddr_) :
		tree(tree_), keyindex(keyindex_), nodeaddr(nodeaddr_) {
	}

	bptree_iterator(const bptree_iterator &rawIterator) = default;
	virtual ~bptree_iterator() {}

	bptree_iterator& operator=(const bptree_iterator &rawIterator) = default;

	operator bool() const {
		return (tree != 0);
	}

	bool operator==(const bptree_iterator &rawIterator) const {
		return (tree == rawIterator.tree && nodeaddr == rawIterator.nodeaddr && keyindex == rawIterator.keyindex);
	}

	bool operator!=(const bptree_iterator &rawIterator) const {
		return !(tree == rawIterator.tree && nodeaddr == rawIterator.nodeaddr && keyindex == rawIterator.keyindex);
	}

	inline bool is_end() {
		//std::cout << "is_end()? " << (tree == 0 || keyindex == -1) << ", tree = " << tree << ", index = " << keyindex << std::endl;
		return (tree == 0 || keyindex == -1);
	}

	bptree_iterator& operator+=(const ptrdiff_t &movement) {
		move_next(movement);
		return (*this);
	}
	bptree_iterator& operator-=(const ptrdiff_t &movement) {
		move_next(-movement);
		return (*this);
	}

	void move_next() {
		move_next(1);
	}

	bptree_iterator& operator++() {
		move_next(1);
		return (*this);
	}

	bptree_iterator operator++(int)
	{
		auto temp(*this);
		move_next(1);
		return temp;
	}

	bptree_iterator operator--(int)
	{
		auto temp(*this);
		move_next(-1);
		return temp;
	}

	bptree_iterator& operator--() {
		move_next(-1);
		return (*this);
	}

	bptree_iterator operator+(const ptrdiff_t &movement) {
		auto old_addr  = nodeaddr;
		auto old_index = keyindex;
		move_next(movement);
		auto temp(*this);
		nodeaddr = old_addr;
		keyindex = old_index;
		return temp;
	}
	bptree_iterator operator-(const ptrdiff_t &movement) {
		auto old_addr  = nodeaddr;
		auto old_index = keyindex;
		move_next(-movement);
		auto temp(*this);
		nodeaddr = old_addr;
		keyindex = old_index;
		return temp;
	}

};

class bptree_node: public kv_indexnode {
	int max_order;
	int max_entries;
public:
	bptree_node(const bp_addr_t &addr, char *buffer, uint16_t buffer_size, bool isnew, bool leaf, int max_order_, int max_entries_):
		kv_indexnode(addr, buffer, buffer_size), max_order(max_order_), max_entries(max_entries_) {

		if (isnew) {
			header()->type = (leaf)? BPLUS_TREE_LEAF:BPLUS_TREE_NON_LEAF;
			header()->children = 0;
			header()->self = addr;
			header()->parent = invalid_key_addr;
			header()->prev= invalid_key_addr;
			header()->next= invalid_key_addr;
		}
	}

	inline bptree_node_header* header() { return (bptree_node_header*) buffer; }
	inline bool is_leaf() { return header()->type == BPLUS_TREE_LEAF; }

	void set_children(int num) { header()->children = num; }
	void inc_children(int num) { header()->children += num; }
	int  get_children() { return header()->children; }



	inline char* offset_ptr() { return buffer + sizeof(bptree_node_header); }
	inline bp_addr_t* key()  { return (bp_addr_t*)offset_ptr(); }
	//inline char* data() { return node_buffer + (max_entries * sizeof(bp_addr_t)); }
	inline bp_addr_t* sub()  { return (bp_addr_t*)(offset_ptr() + ((max_order -1) * sizeof(bp_addr_t))); }

	void dump() {
        /*TR << "NODE ADDR = " << addr ;
        auto keys = key();
        for (int i = 0; i < get_children(); i++) {
            TR << i << "th key addr = " << keys[i] ;
        }*/
    }
};


class bptree {
	std::vector<KvsSlottedPage*> datanode_cache;
public:
	bptree_param param;
	bptree_pool  pool;
	bptree_node *root;
	bptree_meta *meta;
	int level;
    bool dirty;

	bptree(KADI *adi, int ksid_skp, uint32_t prefix, int block_size = 28*1024):
		param(block_size), pool(adi, ksid_skp, prefix, &param), level(0), dirty(false)
	{
	    FTRACE

	    TR << "fetch meta";
		// create or fetch root node
		meta = pool.get_meta();
		if (meta->isnew) {
			root = 0;
		}
		else {
            TR << "meta node is fetched";
            root = 0;
            /*
			root = pool.fetch_tree_node(pool.get_meta()->get_root_addr());
			if (root == 0) {
			    std::cerr << "cannot find the root node" << std::endl;
			}*/
		}

        TR << "fetch meta done";

	}

	bp_addr_t insert_key_to_datanode(char *key, int length) {
		for (KvsSlottedPage* datanode : datanode_cache) {
			bp_addr_t keyaddr = datanode->insert(key, length);
			if (keyaddr != invalid_key_addr) {
                datanode->set_dirty();
                //TR << "DN insert: DN ADDR " << desc(datanode->addr) << ", KEY ADDR " << desc(keyaddr) ;
				return keyaddr;
			}
		}

		KvsSlottedPage* dn = pool.create_data_node();
		datanode_cache.push_back(dn);
        bp_addr_t keyaddr = dn->insert(key, length);
        dn->set_dirty();
        //TR << "DN insert: DN ADDR " << desc(dn->addr) << ", KEY ADDR " << desc(keyaddr) ;
		return keyaddr;
	}

	bool get_key_from_datanode(const bp_addr_t& key, char **keyfound, int &keylength) {
		bp_addr_t dnaddr = create_datanode_addr(bpaddr_pageid(key));
		KvsSlottedPage * dn = pool.fetch_data_node(dnaddr, false);
		if (dn == 0) {
			return false;
		}

		return dn->lookup(key, keyfound, keylength);
	}

	void remove_key_from_datanode(const bp_addr_t& key) {
		bp_addr_t dnaddr = create_datanode_addr(bpaddr_pageid(key));
		KvsSlottedPage * dn = pool.fetch_data_node(dnaddr, false);
		if (dn == 0) {
			std::cerr << "cannot find dn for the given key" << std::endl;
			exit(1);
		}
		dn->remove(key);
		dn->set_dirty();
	}

	int insert(char *userkey, int keylength)
	{
        bptree_node *node = root;

		while (node != NULL) {
			if (node->is_leaf()) {
			    dirty = true;
				return leaf_insert(node, userkey, keylength);
			} else {
				int i = key_binary_search(node, userkey, keylength);
				if (i == INT32_MAX) return -1;
				if (i >= 0) {
						node = pool.fetch_tree_node(node->sub()[i + 1]);
				} else {
						i = -i - 1;
						node = pool.fetch_tree_node(node->sub()[i]);
				}
			}
		}

		root = pool.create_tree_node(true);
		root->key()[0] = insert_key_to_datanode(userkey, keylength);
		root->set_children(1);
		set_dirty(root);
		this->level = 1;
        this->dirty = true;
        //TR << "root node created: addr = " << desc(root->addr) ;
		return 0;
	}

	int remove(char *userkey, int keylength)
	{
		if (root == 0) return -1;

		struct bptree_node *node = root;

		while (node != NULL) {
				if (node->is_leaf()) {
                        this->dirty = true;
						return leaf_remove(node, userkey, keylength);
				} else {
						int i = key_binary_search(node, userkey, keylength);
						if (i == INT32_MAX) return -1;
						if (i >= 0) {
								node = pool.fetch_tree_node(node->sub()[i + 1]);
						} else {
								i = -i - 1;
								node = pool.fetch_tree_node(node->sub()[i]);
						}
				}
		}
		return -1;
	}

	void flush() {
		if (root && dirty) {
			pool.flush(root->addr);
		}
	}
private:
	///
	/// B+ TREE ALGORITHM - INSERT
	///
    inline bp_addr_t replicate_key(const bp_addr_t &key){
        char *k = 0;
        int l = 0;
        get_key_from_datanode(key, &k, l);
        return insert_key_to_datanode(k, l);
        //std::cerr << "split key = " << std::string(k + 4, 12) << std::endl;
    }
	int leaf_insert(bptree_node *leaf, char *userkey, int keylength)
	{
		/* Search key location */
		int insert = key_binary_search(leaf, userkey, keylength);
		if (insert == INT32_MAX) return -1;
		//std::cout << "binary search result = " << insert << std::endl;
		if (insert >= 0) {
			/* Already exists */
			return 0;
		}

		bp_addr_t key = insert_key_to_datanode(userkey, keylength);
		insert = -insert - 1;

        //TR << "tree insert key = " << key ;
		/* leaf is full */
	  	if (leaf->get_children() == param.max_entries) {
				bp_addr_t split_key;
				/* split = [m/2] */
				int split = (param.max_entries + 1) / 2;
				bptree_node *sibling = pool.create_tree_node(true);

				/* sibling leaf replication due to location of insertion */
				if (insert < split) {
						split_key = leaf_split_left(leaf, sibling, key, insert);
				} else {
						split_key = leaf_split_right(leaf, sibling, key, insert);
				}

                split_key = replicate_key(split_key);

				/* build new parent */
				if (insert < split) {
						return parent_node_build(sibling, leaf, split_key);
				} else {
						return parent_node_build(leaf, sibling, split_key);
				}
		} else {
				leaf_simple_insert(leaf, key, insert);
				//leaf->set_dirty();
				set_dirty(leaf);
		}

		return 0;
	}

	void left_node_add(bptree_node *node, bptree_node *left)
	{
		bptree_node *prev = pool.fetch_tree_node(node->header()->prev);
		if (prev != NULL) {
				prev->header()->next = left->header()->self;
				left->header()->prev = prev->header()->self;
				set_dirty(prev);//prev->set_dirty();
		} else {
				left->header()->prev = invalid_key_addr;
		}
		left->header()->next = node->header()->self;
		node->header()->prev = left->header()->self;
	}

	void right_node_add(bptree_node *node, bptree_node *right)
	{
		std::cout << ">> add right node" << std::endl;
		std::cout << "current node: curr = " << node->header()->self
				  << ", next = " << node->header()->next << ", invalid key = " << invalid_key_addr << std::endl;

		bptree_node *next = pool.fetch_tree_node(node->header()->next);
		if (next != NULL) {
				std::cout << "current right is not null" << std::endl;
				next->header()->prev = right->header()->self;
				right->header()->next = next->header()->self;
				set_dirty(next); //next->set_dirty();
		} else {
				std::cout << "set right's next to null" << std::endl;
				right->header()->next = invalid_key_addr;
		}
		right->header()->prev = node->header()->self;
		node->header()->next = right->header()->self;
		std::cout << "right's addr = " << right->header()->self << std::endl;
	}

	int parent_node_build(bptree_node *l_ch,
	                             bptree_node *r_ch, bp_addr_t key)
	{
		if (l_ch->header()->parent == invalid_key_addr && r_ch->header()->parent == invalid_key_addr) {
			/* new parent */

			bptree_node *parent = pool.create_tree_node(false);
			parent->key()[0] = key;
			parent->sub()[0] = l_ch->header()->self;
			parent->sub()[1] = r_ch->header()->self;
			parent->set_children (2);
			/* write new parent and update root */
			this->root = parent;
			l_ch->header()->parent = parent->header()->self;
			r_ch->header()->parent = parent->header()->self;
			this->level++;
			/* flush parent, left and right child */
			set_dirty(l_ch);//l_ch->set_dirty();
			set_dirty(r_ch);//r_ch->set_dirty();
			set_dirty(parent);//parent->set_dirty();
			return 0;
		} else if (r_ch->header()->parent == invalid_key_addr) {
				return non_leaf_insert(pool.fetch_tree_node(l_ch->header()->parent), l_ch, r_ch, key);
		} else {
				return non_leaf_insert(pool.fetch_tree_node(r_ch->header()->parent), l_ch, r_ch, key);
		}
	}

	bp_addr_t non_leaf_split_left(bptree_node *node,
	                	         bptree_node *left, bptree_node *l_ch,
	                	         bptree_node *r_ch, bp_addr_t key, int insert)
	{
	        int i;
	        bp_addr_t split_key;

	        /* split = [m/2] */
	        int split = (param.max_order + 1) / 2;

	        /* split as left sibling */
	        left_node_add(node, left);

	        /* calculate split nodes' children (sum as (order + 1))*/
	        int pivot = insert;
	        left->set_children (split);
	        node->set_children (param.max_order - split + 1);

	        /* sum = left->children = pivot + (split - pivot - 1) + 1 */
	        /* replicate from key[0] to key[insert] in original node */
	        memmove(&left->key()[0], &node->key()[0], pivot * sizeof(bp_addr_t));
	        memmove(&left->sub()[0], &node->sub()[0], pivot * sizeof(bp_addr_t));

	        /* replicate from key[insert] to key[split - 1] in original node */
	        memmove(&left->key()[pivot + 1], &node->key()[pivot], (split - pivot - 1) * sizeof(bp_addr_t));
	        memmove(&left->sub()[pivot + 1], &node->sub()[pivot], (split - pivot - 1) * sizeof(bp_addr_t));

	        /* flush sub-nodes of the new splitted left node */
	        for (i = 0; i < left->get_children(); i++) {
				if (i != pivot && i != pivot + 1) {
						sub_node_flush(left, left->sub()[i]);
				}
	        }

	        /* insert new key and sub-nodes and locate the split key */
	        left->key()[pivot] = key;
	        if (pivot == split - 1) {
	                /* left child in split left node and right child in original right one */
	                sub_node_update(left, pivot, l_ch);
	                sub_node_update(node, 0, r_ch);
	                split_key = key;
	        } else {
	                /* both new children in split left node */
	                sub_node_update(left, pivot, l_ch);
	                sub_node_update(left, pivot + 1, r_ch);
	                node->sub()[0] = node->sub()[split - 1];
	                split_key = node->key()[split - 2];
	        }

	        /* sum = node->children = 1 + (node->get_children() - 1) */
	        /* right node left shift from key[split - 1] to key[children - 2] */
	        memmove(&node->key()[0], &node->key()[split - 1], (node->get_children() - 1) * sizeof(bp_addr_t));
	        memmove(&node->sub()[1], &node->sub()[split], (node->get_children()- 1) * sizeof(bp_addr_t));

	        return split_key;
	}

	bp_addr_t non_leaf_split_right1(bptree_node *node,
	                        	   bptree_node *right, bptree_node *l_ch,
	                        	   bptree_node *r_ch, bp_addr_t key, int insert)
	{
	        int i;

	        /* split = [m/2] */
	        int split = (param.max_order + 1) / 2;

	        /* split as right sibling */
	        right_node_add(node, right);

	        /* split key is key[split - 1] */
	        bp_addr_t split_key = node->key()[split - 1];

	        /* calculate split nodes' children (sum as (order + 1))*/
	        int pivot = 0;
	        node->set_children(split);
	        right->set_children(param.max_order - split + 1);

	        /* insert new key and sub-nodes */
	        right->key()[0] = key;
	        sub_node_update(right, pivot, l_ch);
	        sub_node_update(right, pivot + 1, r_ch);

	        /* sum = right->children = 2 + (right->get_children() - 2) */
	        /* replicate from key[split] to key[param.max_order - 2] */
	        memmove(&right->key()[pivot + 1], &node->key()[split], (right->get_children() - 2) * sizeof(bp_addr_t));
	        memmove(&right->sub()[pivot + 2], &node->sub()[split + 1], (right->get_children() - 2) * sizeof(bp_addr_t));

	        /* flush sub-nodes of the new splitted right node */
	        for (i = pivot + 2; i < right->get_children(); i++) {
	                sub_node_flush(right, right->sub()[i]);
	        }

	        return split_key;
	}

	bp_addr_t non_leaf_split_right2(bptree_node *node,
	                        	   bptree_node *right, bptree_node *l_ch,
	                        	   bptree_node *r_ch, bp_addr_t key, int insert)
	{
	        int i;

	        /* split = [m/2] */
	        int split = (param.max_order + 1) / 2;

	        /* split as right sibling */
	        right_node_add(node, right);

	        /* split key is key[split] */
	        bp_addr_t split_key = node->key()[split];

	        /* calculate split nodes' children (sum as (order + 1))*/
	        int pivot = insert - split - 1;
	        node->set_children (split + 1);
	        right->set_children (param.max_order - split);

	        /* sum = right->children = pivot + 2 + (param.max_order - insert - 1) */
	        /* replicate from key[split + 1] to key[insert] */
	        memmove(&right->key()[0], &node->key()[split + 1], pivot * sizeof(bp_addr_t));
	        memmove(&right->sub()[0], &node->sub()[split + 1], pivot * sizeof(bp_addr_t));

	        /* insert new key and sub-node */
	        right->key()[pivot] = key;
	        sub_node_update(right, pivot, l_ch);
	        sub_node_update(right, pivot + 1, r_ch);

	        /* replicate from key[insert] to key[order - 1] */
	        memmove(&right->key()[pivot + 1], &node->key()[insert], (param.max_order - insert - 1) * sizeof(bp_addr_t));
	        memmove(&right->sub()[pivot + 2], &node->sub()[insert + 1], (param.max_order - insert - 1) * sizeof(bp_addr_t));

	        /* flush sub-nodes of the new splitted right node */
	        for (i = 0; i < right->get_children(); i++) {
	                if (i != pivot && i != pivot + 1) {
	                        sub_node_flush(right, right->sub()[i]);
	                }
	        }

	        return split_key;
	}

	void non_leaf_simple_insert(bptree_node *node,
	                        	   bptree_node *l_ch, bptree_node *r_ch,
	                        	   bp_addr_t key, int insert)
	{
	        memmove(&node->key()[insert + 1], &node->key()[insert], (node->get_children() - 1 - insert) * sizeof(bp_addr_t));
	        memmove(&node->sub()[insert + 2], &node->sub()[insert + 1], (node->get_children() - 1 - insert) * sizeof(bp_addr_t));
	        /* insert new key and sub-nodes */
	        node->key()[insert] = key;
	        sub_node_update(node, insert, l_ch);
	        sub_node_update(node, insert + 1, r_ch);
	        node->inc_children(1);
	}

	int non_leaf_insert(bptree_node *node,
	                	   bptree_node *l_ch, bptree_node *r_ch, bp_addr_t key)
	{
	        /* Search key location */
	        int insert = key_binary_search(node, key);
            if (insert == INT32_MAX) return -1;
	        assert(insert < 0);
	        insert = -insert - 1;

	        /* node is full */
	        if (node->get_children() == param.max_order) {
	                bp_addr_t split_key;
	                /* split = [m/2] */
	                int split = (node->get_children() + 1) / 2;
	                bptree_node *sibling = pool.create_tree_node(false);
	                if (insert < split) {
	                        split_key = non_leaf_split_left(node, sibling, l_ch, r_ch, key, insert);
	                } else if (insert == split) {
	                        split_key = non_leaf_split_right1(node, sibling, l_ch, r_ch, key, insert);
	                } else {
	                        split_key = non_leaf_split_right2(node, sibling, l_ch, r_ch, key, insert);
	                }

                    split_key = replicate_key(split_key);

	                /* build new parent */
	                if (insert < split) {
	                        return parent_node_build(sibling, node, split_key);
	                } else {
	                        return parent_node_build(node, sibling, split_key);
	                }
	        } else {
	                non_leaf_simple_insert(node, l_ch, r_ch, key, insert);
	                node->set_dirty();
	        }
	        return 0;
	}

	bp_addr_t leaf_split_left(bptree_node *leaf,
	                	     bptree_node *left, bp_addr_t key,  int insert)
	{
	        /* split = [m/2] */
	        int split = (leaf->get_children() + 1) / 2;

	        /* split as left sibling */
	        left_node_add(leaf, left);

	        /* calculate split leaves' children (sum as (entries + 1)) */
	        int pivot = insert;
	        left->set_children(split);
	        leaf->set_children(param.max_entries - split + 1);

	        /* sum = left->children = pivot + 1 + (split - pivot - 1) */
	        /* replicate from key[0] to key[insert] */
	        memmove(&left->key()[0], &leaf->key()[0], pivot * sizeof(bp_addr_t));
	        //memmove(&data(left)[0], &data(leaf)[0], pivot * sizeof(long));

	        /* insert new key and data */
	        left->key()[pivot] = key;
	        //data(left)[pivot] = data;

	        /* replicate from key[insert] to key[split - 1] */
	        memmove(&left->key()[pivot + 1], &leaf->key()[pivot], (split - pivot - 1) * sizeof(bp_addr_t));
	        //memmove(&data(left)[pivot + 1], &data(leaf)[pivot], (split - pivot - 1) * sizeof(long));

	        /* original leaf left shift */
	        memmove(&leaf->key()[0], &leaf->key()[split - 1], leaf->get_children() * sizeof(bp_addr_t));
	        //memmove(&data(leaf)[0], &data(leaf)[split - 1], leaf->get_children() * sizeof(long));

	        return leaf->key()[0];
	}

	bp_addr_t leaf_split_right(bptree_node *leaf,
	                	      bptree_node *right, bp_addr_t key, int insert)
	{
	        /* split = [m/2] */
	        int split = (leaf->get_children() + 1) / 2;

	        /* split as right sibling */
	        right_node_add(leaf, right);

	        /* calculate split leaves' children (sum as (entries + 1)) */
	        int pivot = insert - split;
	        leaf->set_children(split);
	        right->set_children(param.max_entries - split + 1);


	        std::cout << "curr's first key = " << leaf->key()[0] << ", right addr = " << right->addr <<std::endl;
	        std::cout << "curr's last key = " << leaf->key()[split-1] << ", right addr = " << right->addr <<std::endl;
	        std::cout << "right's first key = " << leaf->key()[split] << ", right addr = " << right->addr <<std::endl;

	        /* sum = right->children = pivot + 1 + (param.max_entries - pivot - split) */
	        /* replicate from key[split] to key[children - 1] in original leaf */
	        memmove(&right->key()[0], &leaf->key()[split], pivot * sizeof(bp_addr_t));
	        //memmove(&data(right)[0], &data(leaf)[split], pivot * sizeof(long));

	        /* insert new key and data */
	        right->key()[pivot] = key;
	        //data(right)[pivot] = data;

	        /* replicate from key[insert] to key[children - 1] in original leaf */
	        memmove(&right->key()[pivot + 1], &leaf->key()[insert], (param.max_entries - insert) * sizeof(bp_addr_t));
	        //memmove(&data(right)[pivot + 1], &data(leaf)[insert], (param.max_entries - insert) * sizeof(long));

	        std::cout << "right's first key = " << right->key()[0] << ", right addr = " << right->addr <<std::endl;

	        return right->key()[0];
	}

	void leaf_simple_insert(bptree_node *leaf,
	                	       bp_addr_t key, int insert)
	{
	        memmove(&leaf->key()[insert + 1], &leaf->key()[insert], (leaf->get_children() - insert) * sizeof(bp_addr_t));
	        //memmove(&data(leaf)[insert + 1], &data(leaf)[insert], (leaf->get_children() - insert) * sizeof(long));
	        leaf->key()[insert] = key;
	        //data(leaf)[insert] = data;
	        leaf->inc_children(1);
	}

	inline void sub_node_update(bptree_node *parent, int index, bptree_node *sub_node)
	{
	        assert(sub_node->header()->self != invalid_key_addr);
	        parent->sub()[index] = sub_node->header()->self;
	        sub_node->header()->parent = parent->header()->self;
	        set_dirty(sub_node);//sub_node->set_dirty();
	}

	inline void sub_node_flush(bptree_node *parent, bp_addr_t sub_offset)
	{
	        bptree_node *sub_node = pool.fetch_tree_node(sub_offset);
	        assert(sub_node != NULL);
	        sub_node->header()->parent = parent->header()->self;
	        set_dirty(sub_node);//sub_node->set_dirty();
	}

private:
	///
	/// B+ TREE ALGORITHM - REMOVE
	///

	static inline int sibling_select(bptree_node *l_sib, bptree_node *r_sib,
	                                 bptree_node *parent, int i)
	{
	        if (i == -1) {
	                /* the frist sub-node, no left sibling, choose the right one */
	                return RIGHT_SIBLING;
	        } else if (i == parent->get_children() - 2) {
	                /* the last sub-node, no right sibling, choose the left one */
	                return LEFT_SIBLING;
	        } else {
	                /* if both left and right sibling found, choose the one with more children */
	                return l_sib->get_children() >= r_sib->get_children() ? LEFT_SIBLING : RIGHT_SIBLING;
	        }
	}

	void non_leaf_shift_from_left(bptree_node *node,
	                        	     bptree_node *left, bptree_node *parent,
	                        	     int parent_key_index, int remove)
	{
	        /* node's elements right shift */
	        memmove(&node->key()[1], &node->key()[0], remove * sizeof(bp_addr_t));
	        memmove(&node->sub()[1], &node->sub()[0], (remove + 1) * sizeof(off_t));

	        /* parent key right rotation */
	        node->key()[0] = parent->key()[parent_key_index];
	        parent->key()[parent_key_index] = left->key()[left->get_children() - 2];

	        /* borrow the last sub-node from left sibling */
	        node->sub()[0] = left->sub()[left->get_children() - 1];
	        sub_node_flush(node, node->sub()[0]);

	        left->inc_children(-1);
	}

	void non_leaf_merge_into_left(bptree_node *node,
	                        	     bptree_node *left, bptree_node *parent,
	                        	     int parent_key_index, int remove)
	{
	        /* move parent key down */
	        left->key()[left->get_children() - 1] = parent->key()[parent_key_index];

	        /* merge into left sibling */
	        /* key sum = node->get_children() - 2 */
	        memmove(&left->key()[left->get_children()], &node->key()[0], remove * sizeof(bp_addr_t));
	        memmove(&left->sub()[left->get_children()], &node->sub()[0], (remove + 1) * sizeof(off_t));

	        /* sub-node sum = node->get_children() - 1 */
	        memmove(&left->key()[left->get_children() + remove], &node->key()[remove + 1], (node->get_children() - remove - 2) * sizeof(bp_addr_t));
	        memmove(&left->sub()[left->get_children() + remove + 1], &node->sub()[remove + 2], (node->get_children() - remove - 2) * sizeof(off_t));

	        /* flush sub-nodes of the new merged left node */
	        int i, j;
	        for (i = left->get_children(), j = 0; j < node->get_children() - 1; i++, j++) {
	                sub_node_flush(left, left->sub()[i]);
	        }

	        left->inc_children(node->get_children() - 1);
	}

	void non_leaf_shift_from_right(bptree_node *node,
	                        	      bptree_node *right, bptree_node *parent,
	                        	      int parent_key_index)
	{
	        /* parent key left rotation */
	        node->key()[node->get_children() - 1] = parent->key()[parent_key_index];
	        parent->key()[parent_key_index] = right->key()[0];

	        /* borrow the frist sub-node from right sibling */
	        node->sub()[node->get_children()] = right->sub()[0];
	        sub_node_flush(node, node->sub()[node->get_children()]);
	        node->inc_children(1);

	        /* right sibling left shift*/
	        memmove(&right->key()[0], &right->key()[1], (right->get_children() - 2) * sizeof(bp_addr_t));
	        memmove(&right->sub()[0], &right->sub()[1], (right->get_children() - 1) * sizeof(off_t));

	        right->inc_children(-1);
	}

	void non_leaf_merge_from_right(bptree_node *node,
	                        	      bptree_node *right, bptree_node *parent,
	                        	      int parent_key_index)
	{
	        /* move parent key down */
	        node->key()[node->get_children() - 1] = parent->key()[parent_key_index];
	        node->inc_children(1);

	        /* merge from right sibling */
	        memmove(&node->key()[node->get_children() - 1], &right->key()[0], (right->get_children() - 1) * sizeof(bp_addr_t));
	        memmove(&node->sub()[node->get_children() - 1], &right->sub()[0], right->get_children() * sizeof(off_t));

	        /* flush sub-nodes of the new merged node */
	        int i, j;
	        for (i = node->get_children() - 1, j = 0; j < right->get_children(); i++, j++) {
	                sub_node_flush(node, node->sub()[i]);
	        }

	        node->inc_children(right->get_children() - 1);
	}

	static inline void non_leaf_simple_remove(bptree_node *node, int remove)
	{
	        assert(node->get_children() >= 2);
	        memmove(&node->key()[remove], &node->key()[remove + 1], (node->get_children() - remove - 2) * sizeof(bp_addr_t));
	        memmove(&node->sub()[remove + 1], &node->sub()[remove + 2], (node->get_children() - remove - 2) * sizeof(off_t));
	        node->inc_children(-1);
	}

	void non_leaf_remove(bptree_node *node, int remove)
	{
	        if (node->header()->parent == invalid_key_addr) {
	                /* node is the root */
	                if (node->get_children() == 2) {
	                        /* replace old root with the first sub-node */
	                        bptree_node *root = pool.fetch_tree_node(node->sub()[0]);
	                        root->header()->parent = invalid_key_addr;
	                        this->root = root;
	                        this->level--;
	                        node_delete(node, NULL, NULL);
	                        set_dirty(root);//root->set_dirty();
	                } else {
	                        non_leaf_simple_remove(node, remove);
	                        set_dirty(node);//node->set_dirty();
	                }
	        } else if (node->get_children() <= (param.max_order + 1) / 2) {
	                bptree_node *l_sib = pool.fetch_tree_node(node->header()->prev);
	                bptree_node *r_sib = pool.fetch_tree_node(node->header()->next);
	                bptree_node *parent = pool.fetch_tree_node(node->header()->parent);

	                int i = parent_key_index(parent, node->key()[0]);

	                /* decide which sibling to be borrowed from */
	                if (sibling_select(l_sib, r_sib, parent, i)  == LEFT_SIBLING) {
	                        if (l_sib->get_children() > (param.max_order + 1) / 2) {
	                                non_leaf_shift_from_left(node, l_sib, parent, i, remove);
	                                /* flush nodes */
	                                set_dirty(node);//node->set_dirty();
	                                set_dirty(l_sib);//l_sib->set_dirty();
	                                set_dirty(r_sib);//r_sib->set_dirty();
	                                set_dirty(parent);//parent->set_dirty();
	                        } else {
	                                non_leaf_merge_into_left(node, l_sib, parent, i, remove);
	                                /* delete empty node and flush */
	                                node_delete(node, l_sib, r_sib);
	                                /* trace upwards */
	                                non_leaf_remove(parent, i);
	                        }
	                } else {
	                        /* remove at first in case of overflow during merging with sibling */
	                        non_leaf_simple_remove(node, remove);

	                        if (r_sib->get_children() > (param.max_order + 1) / 2) {
	                                non_leaf_shift_from_right(node, r_sib, parent, i + 1);
	                                /* flush nodes */
	                                set_dirty(node);//node->set_dirty();
	                                set_dirty(l_sib);//l_sib->set_dirty();
	                                set_dirty(r_sib);//r_sib->set_dirty();
	                                set_dirty(parent);//parent->set_dirty();
	                        } else {
	                                non_leaf_merge_from_right(node, r_sib, parent, i + 1);
	                                /* delete empty right sibling and flush */
	                                bptree_node *rr_sib = pool.fetch_tree_node(r_sib->header()->next);
	                                node_delete(r_sib, node, rr_sib);
	                                set_dirty(l_sib);//l_sib->set_dirty();
	                                /* trace upwards */
	                                non_leaf_remove(parent, i + 1);
	                        }
	                }
	        } else {
	                non_leaf_simple_remove(node, remove);
	                set_dirty(node);//node->set_dirty();
	        }
	}

	void leaf_shift_from_left(bptree_node *leaf,
	                		 bptree_node *left, bptree_node *parent,
	                		 int parent_key_index, int remove)
	{
	        /* right shift in leaf node */
	        memmove(&leaf->key()[1], &leaf->key()[0], remove * sizeof(bp_addr_t));
	        //memmove(&data(leaf)[1], &data(leaf)[0], remove * sizeof(off_t));

	        /* borrow the last element from left sibling */
	        leaf->key()[0] = left->key()[left->get_children() - 1];
	        //data(leaf)[0] = data(left)[left->get_children() - 1];
	        left->inc_children(-1);

	        /* update parent key */
	        parent->key()[parent_key_index] = leaf->key()[0];
	}

	void leaf_merge_into_left(bptree_node *leaf,
	                		 bptree_node *left, int parent_key_index, int remove)
	{
	        /* merge into left sibling, sum = leaf->get_children() - 1*/
	        memmove(&left->key()[left->get_children()], &leaf->key()[0], remove * sizeof(bp_addr_t));
	        //memmove(&data(left)[left->get_children()], &data(leaf)[0], remove * sizeof(off_t));
	        memmove(&left->key()[left->get_children() + remove], &leaf->key()[remove + 1], (leaf->get_children() - remove - 1) * sizeof(bp_addr_t));
	        //memmove(&data(left)[left->get_children() + remove], &data(leaf)[remove + 1], (leaf->get_children() - remove - 1) * sizeof(off_t));
	        left->inc_children(leaf->get_children() - 1);
	}

	void leaf_shift_from_right(bptree_node *leaf,
	                                  bptree_node *right, bptree_node *parent,
	                                  int parent_key_index)
	{
	        /* borrow the first element from right sibling */
	        leaf->key()[leaf->get_children()] = right->key()[0];
	        //data(leaf)[leaf->get_children()] = data(right)[0];
	        leaf->inc_children(1);

	        /* left shift in right sibling */
	        memmove(&right->key()[0], &right->key()[1], (right->get_children() - 1) * sizeof(bp_addr_t));
	        //memmove(&data(right)[0], &data(right)[1], (right->get_children() - 1) * sizeof(off_t));
	        right->inc_children(-1);

	        /* update parent key */
	        parent->key()[parent_key_index] = right->key()[0];
	}

	inline void leaf_merge_from_right(bptree_node *leaf,
	                                         bptree_node *right)
	{
	        memmove(&leaf->key()[leaf->get_children()], &right->key()[0], right->get_children() * sizeof(bp_addr_t));
	        //memmove(&data(leaf)[leaf->get_children()], &data(right)[0], right->get_children() * sizeof(off_t));
	        leaf->inc_children(right->get_children());
	}

	inline void leaf_simple_remove(bptree_node *leaf, int remove)
	{

        remove_key_from_datanode(leaf->key()[remove]);
        memmove(&leaf->key()[remove], &leaf->key()[remove + 1],
                (leaf->get_children() - remove - 1) * sizeof(bp_addr_t));
        //memmove(&data(leaf)[remove], &data(leaf)[remove + 1], (leaf->get_children() - remove - 1) * sizeof(off_t));
        leaf->inc_children(-1);

	}

	inline int parent_key_index(bptree_node *parent, bp_addr_t key)
	{
	        int index = key_binary_search(parent, key);
            if (index == INT32_MAX) return -1;
	        return index >= 0 ? index : -index - 2;
	}

	int leaf_remove(bptree_node *leaf, char *userkey, int keylength)
	{
			//printf("delete %.*s\n", 12, userkey + 4);
	        int remove = key_binary_search(leaf, userkey, keylength);
	        if (remove == INT32_MAX) return -1;
	        if (remove < 0) {
	                /* Not exist */
	                return -1;
	        }
			//printf("found the key\n");
	        if (leaf->header()->parent == invalid_key_addr) {
	                /* leaf as the root */
	                if (leaf->get_children() == 1) {
	                        /* delete the only last node */
	                        //assert(key == leaf->key()[0]);
	                        this->root = 0;
	                        this->level = 0;
	                        remove_key_from_datanode(leaf->key()[0]);
	                        node_delete(leaf, NULL, NULL);
	                } else {
	                        leaf_simple_remove(leaf, remove);
	                        set_dirty(leaf);//leaf->set_dirty();
	                }
	        } else if (leaf->get_children() <= (param.max_entries + 1) / 2) {
	                bptree_node *l_sib = pool.fetch_tree_node(leaf->header()->prev);
	                bptree_node *r_sib = pool.fetch_tree_node(leaf->header()->next);
	                bptree_node *parent = pool.fetch_tree_node(leaf->header()->parent);

	                int i = parent_key_index(parent, leaf->key()[0]);

	                /* decide which sibling to be borrowed from */
	                if (sibling_select(l_sib, r_sib, parent, i) == LEFT_SIBLING) {
	                        if (l_sib->get_children() > (param.max_entries + 1) / 2) {
	                                leaf_shift_from_left(leaf, l_sib, parent, i, remove);
	                                /* flush leaves */
	                                set_dirty(leaf);//leaf->set_dirty();
	                                set_dirty(l_sib);//l_sib->set_dirty();
	                                set_dirty(r_sib);//r_sib->set_dirty();
	                                set_dirty(parent);//parent->set_dirty();
	                        } else {
	                                leaf_merge_into_left(leaf, l_sib, i, remove);
	                                /* delete empty leaf and flush */
	                                node_delete(leaf, l_sib, r_sib);
	                                /* trace upwards */
	                                non_leaf_remove(parent, i);
	                        }
	                } else {
	                        /* remove at first in case of overflow during merging with sibling */
	                        leaf_simple_remove(leaf, remove);

	                        if (r_sib->get_children() > (param.max_entries + 1) / 2) {
	                                leaf_shift_from_right(leaf, r_sib, parent, i + 1);
	                                /* flush leaves */
	                                set_dirty(leaf);//leaf->set_dirty();
	                                set_dirty(l_sib);//l_sib->set_dirty();
	                                set_dirty(r_sib);//r_sib->set_dirty();
	                                set_dirty(parent);//parent->set_dirty();
	                        } else {
	                                leaf_merge_from_right(leaf, r_sib);
	                                /* delete empty right sibling flush */
	                                bptree_node *rr_sib = pool.fetch_tree_node(r_sib->header()->next);
	                                node_delete(r_sib, leaf, rr_sib);
 	                                set_dirty(l_sib);//l_sib->set_dirty();
	                                /* trace upwards */
	                                non_leaf_remove(parent, i + 1);
	                        }
	                }
	        } else {
	                leaf_simple_remove(leaf, remove);
	                set_dirty(leaf);//leaf->set_dirty();
	        }

	        return 0;
	}

	void node_delete(bptree_node *node, bptree_node *left, bptree_node *right)
	{
	        if (left != NULL) {
	                if (right != NULL) {
	                        left->header()->next = right->header()->self;
	                        right->header()->prev = left->header()->self;
	                        set_dirty(right);//right->set_dirty();
	                } else {
	                        left->header()->next = invalid_key_addr;
	                }
					set_dirty(left);//left->set_dirty();
	        } else {
	                if (right != NULL) {
	                        right->header()->prev = invalid_key_addr;
	                        set_dirty(right);//right->set_dirty();
	                }
	        }

	        assert(node->header()->self != invalid_key_addr);
	        pool.remove_treenode(node);
	}
public:
	///
	/// BINARY SEARCH
	///
	template<class InputIt1, class InputIt2>
	inline int kvkey_lex_compare(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2)
	{
	    for ( ; (first1 != last1) && (first2 != last2); ++first1, (void) ++first2 ) {
	        if (*first1 < *first2) return -1;
	        if (*first2 < *first1) return +1;
	    }

	    if (first1 == last1) {
	    	if (first2 == last2) return 0;
	    	return -1;
	    }

	    return +1;
	}

	inline bool is_same(const char *k1, const int k1_length, bp_addr_t &addr) {
		char *k2;
		int k2_length;
		pool.fetch_key(addr, &k2, k2_length );
		if (k2 == 0) throw "addr is not stored yet";
		return (kvkey_lex_compare(k1, k1+k1_length, k2, k2+k2_length) == 0);
	}

	inline bool is_larger(bp_addr_t &addr1, bp_addr_t &addr2) {
		char *k2, *k1;
		int k1_length, k2_length;
		pool.fetch_key(addr1, &k1, k1_length );
		pool.fetch_key(addr2, &k2, k2_length );
		return (kvkey_lex_compare(k1, k1+k1_length, k2, k2+k2_length) > 0);
	}

	inline bool is_larger(const char *k1, const int k1_length, const char *k2, const int k2_length) {
		return (kvkey_lex_compare(k1, k1+k1_length, k2, k2+k2_length) > 0);
	}

	inline int strcompare(const char *k1, const int k1_length, const char *k2, const int k2_length) {
		return (kvkey_lex_compare(k1, k1+k1_length, k2, k2+k2_length) );
	}

	int key_binary_search(bptree_node *node, const char *userkey, int keylength)
	{
		bool same;
		return key_binary_search(node, userkey, keylength, same);
	}

	int key_binary_search(bptree_node *node, const char *userkey, int keylength, bool &same)
	{
		bp_addr_t *arr = node->key();
		int len = node->is_leaf() ? node->get_children() : node->get_children() - 1;
		int low = -1;
		int high = len;
		char *key;
		int length;
		same = false;
		//std::cout << "children = " << node->get_children()  << ", low = " << low << ", high = " << high << std::endl;

		while (low + 1 < high) {
			int mid = low + (high - low) / 2;
			if (!pool.fetch_key(arr[mid], &key, length)) {
			    return INT32_MAX;
			}
			if (is_larger(userkey, keylength, key, length )) {
				low = mid;
			} else {
				high = mid;
			}
		}

		if (high < len) {
			same = is_same(userkey, keylength, arr[high]);
		}

		if (high >= len || !same) {
			return -high - 1;
		}
		return high;
	}


	int key_binary_search(bptree_node *node, bp_addr_t target)
	{
		char *target_key;
		int target_key_length;
		pool.fetch_key(target, &target_key, target_key_length);
		if (target_key == 0) return INT32_MAX;
		return key_binary_search(node, target_key, target_key_length);
	}

private:
	///
	/// Internal Iterators
	///
	friend class bptree_iterator_impl;

	class bptree_iterator_impl: public bptree_iterator {
        bp_addr_t root_addr;
        bptree *root_tree;
	public:
		bptree_iterator_impl(bptree *tree_, int keyindex_ = 0, bp_addr_t nodeaddr_ = 0):
			bptree_iterator(tree_, keyindex_, nodeaddr_)
		{
		    //TR << "bptree iterator: index = " << keyindex << ", nodeaddr == " << nodeaddr ;
            root_tree = tree_;
            root_addr = nodeaddr_;
		    if (nodeaddr == 0) {
		        end();
		    }
		}

		inline void reset() {
            tree = root_tree;
            nodeaddr = root_addr;
            keyindex = 0;
		}

		virtual void move_next(const long int movement) {
            //TR << "MOVE NEXT " ;
			if (tree == 0) {
                end();
                return;
			}

			bptree_node *node = tree->pool.fetch_tree_node(nodeaddr);
			if (!node) {
				//std::cout << "nodeaddr " << nodeaddr << ", not found\n";
				end();
				return;
			}

            //TR << "MOVE NEXT - index+1" ;
			this->keyindex++;

			if (this->keyindex >= node->get_children()) {

				node = tree->pool.fetch_tree_node(node->header()->next);
				if (node == 0) {
                    //TR << "REACHED TO THE END" ;
					end();
				} else {
                    //TR << "REACHED TO THE NODE END" ;
					this->keyindex = 0;
					nodeaddr = node->addr;
				}
			}
		}

		virtual bool get_key(char **key, int &length) {
            FTRACE

			if (is_end()) return false;
			auto *node = tree->pool.fetch_tree_node(nodeaddr);
            //TR << "fetch  NODE  " << node ;
			if (!node) return false;

            //TR << "get key from  NODE addr " << nodeaddr ;
            //node->dump();


            return tree->get_key_from_datanode(node->key()[keyindex], key, length);
		}

		virtual void begin() {
		    //TR<< "BEGIN" ;
            reset();
            if (tree == 0) return;

			lower_bound("", 0);
		}

		virtual void end() {
			tree = 0; keyindex = -1; nodeaddr = 0;
		}


		virtual void lower_bound(const char *key, int length) {
			if (is_end() || tree->root == 0 ) { reset(); }
            if (tree == 0) return;

			bptree_node *node = tree->root;
			keyindex = -1;

			while (node != 0) {
				int i = tree->key_binary_search(node, key, length);
				if (i == INT32_MAX) return;
				if (node->is_leaf()) {
					if ( i < 0) {
						i = -i -1;
						if (i >= node->get_children()) {
							nodeaddr = node->header()->next;
							node = tree->pool.fetch_tree_node(nodeaddr);
						}
					}
					if (node != 0) {
						nodeaddr = node->addr;
						keyindex = i;
					}
					break;
				} else {
					if (i >= 0) {
						node = tree->pool.fetch_tree_node(node->sub()[i+1]);
					} else {
						i = -i -1;
						node = tree->pool.fetch_tree_node(node->sub()[i]);
					}
				}
			}
		}

		virtual void upper_bound(const char *key, int length) {
			if (is_end() || tree->root == 0) { reset(); }
            if (tree == 0) return;
            
			bool same;
			bptree_node *node = tree->root;
			keyindex = -1;
			while (node != 0) {
				int i = tree->key_binary_search(node, key, length, same);
                if (i == INT32_MAX) return;
				if (node->is_leaf()) {
					if ( i < 0) {
						i = -i -1;
						if (i >= node->get_children()) {
							nodeaddr = node->header()->next;
							node = tree->pool.fetch_tree_node(nodeaddr);
						}
					}
					if (node != 0) {
						if (same) {
							i++;
							if (i >= node->get_children()) {
								nodeaddr = node->header()->next;
								node = tree->pool.fetch_tree_node(nodeaddr);
								i = 0;
							}
						}
						nodeaddr = node->addr;
						keyindex = i;
					}
					break;
				} else {
					if (i >= 0) {
						node = tree->pool.fetch_tree_node(node->sub()[i+1]);
					} else {
						i = -i -1;
						node = tree->pool.fetch_tree_node(node->sub()[i]);
					}
				}
			}
		}

	};
public:
	bptree_iterator *get_iterator() {
		return new bptree_iterator_impl(this, 0, (this->root)? this->root->addr:0);
	}
};



#endif /* SRC_KADI_KADI_BPTREE_H_ */
