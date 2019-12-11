/*
 * B-tree set (C++)
 *
 * Copyright (c) 2018 Project Nayuki. (MIT License)
 * https://www.nayuki.io/page/btree-set
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 * - The above copyright notice and this permission notice shall be included in
 *   all copies or substantial portions of the Software.
 * - The Software is provided "as is", without warranty of any kind, express or
 *   implied, including but not limited to the warranties of merchantability,
 *   fitness for a particular purpose and noninfringement. In no event shall the
 *   authors or copyright holders be liable for any claim, damages or other
 *   liability, whether in an action of contract, tort or otherwise, arising from,
 *   out of or in connection with the Software or the use or other dealings in the
 *   Software.
 */

#ifndef SRC_KADI_KADI_BTREE_H_
#define SRC_KADI_KADI_BTREE_H_

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>
#include <map>
#include <iostream>

#include "kadi_cmds.h"

using namespace std;

template<class InputIt1, class InputIt2>
inline int kvkey_lex_compare(InputIt1 first1, InputIt1 last1, InputIt2 first2,
		InputIt2 last2) {
	for (; (first1 != last1) && (first2 != last2); ++first1, (void) ++first2) {
		if (*first1 < *first2)
			return -1;
		if (*first2 < *first1)
			return +1;
	}

	if (first1 == last1) {
		if (first2 == last2)
			return 0;
		return -1;
	}

	return +1;
}

class SKNode;

class NodePool final {
	uint32_t seq;
	KADI *adi;
	uint32_t prefix;
	uint32_t ksid_skp;
	std::map<int, std::unique_ptr<SKNode> > pool;
	std::vector<std::unique_ptr<SKNode> > erased_pages;

public:
	struct meta_io_context {
		struct __attribute__((packed)) skp_seq_key {
			uint32_t prefix;
			char postfix[4];
		} key_seq_body;

		bool done;
		bool error;
		bool found;
		uint64_t seq = 0;
		const int total_ops = 2;
		int num_finished = 0;
		int num_errors = 0;
		kv_key key_seq = { &key_seq_body, sizeof(skp_seq_key) };
		kv_value value_seq = { 0, 8, 0, 0, 0 };

		std::mutex load_mutex;
		std::condition_variable load_cond;

		meta_io_context() {
			done = false;
			found = false;
			error = false;
			value_seq.value = malloc(8);
			key_seq_body.postfix[0] = '_';
			key_seq_body.postfix[1] = 's';
			key_seq_body.postfix[2] = 'e';
			key_seq_body.postfix[3] = 'q';
		}

		~meta_io_context() {
			free(value_seq.value);
		}

		uint64_t wait() {
			std::unique_lock<std::mutex> lg(load_mutex);
			if (!done)
				load_cond.wait(lg, [&] {return done;});

			error |= (seq == 0);
			if (error) {
				throw new runtime_error("IO error - metactx");
			}

			//std::cout << "last seq = " << seq << std::endl;
			return seq;
		}

	} metactx;

	struct flush_context {
		uint64_t num_ios = 0;
		uint64_t num_done = 0;
		int status = -1; // 0 write_submitted, 1 written, 2 erase submitted, 2 erased, -1
		std::mutex lock;
		std::condition_variable cond;

		void iodone(int opcode) {
			std::unique_lock<std::mutex> lg(lock);
			num_done++;
			if (opcode != 0)
				status = -2;
			if (num_ios == num_done) {
				cond.notify_all();
			}
		}

		bool wait() {
			std::unique_lock<std::mutex> lg(lock);
			if (num_ios != num_done)
				cond.wait(lg, [&] {return (num_ios == num_done);});
			if (status == -2) {
				return false;
			}
			status++;
			return true;

		}
	} flush_ctx;

	std::unique_ptr<SKNode> temp;

public:
	NodePool();

	void initialize(KADI *adi_, int ksid_skp_, uint32_t prefix_) {
		this->seq = 1;
		this->adi = adi_;
		this->prefix = prefix_;
		this->ksid_skp = ksid_skp_;
	}

	int load_metadata_lazy();
	int erase_node(SKNode *node);
	int erase_meta();
	void swap_id(uint32_t a, uint32_t b);

	void replace(uint32_t id, SKNode *node);
	SKNode* wait_for_meta();

	int flush_sknode_async(SKNode *node, bool erase, struct flush_context *ctx);
	void flush();

	std::unique_ptr<SKNode> lookup_erase(uint32_t id) {
		auto it = pool.find(id);
		if (it != pool.end()) {
			std::unique_ptr<SKNode> s = std::move(it->second);
			pool.erase(it);
			return std::move(s);
		}
		return 0;
	}

	bool erase(uint32_t id);

	SKNode* lookup(uint32_t id) {
		auto it = pool.find(id);
		if (it != pool.end()) {
			return it->second.get();
		}
		return 0;
	}

	SKNode* load_sknode_sync(uint32_t id);
	SKNode* create_sknode(bool isleaf);
	SKNode* create_sknode(uint32_t id, bool isleaf);

	void clear() {
		pool.clear();
	}

};

class SKNode final {
public:
	static const int MAX_KEYSIZE = 255;
	static const int DEFAULT_SKNODE_BUFFER_SIZE = 28*1024;
private:
	class NodeList: public std::vector<uint32_t> {
		NodePool *nodepool;
	public:
		using std::vector<uint32_t>::vector;

		NodeList(NodePool *nodepool_) :
				nodepool(nodepool_) {
		}

		SKNode* at(uint32_t index) {
			if (index == this->size())
				return 0;
			const uint32_t id = (*this)[index];
			SKNode *n = nodepool->lookup(id);
			if (n != 0)
				return n;
			return nodepool->load_sknode_sync(id);
		}

	};
public:
	struct __attribute__((packed)) skp_key {
		uint32_t prefix;
		uint32_t id;
	} skpkey_body;

public:
	bool isleaf;
	uint32_t buffer_offset;
	kv_key key;
	kv_value value;
	std::vector<char*> keys;
	NodeList children;
	bool dirty;
	uint32_t id;
	int invalidkeys;
	bool lazy_init;
	NodePool *nodepool;

	// new node
	SKNode(NodePool *nodepool_, uint32_t id_, uint32_t prefix_, bool leaf_,
			void *buffer, int buf_length) :
			isleaf(leaf_), children(nodepool_), dirty(true), id(id_), nodepool(
					nodepool_) {
		key.key = &skpkey_body;
		key.length = sizeof(SKNode::skp_key);
		skpkey_body.id = id_;
		skpkey_body.prefix = prefix_;
		value.value = buffer;
		value.length = buf_length;
		value.offset = 0;
		buffer_offset = 0;
		invalidkeys = 0;
		lazy_init = false;
	}

	// load
	SKNode(NodePool *nodepool_, uint32_t id, uint32_t prefix, void *buffer,
			int buf_length) :
			SKNode(nodepool_, id, prefix, false, buffer, buf_length) {
		dirty = false;
		lazy_init = true;
	}

	bool binary_search(const char *key, uint8_t length, int &index) {
		int l = 0;
		int r = keys.size();

		char *midkey;
		uint8_t midkey_length;

		int mid = (r - l) / 2 + l;

		while (l < r) {
			get_item(mid, &midkey, midkey_length);

			const int result = kvkey_lex_compare(midkey, midkey + midkey_length,
					key, key + length);

			if (result == 0) {
				index = mid;	// found
				return true;
			} else if (result > 0) {
				r = mid;
			} else {
				l = mid + 1;
			}

			mid = (r - l) / 2 + l;
		}

		index = mid;
		return false;
	}

public:
	// tree structure

	void splitChild(std::size_t minKeys, std::size_t index) {
		assert(!this->isLeaf());

		char *middle_key;
		SKNode *left = this->children.at(index);
		SKNode *right = nodepool->create_sknode(left->isLeaf());	// new node

		int middle_index = left->keys.size() / 2;	//minKeys;
		uint8_t middle_key_length;

		this->children.insert(this->children.begin() + index + 1, right->id);

		left->get_item(middle_index, &middle_key, middle_key_length);

		// Handle children
		if (!left->isLeaf()) {
			std::move(left->children.begin() + middle_index + 1,
					left->children.end(), std::back_inserter(right->children));
			left->children.erase(left->children.begin() + middle_index + 1,
					left->children.end());
		}

		this->insert_key(index, middle_key, middle_key_length);
		copy_keys(left, middle_index + 1, right);
		truncate_keys(left, middle_index);

	}

	SKNode* ensureChildRemove(std::size_t minKeys, std::uint32_t index) {
		assert(!this->isLeaf() && index < this->children.size());
		SKNode *child = this->children.at(index);
		if (child->keys.size() > minKeys)  // Already satisfies the condition
			return child;


		assert(child->keys.size() == minKeys);

		// Get siblings
		SKNode *left = index >= 1 ? this->children.at(index - 1) : nullptr;
		SKNode *right =
				index < this->keys.size() ?
						this->children.at(index + 1) : nullptr;
		bool internal = !child->isLeaf();
		assert(left != nullptr || right != nullptr); // At least one sibling exists because degree >= 2
		assert(left == nullptr || left->isLeaf() != internal); // Sibling must be same type (internal/leaf) as child
		assert(right == nullptr || right->isLeaf() != internal); // Sibling must be same type (internal/leaf) as child

		if (left != nullptr && left->keys.size() > minKeys) { // Steal rightmost item from left sibling
		//std::cout << "ensureChildRemove: left, right most -> child " <<std::endl;

			if (child->invalidkeys > 0 && child->isfull(0)) {
				if (!child->compact()) {
					throw "overflow error";
				};
			}
			if (this->invalidkeys > 0 && this->isfull(0)) {
				if (!this->compact()) {
					throw "overflow error";
				};
			}

			if (internal) {
				child->children.insert(child->children.begin(),
						left->children.back());
				left->children.pop_back(); // moved
			}

			//1. child->keys.insert(child->keys.begin(), this->keys.at(index - 1));
			char *movedkey = this->keys.at(index - 1);
			child->insert_key(0, movedkey + 1, *(uint8_t*) movedkey);

			//2. this->keys.at(index - 1) =left->removeKey(left->keys.size() - 1);
			movedkey = left->removeKey(left->keys.size() - 1);
			this->update_key(index - 1, movedkey + 1, *(uint8_t*) movedkey);
			return child;
		} else if (right != nullptr && right->keys.size() > minKeys) { // Steal leftmost item from right sibling
		//std::cout << "ensureChildRemove: child <- right, left most " <<std::endl;

			if (child->invalidkeys > 0 && child->isfull(0)) {
				if (!child->compact()) {
					throw "overflow error";
				};
			}

			if (this->invalidkeys > 0 && this->isfull(0)) {
				if (!this->compact()) {
					throw "overflow error";
				};
			}

			if (internal) {
				child->children.push_back(right->children.front());
				right->children.erase(right->children.begin());      //move
			}

			child->append_keybuffer(this->keys.at(index));

			char *movedkey = right->removeKey(0);

			this->update_key(index, movedkey + 1, *(uint8_t*) movedkey);

			return child;
		} else if (left != nullptr) {  // Merge child into left sibling
			this->mergeChildren(minKeys, index - 1);
			return left; // This is the only case where the return value is different
		} else if (right != nullptr) {  // Merge right sibling into child
			this->mergeChildren(minKeys, index);
			return child;
		} else
			throw std::logic_error("Impossible condition");
	}

	// Merges the child node at index+1 into the child node at index,
	// assuming the current node is not empty and both children have minKeys.
public:
	void mergeChildren(std::size_t minKeys, std::uint32_t index) {
		assert(!this->isLeaf() && index < this->keys.size());

		SKNode &left = *children.at(index + 0);
		SKNode &right = *children.at(index + 1);

		if (left.invalidkeys > 0 && left.isfull(0)) {
			if (!left.compact()) {
				throw "overflow error";
			};
		}

		//left.print_keys(); right.print_keys();

		assert(left.keys.size() == minKeys && right.keys.size() == minKeys);
		if (!left.isLeaf())
			std::move(right.children.begin(), right.children.end(),
					std::back_inserter(left.children));

		left.append_keybuffer(removeKey(index));

		copy_keys(&right, 0, &left);

		nodepool->erase(children[index + 1]);
		children.erase(children.begin() + index + 1);

	}

	char* removeMin(std::size_t minKeys) {
		for (SKNode *node = this;;) {
			if (node->isLeaf())
				return node->removeKey(0);
			else
				node = node->ensureChildRemove(minKeys, 0);
		}
		assert(0);
		return 0;
	}

	char* removeMax(std::size_t minKeys) {
		for (SKNode *node = this;;) {
			assert(node->keys.size() > minKeys);
			if (node->isLeaf())
				return node->removeKey(node->keys.size() - 1);
			else
				node = node->ensureChildRemove(minKeys,
						node->children.size() - 1);
		}
		return 0;
	}

public:
	// helpers

	inline uint32_t _write_key_to_buffer(const char *key, uint8_t length) {
		uint32_t startoffset = buffer_offset;
		if (startoffset + length + 1 > value.length) {
			if (invalidkeys > 0) {
				compact();
			}
			startoffset = buffer_offset;
			if (startoffset + length + 1 > value.length) {
				printf("overflow\n");
				exit(1);
			}
		}

		char *buffer = (char*) value.value;
		memcpy(buffer + buffer_offset, &length, 1);
		buffer_offset += 1;
		memcpy(buffer + buffer_offset, key, length);
		buffer_offset += length;
		//std::cout << "add key length " << (int)length << ": buffer offset = " << buffer_offset <<std::endl;
		if (startoffset > value.length) {
			printf("overflow\n");
			exit(1);
		}

		dirty = true;
		return startoffset;
	}

	int insert_child(int index, SKNode *node) {
		this->children.insert(children.begin() + index, node->id);
		return 0;
	}

	int insert_key(const int mid, const char *key, uint8_t length) {
		// write the string to the buffer
		char *kv_pos = (char*) value.value + _write_key_to_buffer(key, length);
		// update the in-memory structure
		this->keys.insert(this->keys.begin() + mid, kv_pos);

		return 0;
	}

	int update_key(const int mid, const char *key, uint8_t length) {
		// write the string to the buffer
		char *kv_pos = (char*) value.value + _write_key_to_buffer(key, length);
		// update the in-memory structure
		this->keys.erase(this->keys.begin() + mid);
		this->keys.insert(this->keys.begin() + mid, kv_pos);
		this->invalidkeys++;

		return 0;
	}

	int append_keybuffer(const char *buf) {
		// write the string to the buffer
		const uint8_t length = *(uint8_t*) buf;
		const char *key = buf + sizeof(uint8_t);
		//std::cout << "copy key = " << std::string(key, length) << ", to offset " << buffer_offset<< std::endl;
		char *kv_pos = (char*) value.value + _write_key_to_buffer(key, length);
		this->keys.push_back(kv_pos);

		return 0;
	}

	void finalize() {
		int index = -1;
		uint16_t *footer = (uint16_t*) ((char*) value.value + value.length);

		footer[index--] = this->children.size();
		footer[index--] = this->keys.size();
		footer[index--] = this->invalidkeys;
		footer[index--] = buffer_offset;

		// write key offsets
		for (const char *entry : this->keys) {
			footer[index--] = (entry - (char*) value.value);
		}

		// write node id
		for (size_t i = 0; i < this->children.size(); i++) {
			footer[index--] = children.at(i)->id;// place holder for on-demand loading
		}

		//std::cout << "finalized Node " << id << ", buffer_offset = " << buffer_offset << std::endl;
	}

	bool isLeaf() const {
		return children.empty();
	}

	void copy_keys(SKNode *from, int from_index, SKNode *to) {
		auto it = from->keys.begin() + from_index;
		while (it != from->keys.end()) {
			to->append_keybuffer((*it));
			it++;
		};
	}

	void truncate_keys(SKNode *from, int from_index) {
		from->invalidkeys = from->keys.size() - from_index;
		from->keys.erase(from->keys.begin() + from_index, from->keys.end());
		from->compact();

	}

	bool compact() {
		if (this->invalidkeys == 0)
			return false;

		int newbuffer_pos = 0;
		char *newbuffer = (char*) malloc(value.length);

		const int num_keys = this->keys.size();
		for (int i = 0; i < num_keys; i++) {
			char *b = this->keys[i];
			const uint8_t length = (*(uint8_t*) b) + 1;
			memcpy(newbuffer + newbuffer_pos, b, length);

			this->keys[i] = newbuffer + newbuffer_pos;
			newbuffer_pos += length + 1;
		}

		free(value.value);

		value.value = newbuffer;
		buffer_offset = newbuffer_pos;
		invalidkeys = 0;

		return true;
	}

	int parse() {
		if (!lazy_init)
			return 0;

		int index = -1;
		uint16_t *footer = (uint16_t*) ((char*) value.value + value.length);
		int num_children = footer[index--];
		int num_keys = footer[index--];
		this->invalidkeys = footer[index--];
		this->buffer_offset = footer[index--];
		this->isleaf = (num_children == 0);

		for (int i = 0; i < num_keys; i++) {
			this->keys.push_back((char*) value.value + footer[index--]);
		}

		for (int i = 0; i < num_children; i++) {
			children.push_back(footer[index--]);
		}

		return 0;
	}

	// footer - isleaf,  last offset, # of invalid keys, # of valid keys,
	inline bool isfull(int length) {
		const int newentry_size = (length + 1);
		const int metadata_size = 4 * sizeof(uint16_t);
		const int newkey_size = (keys.size() + 1) * sizeof(uint16_t);
		const int children_size =
				(isleaf) ? (newkey_size + 1) * sizeof(uint16_t) : 0;
		const int overflow_size = MAX_KEYSIZE + 1;
		//printf("isfull: buffer_offset = %d, metadata size = %d, new offset = %d\n", buffer_offset, metadata_size+ newkey_size+ children_size , buffer_offset + overflow_size + newentry_size + metadata_size + newkey_size + children_size);
		return (buffer_offset + overflow_size + newentry_size + metadata_size
				+ newkey_size + children_size) > value.length;
	}

	inline void get_item(int pos, char **key, uint8_t &keylength) {
		char *buf = keys[pos];
		keylength = *(uint8_t*) buf;
		*key = buf + sizeof(uint8_t);
	}
	char* removeKey(std::uint32_t index) {
		char *result = keys.at(index);
		keys.erase(keys.begin() + index);
		invalidkeys++;
		return result;
	}

public:
	// debug functions

	void dump() {
		std::cout << ">> Node id = " << id << ", offset " << buffer_offset
				<< std::endl;
		for (char *pos : this->keys) {
			std::cout << ">>  key = " << std::string(pos + 1, *(uint8_t*) pos)
					<< std::endl;
		}
		std::cout << ">> done" << std::endl;
	}

	void print_keys(std::string header = "") {
		if (header.length() > 0)
			printf("[%s] ", header.c_str());
		printf("nodeid = %d, %ld keys: offset = %d \n", id, keys.size(),
				buffer_offset);
		for (char *key : keys) {
			const uint8_t length = *(uint8_t*) key;
			const char *k = key + sizeof(uint8_t);
			std::cout << std::string(k, length) << " ";
		}
		printf("\n");
	}

	void print_children() {
		printf("chilren: \n");
		for (uint32_t id : children) {
			printf("%d ", id);
		}
		printf("\n");
	}

};

class SKTree final {
private:
	SKNode *root;
	uint32_t minKeys;  // At least 1, equal to degree-1
	std::size_t count;
	std::size_t exist = 0;
	NodePool nodepool;
public:

	SKTree(KADI *adi_, int ksid_skp_, uint32_t prefix_) :
			root(0), count(0) {

		minKeys =(SKNode::DEFAULT_SKNODE_BUFFER_SIZE - 4 * sizeof(uint16_t) /*metadata_size*/)
				 / (SKNode::MAX_KEYSIZE + 1 + sizeof(uint16_t) + /* children id*/ sizeof(uint16_t) - 1);

		if (minKeys == 0) {
			throw "BUFFER SIZE is too small";
		}

		//std::cout << "min keys = " << minKeys << std::endl;
		nodepool.initialize(adi_, ksid_skp_, prefix_);
		nodepool.load_metadata_lazy();
	}

	void wait_until_initialized() {
		root = nodepool.wait_for_meta();
	}

public:
	void erase_all() {
		std::vector<SKNode *> nodes;
		_find_all(root, nodes);
		nodes.push_back(root);

		for (SKNode *n : nodes) {
			nodepool.erase_node(n);
		}
		nodepool.erase_meta();
		//std::cout << "done" << std::endl;
	}

	void _find_all(SKNode *node, std::vector<SKNode *> &nodes) {
		if (node->isLeaf()) {
			return;
		}
		else {

			for (int i =0 ; i < (int) node->children.size() ; i++) {
				try {
					SKNode *p = node->children.at(i);
					nodes.push_back(p);
					_find_all(p, nodes);
				}
				catch(std::runtime_error *err) {}
			}
		}
	}

	bool find(const char *key, uint8_t length, bool verbose = false) {
		SKNode *node = root;
		while (true) {
			int index;
			bool found = node->binary_search(key, length, index);

			if (found) {
				if (verbose) {
					std::string found = "found " + std::string(key, length);
					node->print_keys(found);
				}
				return true;
			}

			if (node->isLeaf()) {
				if (verbose) {
					std::string found = "not found " + std::string(key, length);
					node->print_keys(found);
				}
				return false;
			} else {
				node = node->children.at(index);
			}
		}
		return false;
	}

	bool insert(const char *key, uint8_t length) {
		if (root->isfull(length)) {
			SKNode *child = root;
			root = nodepool.create_sknode(false); // std::unique_ptr<Node>(new Node(maxKeys, false)); // Increment tree height
			nodepool.swap_id(child->id, root->id); //  root_id = 0 <-> child_id = 1

			root->children.push_back(child->id);
			root->splitChild(minKeys, 0);
		}

		SKNode *node = root;
		while (true) {
			assert(!node->isfull(length));
			int index;
			bool found = node->binary_search(key, length, index);

			if (found) {
				//std::cout << "set1: exist key = " << std::string(key, length)<< std::endl;
				exist++;
				return true;
			}

			if (node->isLeaf()) {
				node->insert_key(index, key, length);
				count++;
				return true;
			} else {
				SKNode *child = node->children.at(index);
				if (child->isfull(length)) {
					node->splitChild(minKeys, index);
					char *mid_entry = node->keys.at(index);
					const int result = kvkey_lex_compare(mid_entry + 1,
							mid_entry + 1 + *(uint8_t*) mid_entry, key,
							key + length);
					if (result == 0)
						return true;
					else if (result < 0)
						child = node->children.at(index + 1);
				}
				node = child;
			}
		}
		return false;
	}

	std::size_t erase(const char *key, uint8_t length) {
		// Walk down the tree
		int index;
		bool found = root->binary_search(key, length, index);

		SKNode *node = root;
		while (true) {
			if (node->isLeaf()) {
				if (found) {
					// Simple removal from leaf
					node->removeKey(index);
					count--;
					return 1;
				} else
					return 0;

			} else {  // Internal node
				if (found) {
					// Key is stored at current node

					SKNode *left = node->children.at(index + 0);
					SKNode *right = node->children.at(index + 1);

					if (left->keys.size() > minKeys) { // Replace key with predecessor
						node->keys.at(index) = left->removeMax(minKeys);
						assert(count > 0);
						count--;
						return 1;
					} else if (right->keys.size() > minKeys) { // Replace key with successor
						node->keys.at(index) = right->removeMin(minKeys);
						assert(count > 0);
						count--;
						return 1;
					} else { // Merge key and right node into left node, then recurse
						node->mergeChildren(minKeys, index);
						if (node == root && root->keys.empty()) {
							assert(root->children.size() == 1);
							SKNode *newRoot = root->children.at(0);

							// remove root from the pool
							nodepool.replace(0, newRoot);

							root = newRoot;  // Decrement tree height
						}
						node = left;
						index = minKeys; // Index known due to merging; no need to search
					}

				} else {  // Key might be found in some child
					SKNode *child = node->ensureChildRemove(minKeys, index);
					if (node == root && root->keys.empty()) {
						assert(root->children.size() == 1);
						SKNode *newRoot = root->children.at(0);

						// remove root from the pool
						nodepool.replace(0, newRoot);

						root = newRoot;  // Decrement tree height
					}
					node = child;
					found = node->binary_search(key, length, index);
				}
			}
		}
	}

	void flush() {
		nodepool.flush();
	}

	void dump() {
		root->dump();
	}

	bool empty() const {
		return count == 0;
	}

	std::size_t size() const {
		return count;
	}

};

#endif /* SRC_KADI_KADI_BTREE_H_ */
