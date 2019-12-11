#include "kadi_cmds.h"
#include "kadi_sort.h"
#include <iostream>

inline std::string construct_stringdict_key(uint32_t bitmap, uint32_t prefix) {
	return std::string("dict_") + std::to_string(bitmap) + "_" + std::to_string(prefix);
}

inline std::string construct_ssp_key(uint32_t bitmap, uint32_t prefix, uint32_t id) {
	return std::string("ssp_") + std::to_string(bitmap) + "_" + std::to_string(prefix) + "_" + std::to_string(id);
}

struct __attribute__((packed)) skp_key {
	char header[4];
	uint32_t prefix;
	uint32_t id;
};

inline kv_key *create_skp_key(uint32_t prefix, uint32_t id) {
	static const char *hdr = "SKP_";
	kv_key *k = new kv_key();
	k->length = sizeof(skp_key);
	k->key = malloc(k->length);
	auto sk = ((struct skp_key*)k->key);
	memcpy(sk->header, hdr, 4);
	sk->prefix = prefix;
	sk->id = id;
	return k;
}

inline kv_value *create_skp_value(int valuesize = 28*1024) {
	kv_value *k = new kv_value();
	k->length = valuesize;
	k->value  = malloc(k->length);
	k->offset = 0;
	return k;
}

void skp_callback2(kv_io_context &op, void* private_data)
{
	SortedKeyPage *ctx = (SortedKeyPage *)private_data;
	ctx->io_finished(op.retcode);
}

void SortedKeyPage::io_finished(int retcode)
{
	std::lock_guard<std::mutex> lg(load_mutex);
	io_ret  = retcode;
	io_done = true;

	load_cond.notify_one();
}

int SortedKeyPage::init(KADI *adi, int ksid_skp, uint32_t prefix, int id) {
	this->kadi = adi;
	this->prefix = prefix;
	this->ksid_skp = ksid_skp;
	this->id = id;
	skp_key = create_skp_key(prefix, 0);
	skp_value = create_skp_value();

	return this->kadi->kv_retrieve_aio(ksid_skp, skp_key, skp_value, { skp_callback2, this} );
}

int SortedKeyPage::wait_for_directory_page() {
	{
		std::unique_lock<std::mutex> lg(this->load_mutex);
		load_cond.wait(lg, [&]{ return io_done; });
	}

	this->directory = new SortedDirectory();

	if (isnew) {
		this->directory->init(true, create_skp_value());
	}
	else {
		this->directory = new SortedDirectory();
					this->directory->load_index(value);

	load_directory( (io_ret == 0), skp_key, skp_value );

	return 0;
}

int SortedKeyPage::load_skp_sync(int id) {
	kv_key* key = create_skp_key(prefix, id);
	kv_value *value = create_skp_value();

	int ret = this->kadi->kv_retrieve_sync(ksid_skp, key, value);
	if (ret != 0) return -1;

	load_data (false, key, value);

	return 0;
}
// new, existing
void SortedKeyPage::load_directory(bool isnew, kv_key *key, kv_value *value) {
		/*
		const char *buffer = (char *)value->value;
		const uint16_t *footer = (uint16_t*)(buffer + value->length);
		const bool isleaf = (footer[-1] == 1);
		if (isleaf) {
			SortedData *d = new SortedData();
			d->load_index(false, value);

			this->directory = new SortedDirectory();
			this->directory->load_index(true, create_skp_value());
			this->directory->add_entry(d->min_key(), key);
			this->data.push_back(d);
		}
		else {
			// load the directory page
			this->directory = new SortedDirectory();
			this->directory->load_index(value);
		}
		*/
	}
}

int SortedKeyPage::SortedData::load_index(bool is_new, kv_value *value) {
	this->dirty  = false;
	this->finalized = false;
	this->value = value;
	if (is_new) {
		buffer_offset = 0;
		num_invalid_keys = 0;
	}
	else {
		int ne_index = -1;
		const uint16_t *footer = (uint16_t*)((char*)value->value + value->length);

		isleaf = (footer[ne_index--] == 1)? true:false;
		buffer_offset = footer[ne_index--];
		num_invalid_keys = footer[ne_index--];
		const int valid_records = footer[ne_index--];

		for (int i =0 ; i < valid_records; i++) {
			keylist.push_back((char*)value->value+footer[ne_index--]);
		}
	}
	return 0;
}

void SortedKeyPage::SortedData::finalize() {
	uint16_t index = -1;
	uint16_t *footer = (uint16_t*)((char*)value->value + value->length);

	footer[index--] = 1; // isleaf
	footer[index--] = buffer_offset;
	footer[index--] = num_invalid_keys;
	footer[index--] = keylist.size();

	for (const char *entry: keylist) {
		footer[index--] = (entry - (char*)value->value);
	}

	finalized = true;
}


bool SortedKeyPage::SortedData::local_compact() {
	if (num_invalid_keys == 0) return false;

	int newbuffer_pos = 0;
	char *newbuffer = (char*) malloc(value->length);

	const int num_keys = this->keylist.size();
	for (int i =0 ;i < num_keys; i++) {
		char *b = this->keylist[i];
		const uint8_t length = *(uint8_t*)b + 1;
		memcpy(newbuffer + newbuffer_pos, b, length);

		this->keylist[i] = (char*)value->value + newbuffer_pos;
		newbuffer_pos += length;
	}

	free(value->value);

	value->value  = newbuffer;
	buffer_offset = newbuffer_pos;
	num_invalid_keys = 0;

	return true;
}


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

int SortedKeyPage::SortedData::find_insert_pos(ssl_type &arr, const char *key, uint8_t length, bool &newentry)
{
	int l = 0;
	int r = arr.size();

	if (l >= r || r == 0) {
		newentry = true;
		return -1;
	}

	char *midkey;
	uint8_t midkey_length;

	int mid = (r - l) / 2 + l;
	while (l < r) {
		get_item(mid, &midkey, midkey_length);

		const int result = kvkey_lex_compare(midkey, midkey+midkey_length, key, key+length);

		if (result == 0) {
			newentry = false;
			return mid;
		}
		else if (result > 0) {
			r = mid ;
		}
		else {
			l = mid +1;
		}

		mid = (r - l) / 2 + l;
	}
	newentry = true;
	return mid;
}

void SortedKeyPage::SortedData::dump()
{
	std::cout << "sorted key page dump ----" << std::endl;

	for (char *b: this->keylist) {
		uint8_t len = *(uint8_t*)b;
		std::cout << std::string(b + 1, len) << std::endl;
	}
	std::cout << "----" << this->keylist.size() << " keys are found" << std::endl;
}

int SortedKeyPage::SortedData::remove(const char *key, uint8_t length) {
	bool isnewentry;

	const int mid = find_insert_pos(this->keylist, key, length, isnewentry);
	if (isnewentry) return -1;

	this->keylist.erase( this->keylist.begin() + mid );

	this->num_invalid_keys++;

	return (mid == 0)? 1:0;
}

int SortedKeyPage::SortedData::insert(const char *key, uint8_t length) {
	bool isnewentry;

	const int mid = find_insert_pos(this->keylist, key, length, isnewentry);

	// same key?
	if (!isnewentry) return 0;

	// no space? try compacting
	if (isfull(length)) {
		if (!local_compact()) return 1;
	}

	// write the string to the buffer
	char *kv_pos = (char*)value->value + _write_key_to_buffer(key, length);

	// update the in-memory structure
	if (mid != -1) {
		this->keylist.insert( this->keylist.begin() + mid, kv_pos );
	}
	else {
		this->keylist.push_back(kv_pos);
	}

	dirty = true;
	return 0;
}


int SortedKeyPage::insert(const char *key, uint8_t length)
{
	SortedData *d = this->directory->get_data_page(key, length);
	int ret = d->insert(key, length);
	if (ret == 1) {
		// split

	}
	return 0;
}

int SortedKeyPage::remove(const char *key, uint8_t length)
{
	SortedData *d = this->directory->get_data_page(key, length);
	int ret = d->remove(key, length);
	if (ret == 1) {
		// new min
	}
	return ret;
}

int SortedKeyPage::SortedDirectory::load_index(kv_value *value)
{
	this->value = value;


}

int SortedKeyPage::SortedDirectory::add_entry(char *entry, kv_key *skp_key) {

	return 0;
}

SortedKeyPage::SortedData *SortedKeyPage::SortedDirectory::get_data_page(const char *key, uint8_t length) {

	return 0;
}


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



class BTreeSet final {

	private: class Node;  // Forward declaration

	/*---- Fields ----*/

	private: Node* root;  // Not nullptr
	private: std::size_t count;

	private: const std::uint32_t minKeys;  // At least 1, equal to degree-1
	private: const std::uint32_t maxKeys;  // At least 3, odd number, equal to minKeys*2+1



	/*---- Constructors ----*/

	// The degree is the minimum number of children each non-root internal node must have.
	public: explicit BTreeSet(std::uint32_t degree) :
			minKeys(degree - 1),
			maxKeys(degree <= UINT32_MAX / 2 ? degree * 2 - 1 : 0) {  // Avoid overflow
		if (degree < 2)
			throw std::domain_error("Degree must be at least 2");
		if (degree > UINT32_MAX / 2)  // In other words, need maxChildren <= UINT32_MAX
			throw std::domain_error("Degree too large");
		clear();
	}



	/*---- Methods ----*/

	public: bool empty() const {
		return count == 0;
	}


	public: std::size_t size() const {
		return count;
	}


	public: void clear() {
		nodepool.clear();
		root = nodepool.create_new_node(maxKeys, true);
		count = 0;
	}


	using SearchResult = std::pair<bool,std::uint32_t>;

	public: bool contains(char* &val) const {
		// Walk down the tree
		const Node *node = root;
		while (true) {
			SearchResult sr = node->search(val);
			if (sr.first)
				return true;
			else if (node->isLeaf())
				return false;
			else  // Internal node
				node = node->children.at(sr.second);
		}
	}



	public: void insert(const char* val, uint8_t length) {
		// Special preprocessing to split root node
		if (root->keys.size() == maxKeys) {
			Node* child = root;
			root = nodepool.create_new_node(maxKeys, false); // std::unique_ptr<Node>(new Node(maxKeys, false)); // Increment tree height
			root->children.push_back(child);
			root->splitChild(minKeys, maxKeys, 0);
		}

		// Walk down the tree
		Node *node = root;
		while (true) {
			// Search for index in current node
			assert(node->keys.size() < maxKeys);
			assert(node == root || node->keys.size() >= minKeys);
			SearchResult sr = node->search(val, length);
			if (sr.first)
				return;  // Key already exists in tree
			std::uint32_t index = sr.second;

			if (node->isLeaf()) {  // Simple insertion into leaf
				if (count == SIZE_MAX)
					throw std::length_error("Maximum size reached");
				char *offset = node->value;
				node->keys.insert(node->keys.begin() + index, val);
				count++;
				return;  // Successfully inserted

			} else {  // Handle internal node
				Node *child = node->children.at(index);
				if (child->keys.size() == maxKeys) {  // Split child node
					node->splitChild(minKeys, maxKeys, index);
					char* middleKey = node->keys.at(index);
					if (val == middleKey)
						return;  // Key already exists in tree
					else if (val > middleKey)
						child = node->children.at(index + 1);
				}
				node = child;
			}
		}
	}


	public: std::size_t erase(char* &val) {
		// Walk down the tree
		bool found;
		std::uint32_t index;
		{
			SearchResult sr = root->search(val);
			found = sr.first;
			index = sr.second;
		}
		Node *node = root;
		while (true) {
			assert(node->keys.size() <= maxKeys);
			assert(node == root || node->keys.size() > minKeys);
			if (node->isLeaf()) {
				if (found) {  // Simple removal from leaf
					node->removeKey(index);
					assert(count > 0);
					count--;
					return 1;
				} else
					return 0;

			} else {  // Internal node
				if (found) {  // Key is stored at current node
					Node *left  = node->children.at(index + 0);
					Node *right = node->children.at(index + 1);
					assert(left != nullptr && right != nullptr);
					if (left->keys.size() > minKeys) {  // Replace key with predecessor
						node->keys.at(index) = left->removeMax(minKeys);
						assert(count > 0);
						count--;
						return 1;
					} else if (right->keys.size() > minKeys) {  // Replace key with successor
						node->keys.at(index) = right->removeMin(minKeys);
						assert(count > 0);
						count--;
						return 1;
					} else {  // Merge key and right node into left node, then recurse
						node->mergeChildren(minKeys, index);
						if (node == root && root->keys.empty()) {
							assert(root->children.size() == 1);
							Node *newRoot = root->children.at(0);
							root = newRoot;  // Decrement tree height
						}
						node = left;
						index = minKeys;  // Index known due to merging; no need to search
					}

				} else {  // Key might be found in some child
					Node *child = node->ensureChildRemove(minKeys, index);
					if (node == root && root->keys.empty()) {
						assert(root->children.size() == 1);
						Node* newRoot = root->children.at(0);
						root = newRoot;  // Decrement tree height
					}
					node = child;
					SearchResult sr = node->search(val);
					found = sr.first;
					index = sr.second;
				}
			}
		}
	}


	// For unit tests
	public: void checkStructure() const {
		// Check size and root node properties
		if (root == nullptr || (count > maxKeys && root->isLeaf())
				|| (count <= minKeys * 2 && (!root->isLeaf() || root->keys.size() != count)))
			throw std::logic_error("Invalid size or root type");

		// Calculate height by descending into one branch
		int height = 0;
		for (const Node *node = root; !node->isLeaf(); node = node->children.at(0)) {
			if (height == INT_MAX)
				throw std::logic_error("Integer overflow");
			height++;
		}

		// Check all nodes and total size
		if (root->checkStructure(minKeys, maxKeys, true, height, nullptr, nullptr) != count)
			throw std::logic_error("Size mismatch");
	}



	/*---- Helper class: B-tree node ----*/

	private: class Node final {

		inline kv_value *create_skp_value(int valuesize = 28*1024) {
			kv_value *k = new kv_value();
			k->length = valuesize;
			k->value  = malloc(k->length);
			k->offset = 0;
			return k;
		}

		/*-- Fields --*/

		// Size is in the range [0, maxKeys] for root node, [minKeys, maxKeys] for all other nodes.
		public: std::vector<char*> keys;

		// If leaf then size is 0, otherwise if internal node then size always equals keys.size()+1.
		public: std::vector< Node* > children;

		int id;
		kv_value *value;
		/*-- Constructor --*/

		// Note: Once created, a node's structure never changes between a leaf and internal node.
		public: Node(int id_, std::uint32_t maxKeys, bool leaf): id(id_) {
			assert(maxKeys >= 3 && maxKeys % 2 == 1);
			keys.reserve(maxKeys);
			if (!leaf)
				children.reserve(maxKeys + 1);
			value = create_skp_value();
		}

		public: Node(int id_, std::uint32_t maxKeys, bool leaf): id(id_) {
			assert(maxKeys >= 3 && maxKeys % 2 == 1);
			keys.reserve(maxKeys);
			if (!leaf)
				children.reserve(maxKeys + 1);
			value = create_skp_value();
		}




		/*-- Methods for getting info --*/

		public: bool isLeaf() const {
			return children.empty();
		}


		// Searches this node's keys vector and returns (true, i) if obj equals keys[i],
		// otherwise returns (false, i) if children[i] should be explored. For simplicity,
		// the implementation uses linear search. It's possible to replace it with binary search for speed.
		public: SearchResult search(char* &val) const {
			std::uint32_t i = 0;
			while (i < keys.size()) {
				const char* elem = keys.at(i);
				if (val == elem) {
					assert(i < keys.size());
					return SearchResult(true, i);  // Key found
				} else if (val > elem)
					i++;
				else  // val < elem
					break;
			}
			assert(i <= keys.size());
			return SearchResult(false, i);  // Not found, caller should recurse on child
		}


		/*-- Methods for insertion --*/

		// For the child node at the given index, this moves the right half of keys and children to a new node,
		// and adds the middle key and new child to this node. The left half of child's data is not moved.
		public: void splitChild(std::size_t minKeys, std::size_t maxKeys, std::size_t index) {
			assert(!this->isLeaf() && index <= this->keys.size() && this->keys.size() < maxKeys);
			Node *left = this->children.at(index);
			assert(left->keys.size() == maxKeys);
			this->children.insert(this->children.begin() + index + 1, nodepool.create_new_node(maxKeys, left->isLeaf()));
			Node *right = this->children.at(index + 1);

			// Handle children
			if (!left->isLeaf()) {
				std::move(left->children.begin() + minKeys + 1, left->children.end(), std::back_inserter(right->children));
				left->children.erase(left->children.begin() + minKeys + 1, left->children.end());
			}

			// Handle keys
			this->keys.insert(this->keys.begin() + index, std::move(left->keys.at(minKeys)));
			std::move(left->keys.begin() + minKeys + 1, left->keys.end(), std::back_inserter(right->keys));
			left->keys.erase(left->keys.begin() + minKeys, left->keys.end());
		}


		/*-- Methods for removal --*/

		// Performs modifications to ensure that this node's child at the given index has at least
		// minKeys+1 keys in preparation for a single removal. The child may gain a key and subchild
		// from its sibling, or it may be merged with a sibling, or nothing needs to be done.
		// A reference to the appropriate child is returned, which is helpful if the old child no longer exists.
		public: Node *ensureChildRemove(std::size_t minKeys, std::uint32_t index) {
			// Preliminaries
			assert(!this->isLeaf() && index < this->children.size());
			Node *child = this->children.at(index);
			if (child->keys.size() > minKeys)  // Already satisfies the condition
				return child;
			assert(child->keys.size() == minKeys);

			// Get siblings
			Node *left = index >= 1 ? this->children.at(index - 1) : nullptr;
			Node *right = index < this->keys.size() ? this->children.at(index + 1) : nullptr;
			bool internal = !child->isLeaf();
			assert(left != nullptr || right != nullptr);  // At least one sibling exists because degree >= 2
			assert(left  == nullptr || left ->isLeaf() != internal);  // Sibling must be same type (internal/leaf) as child
			assert(right == nullptr || right->isLeaf() != internal);  // Sibling must be same type (internal/leaf) as child

			if (left != nullptr && left->keys.size() > minKeys) {  // Steal rightmost item from left sibling
				if (internal) {
					child->children.insert(child->children.begin(), left->children.back());
					left->children.pop_back();
				}
				child->keys.insert(child->keys.begin(), this->keys.at(index - 1));
				this->keys.at(index - 1) = left->removeKey(left->keys.size() - 1);
				return child;
			} else if (right != nullptr && right->keys.size() > minKeys) {  // Steal leftmost item from right sibling
				if (internal) {
					child->children.push_back(right->children.front());
					right->children.erase(right->children.begin());
				}
				child->keys.push_back(this->keys.at(index));
				this->keys.at(index) = right->removeKey(0);
				return child;
			} else if (left != nullptr) {  // Merge child into left sibling
				this->mergeChildren(minKeys, index - 1);
				return left;  // This is the only case where the return value is different
			} else if (right != nullptr) {  // Merge right sibling into child
				this->mergeChildren(minKeys, index);
				return child;
			} else
				throw std::logic_error("Impossible condition");
		}


		// Merges the child node at index+1 into the child node at index,
		// assuming the current node is not empty and both children have minKeys.
		public: void mergeChildren(std::size_t minKeys, std::uint32_t index) {
			assert(!this->isLeaf() && index < this->keys.size());
			Node &left  = *children.at(index + 0);
			Node &right = *children.at(index + 1);
			assert(left.keys.size() == minKeys && right.keys.size() == minKeys);
			if (!left.isLeaf())
				std::move(right.children.begin(), right.children.end(), std::back_inserter(left.children));
			left.keys.push_back(removeKey(index));
			std::move(right.keys.begin(), right.keys.end(), std::back_inserter(left.keys));
			children.erase(children.begin() + index + 1);
		}


		// Removes and returns the minimum key among the whole subtree rooted at this node.
		// Requires this node to be preprocessed to have at least minKeys+1 keys.
		public: char* removeMin(std::size_t minKeys) {
			for (Node *node = this; ; ) {
				assert(node->keys.size() > minKeys);
				if (node->isLeaf())
					return node->removeKey(0);
				else
					node = node->ensureChildRemove(minKeys, 0);
			}
			assert(0);
			return 0;
		}


		// Removes and returns the maximum key among the whole subtree rooted at this node.
		// Requires this node to be preprocessed to have at least minKeys+1 keys.
		public: char* removeMax(std::size_t minKeys) {
			for (Node *node = this; ; ) {
				assert(node->keys.size() > minKeys);
				if (node->isLeaf())
					return node->removeKey(node->keys.size() - 1);
				else
					node = node->ensureChildRemove(minKeys, node->children.size() - 1);
			}
			assert(0);
			return 0;
		}


		// Removes and returns this node's key at the given index.
		public: char* removeKey(std::uint32_t index) {
			char* result = keys.at(index);
			keys.erase(keys.begin() + index);
			return result;
		}


		/*-- Miscellaneous methods --*/

		// Checks the structure recursively and returns the total number
		// of keys in the subtree rooted at this node. For unit tests.
		public: std::size_t checkStructure(std::size_t minKeys, std::size_t maxKeys, bool isRoot, int leafDepth, char* *min, char* *max) {
			// Check basic fields
			const std::size_t numKeys = keys.size();
			if (isLeaf() != (leafDepth == 0))
				throw std::logic_error("Incorrect leaf/internal node type");
			if (numKeys > maxKeys)
				throw std::logic_error("Invalid number of keys");
			if (isRoot && !isLeaf() && numKeys == 0)
				throw std::logic_error("Invalid number of keys");
			if (!isRoot && numKeys < minKeys)
				throw std::logic_error("Invalid number of keys");

			// Check keys for strict increasing order
			for (std::size_t i = 0; i < numKeys; i++) {
				char* &key = keys.at(i);
				bool fail = i == 0 && min != nullptr && key <= *min;
				fail |= i >= 1 && key <= keys.at(i - 1);
				fail |= i == numKeys - 1 && max != nullptr && key >= *max;
				if (fail)
					throw std::logic_error("Invalid key ordering");
			}

			// Check children recursively and count keys in this subtree
			std::size_t count = numKeys;
			if (!isLeaf()) {
				if (children.size() != numKeys + 1)
					throw std::logic_error("Invalid number of children");
				// Check children pointers and recurse
				for (std::size_t i = 0; i < children.size(); i++) {
					std::size_t temp = children.at(i)->checkStructure(
						minKeys, maxKeys, false, leafDepth - 1,
						(i == 0 ? min : &keys.at(i - 1)), (i == numKeys ? max : &keys.at(i)));
					if (SIZE_MAX - temp < count)
						throw std::logic_error("Size overflow");
					count += temp;
				}
			}
			return count;
		}

	};

	class NodePool final {
		int seq;
		std::map<int, std::unique_ptr<Node> > pool;
	public:
		NodePool (): seq(0) {}

		Node* create_new_node(uint32_t maxkeys, bool isleaf) {
			int id = seq++;
			std::unique_ptr<Node> ptr (new Node(id, maxkeys, isleaf));
			Node *n = ptr.get();
			pool.insert(std::make_pair(id, std::move(ptr) ));
			return n;
		}

		void clear() {
			pool.clear();
		}
	};

	static NodePool nodepool;

};



int btree_test();



#endif /* SRC_KADI_KADI_BTREE_H_ */

