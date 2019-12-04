#include <set>
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <random>
#include <stdexcept>
#include <set>

#include "kadi/kadi_bptree.h"
#include "env.h"

#include "test_impl.h"

void generate_treenode_test_keys(uint32_t PREFIX, std::vector<std::tuple<int, char*, int>> &keys)
{
	// 10 sequential keys
	int numkeys = 10;
	int numdeletedkeys = 0;

	for (int i = 0 ; i < numkeys; i ++) {
		char *key = (char*)malloc(16);
		memcpy(key, &PREFIX, 4);
		sprintf(key+4, "%012d", i);
		printf("INSERT: key %d\n", i);
		keys.push_back(make_tuple(0, key, 16));
	}

	//// random delete
	//uniform_int_distribution<int> key(0, numkeys - 1);
	for (int i =0; i < numdeletedkeys; i++) {
		char *key = (char*)malloc(16);
		memcpy(key, &PREFIX, 4);
		sprintf(key+4, "%012d", i);
		printf("REMOVE: key %d\n", i);
		keys.push_back(make_tuple(1, key, 16));
	}

	// variable length key insert
	//uniform_int_distribution<int> keyDist(0, 10000000UL - 1);
}

void test_binarysearch(KADI *kadi) {
	const static int ksid_skp = 6;
	const uint32_t PREFIX = 0xdeadbeef;
	bptree tree(kadi, ksid_skp, PREFIX);

	std::vector<std::tuple<int, char*, int>> keys;
	generate_treenode_test_keys(PREFIX, keys);

	// test binary search
	bptree_data *node3 = tree.pool.create_data_node();
	auto a = node3->write_key("hello", 5);
	auto b = node3->write_key("hello1", 6);
	auto c = node3->write_key("hella", 5);
	auto d = node3->write_key("hello", 5);
	auto e = node3->write_key("hello2", 6);
	bool result;

	result = tree.is_larger(tree.pool.fetch_keyblock(b),
							tree.pool.fetch_keyblock(e));
	assert_equals(false, result);

	result = tree.is_larger(tree.pool.fetch_keyblock(a),
							tree.pool.fetch_keyblock(b));
	assert_equals(false, result);
	result = tree.is_larger(tree.pool.fetch_keyblock(a),
							tree.pool.fetch_keyblock(c));
	assert_equals(true, result);
	result = tree.is_larger(tree.pool.fetch_keyblock(b),
							tree.pool.fetch_keyblock(c));
	assert_equals(true, result);
	result = tree.is_larger(tree.pool.fetch_keyblock(a),
							tree.pool.fetch_keyblock(d));
	assert_equals(false, result);

	std::cout << ">> test_binarysearch: passed\n";
}

void test_treenode(KADI *kadi) {
	const static int ksid_skp = 6;
	const uint32_t PREFIX = 0xdeadbeef;
	bptree tree(kadi, ksid_skp, PREFIX);

	std::vector<std::tuple<int, char*, int>> keys;
	generate_treenode_test_keys(PREFIX, keys);
	try {
		int op, l, ret;
		char *k;

		for (const auto &t : keys) {
			std::cout << "!" << std::endl;
			std::tie(op, k, l) = t;
			if (op == 0) {
				std::cout << "------------------INSERT -----------------\n";
				ret = tree.insert(k, l);
			} else if (op == 1) {
				std::cout << "------------------DELETE -----------------\n";
				ret = tree.remove(k, l);
			}

			if (ret != 0) {
				std::cout << "failed: op " << op << ", k " << k << ", l " << l
						<< std::endl;
				exit(1);
			}
		}

		auto it = tree.get_iterator();
		it->begin();

		char *rkey;
		int rkey_length;
		while (!it->is_end()) {
			printf("fetching..\n");
			it->get_key(&rkey, rkey_length);
			printf("returned key : %.*s\n", 12, rkey + 4);
			it->move_next();
		}
	} catch (const char *p) {
		std::cerr << p << std::endl;
		exit(1);
	}

	std::cout << ">> test_treenode: passed\n";
}

int main(int argc, char *argv[]) {
	KVCephEnv env;
	usleep(5000);

	test_treenode(&env.kadi);
	//test_datanode(&env.kadi);
	//test_binarysearch(&env.kadi);

	env.join();
	return 0;
}
