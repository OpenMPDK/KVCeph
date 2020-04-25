/*
 * test.h
 *
 *  Created on: Nov 9, 2019
 *      Author: root
 */

#ifndef SRC_TEST_IMPL_H_
#define SRC_TEST_IMPL_H_

std::default_random_engine randGen((std::random_device())());

#define assert_equals(expected, actual) if (expected != actual) { cerr << "[" << __FILE__ << ":" << __LINE__ << "] Assert Failure: expected " << expected << " but was " << actual << endl; }

void load_oplog(KVCephEnv &env, int spaceid, uint32_t prefix, uint32_t searchprefix ) {
	env.write_test_keys(spaceid, prefix, 10);
	uint64_t processed_keys = env.get_adi()->list_oplog(spaceid, searchprefix, [&] (int groupid, uint64_t sequence, const char* key, int length){
			cout << "read: group"  << groupid << ", seq " << sequence << ", " << print_key((const char*)key, length) << ", length = " << length << endl;
	});
	cout << " processed keys = " << processed_keys << endl;
}

void create_test_keys(int numkeys, const uint32_t PREFIX, std::vector<std::string> &keys) {
	char buf[256];
	const int RANGE = 10000;
	keys.reserve(numkeys);

	uniform_int_distribution<int> valueDist(0, RANGE - 1);
	for (int i =0 ;i < numkeys; i++) {
		memcpy(buf, &PREFIX, 4);
		sprintf(buf+4, "%012d", valueDist(randGen));
		keys.push_back(buf);
	}
}

void test_bitmap(bptree_data *node3, int *keys, int num_keys, bool verbose = false) {
	uniform_int_distribution<int> keyselector(0, num_keys - 1);

	char *key = (char*)malloc(256);

	std::vector<bp_addr_t> addrs;

	while (true) {
		int keylength = keys[keyselector(randGen)];
		bp_addr_t keyaddr = node3->write_key(key, keylength);
		if (keyaddr == invalid_key_addr) {
			break;
		}
		addrs.push_back(keyaddr);
		if (verbose) {
			cout << "wrote one entry : length = " << keylength << endl;
			node3->freemgr.print_freespace();
		}
	}

	uint32_t expected = addrs.size();

	if (verbose) cout << "number of keys added = " << expected << endl;

	for (const auto &addr : addrs) {
		node3->delete_key(addr);
		if (verbose) {
			cout << "delete one entry" << endl;
			node3->freemgr.print_freespace();
		}
	}

	if (verbose) cout << "deleted all " << endl;
	if (verbose) node3->freemgr.print_freespace();


}

void test_bitmap(bptree_data *node3, int keylength, bool verbose = false) {
	std::vector<bp_addr_t> addrs;
	char *key = (char*)malloc(keylength);

	while (true) {
		bp_addr_t keyaddr = node3->write_key(key, keylength );
		if (keyaddr == invalid_key_addr) {
			break;
		}
		addrs.push_back(keyaddr);
		if (verbose) {
			cout << "wrote one entry : length = " << keylength << endl;
			node3->freemgr.print_freespace();
		}
	}

	uint32_t expected = addrs.size();

	if (verbose) cout << "number of keys added = " << expected << endl;

	for (const auto &addr : addrs) {
		node3->delete_key(addr);
		if (verbose) {
			cout << "delete one entry : length = " << keylength << endl;
			node3->freemgr.print_freespace();
		}
	}

	if (verbose) cout << "deleted all " << endl;
	if (verbose) node3->freemgr.print_freespace();


	if (verbose) cout << "write all again" << endl;
	addrs.clear();
	while (true) {
		bp_addr_t keyaddr = node3->write_key(key, keylength );
		if (keyaddr == invalid_key_addr) {
			break;
		}
		addrs.push_back(keyaddr);
	}

	if (expected != addrs.size()) {
		cout << "2: number of keys added = " << addrs.size() << ", expected = " << expected << endl;
		exit(1);
	}

	for (const auto &addr : addrs) {
		node3->delete_key(addr);
	}
	if (verbose) node3->freemgr.print_freespace();

}


void test_datanode(KADI *adi) {

	const int num_testkeys = 10;
	std::vector<std::string> keys;
	const uint32_t PREFIX = 0xfeedbeef;

	//const int SPACEID = 1;
	//load_oplog(env, SPACEID, PREFIX, 0xffffffff);

	create_test_keys(num_testkeys, PREFIX, keys);

	int ksid_skp = 6;

	bptree tree(adi, ksid_skp, PREFIX);

	// create or delete pages

	bptree_node *node1 = tree.pool.create_tree_node(true);
	bptree_node *node2 = tree.pool.fetch_tree_node(node1->addr);

	if (node1 != node2) {
		printf("node mismatch %p != %p \n", node1, node2); exit(1);
	}

	bptree_data *node3 = tree.pool.create_data_node();

	test_bitmap(node3, 63);  // 1 fragment
	test_bitmap(node3, 64);	 // 2 fragments
	test_bitmap(node3, 127); // 2 fragments
	test_bitmap(node3, 128); // 3 fragments
	test_bitmap(node3, 255); // 4 fragments
	test_bitmap(node3, new int[5] {63, 64, 127, 128, 255}, 5);	// random

	/*for (std::string &s : keys) {
		cout << print_key(s.c_str(), s.length()) << ", len = " << s.length()<< endl;
	}*/
	std::cout << ">> " << __func__ << ": passed" << std::endl;
}




#endif /* SRC_TEST_IMPL_H_ */
