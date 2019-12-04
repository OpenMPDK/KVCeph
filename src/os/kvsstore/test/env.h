/*
 * env.h
 *
 *  Created on: Nov 2, 2019
 *      Author: yangwook.k
 */

#ifndef SRC_ENV_H_
#define SRC_ENV_H_

#include <math.h>
#include <iostream>
#include <thread>
#include <atomic>
#include <unistd.h>
#include "kadi/kadi_cmds.h"
#include "kadi/kadi_helpers.h"
using namespace std;


class KVCephEnv {

public:
	KADI kadi;

	std::thread *poll_thread;
	std::atomic_bool poll_stoprequested = {false};
	std::atomic<uint64_t> num_written_keys;

	KVCephEnv(): kadi(0), poll_thread(0), num_written_keys(0) {
		int opened = kadi.open("/dev/nvme0n1", 0);
		if (opened != 0) { std::cout << "cannot open the device" << std::endl; exit(1); }

		poll_thread = new std::thread(poll_completion, this);
	}

	void io_complete(KADI *adi)
	{
		std::cout << ">> poll_completion thread started" << std::endl;
		while (!poll_stoprequested) {
			uint32_t num_events = 10;
			adi->poll_completion(num_events, 1000000);

		}
		std::cout << ">> poll_completion thread finished" << std::endl;
	}

	void join() {
		poll_stoprequested = true;
		if (poll_thread) poll_thread->join();
	}

	///
	/// write test keys
	///


	inline void set_kv_key(kv_key *key, uint32_t prefix, int bucket, int id) {
		char buf[16];
		sprintf(buf, "%.3d+%.4d", bucket, id);
		memcpy((char*)key->key, &prefix, sizeof(uint32_t));
		memcpy((char*)key->key + sizeof(uint32_t), &buf,8);
		key->length = 12;
	}

	inline void set_kv_value(kv_value *value, int id) {
		char buf[5];
		sprintf(buf, "%.4d", id);
		memcpy((char*)value->value, &buf,4);
		value->length = 4;
	}


	inline void print_kv_key(void *key) {
		cout << "key: prefix " << *(uint32_t*)key << " bucket+id ";
		for (int i =4 ; i < 12; i++)
			cout << *((char*)key + i);
		cout << endl;
	}
	inline void print_kv_value(void *value) {
		cout << "value: ";
		for (int i =0 ; i < 4; i++)
			cout << *((char*)value + i);
		cout << endl;
	}

	static void ceph_write_callback(kv_io_context &op, void* private_data) {

		cout << "written: " << print_key((const char*)op.key.key, op.key.length) << ", len = " << (int)op.key.length << ", op.ret = " << op.retcode << endl;

		((KVCephEnv*)private_data)->num_written_keys.fetch_add(1, std::memory_order_relaxed);
	}

	int write_test_keys(int bucket, uint32_t  prefix, uint32_t num_keys_per_loop) {
		int count = 0;
		kv_key     *key = KVMemPool::Alloc_key(144);
		kv_value *value = KVMemPool::Alloc_value(16);

		for (unsigned j =0; j < num_keys_per_loop; j++) {
			set_kv_key(key, prefix, bucket, count);
			set_kv_value(value, count);

			int ret = kadi.kv_store_aio(bucket, key, value, { ceph_write_callback, this } );
			if (ret != 0) { cout << "write failed " << endl; return count; }
			count++;
		}

		KVMemPool::Release_key(key);
		KVMemPool::Release_value(value);

		while(num_keys_per_loop > num_written_keys.load(std::memory_order_relaxed)) {
			usleep(1);
		}

		std::cout << ">> wrote " << num_keys_per_loop << " key-value pairs to bucket " << bucket << std::endl;

		return num_keys_per_loop;
	}



	KADI *get_adi() { return &kadi; }

	static void poll_completion(KVCephEnv *env) { env->io_complete(&env->kadi); 	}
};




#endif /* SRC_ENV_H_ */
