/*
 * kadi_sort.h
 *
 *  Created on: Sep 11, 2019
 *      Author: root
 */

#ifndef SRC_KADI_KADI_SORT_H_
#define SRC_KADI_KADI_SORT_H_
#include "kadi_types.h"
#include <mutex>
#include <condition_variable>

class KADI;
struct oplog_info;

class SortedKeyPage {
	KADI *kadi;
	int ksid_skp;
	uint32_t prefix;
	int id;
	kv_key   *skp_key;
	kv_value *skp_value;

	bool io_done;
	int  io_ret;
	std::mutex load_mutex;
	std::condition_variable load_cond;
	typedef std::vector<char*> ssl_type;

	class SortedData {

		ssl_type keylist;

	public:
		uint16_t buffer_offset;
		bool finalized;
		bool dirty;
		bool isleaf;
		int num_invalid_keys = 0;
		kv_value *value;
		void dump();
		int load_index(bool is_new, kv_value *value);
		int insert(const char *key, uint8_t length);
		int find_insert_pos(ssl_type &arr, const char *key, uint8_t length, bool &newentry);
		bool local_compact();
		void finalize();
		int remove(const char *key, uint8_t length);

		inline bool isfull(int length) {
			// footer - isleaf,  last offset, # of invalid keys, # of valid keys,
			return (buffer_offset + (length + 1) /* entry size */ + (keylist.size() + 4) * sizeof(uint16_t) ) > value->length;
		}


		inline void get_item(int pos, char **key, uint8_t &keylength) {
			char *buf = keylist[pos];
		    keylength = *(uint8_t*)buf;
		    *key = buf+sizeof(uint8_t);
		}

		inline uint32_t _write_key_to_buffer(const char *key, uint8_t length) {
			const uint32_t startoffset = buffer_offset;
			char *buffer = (char*)value->value;
			memcpy(buffer + buffer_offset, &length,  1); buffer_offset +=1;
			memcpy(buffer + buffer_offset, key, length); buffer_offset +=length;
			return startoffset;
		}

		char *min_key() { return 0; }
	};

	class SortedDirectory {
		kv_value *value;
		ssl_type keylist;
		std::vector<int> idlist;
	public:
		int load_index(kv_value *value);
		int add_entry(char *entry, kv_key *skp_key);
		SortedData *get_data_page(const char *key, uint8_t length);
	};



public:
	SortedDirectory *directory;
	std::vector<SortedData*> data;
	SortedKeyPage():kadi(0), ksid_skp(0), prefix(0), id(-1), skp_key(0), skp_value(0), io_done(false), io_ret(-1), directory(0) {}
	int init(KADI *adi, int ksid_skp, uint32_t prefix, int id);
	int wait_for_directory_page();
	void io_finished(int retcode);
	void load_directory(bool isnew, kv_key *key, kv_value *value);
	int load_skp_sync(int id);

	int remove(const char *key, uint8_t length);
	int insert(const char *key, uint8_t length);
};


#endif /* SRC_KADI_KADI_SORT_H_ */
