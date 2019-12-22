#ifndef SRC_OS_KVSSTORE_KVSSTORE_KVCMDS_H_
#define SRC_OS_KVSSTORE_KVSSTORE_KVCMDS_H_

#include <map>

#include "kvsstore_types.h"
#include "kvio/kvio.h"


class KvsStoreDB
{
	CephContext *cct;
	KADIWrapper kadi;
	std::mutex compact_lock;
	std::condition_variable compact_cond;
	static const int skip_skp = 6;
public:
	KvsStoreDB(CephContext *cct_): cct(cct_), kadi(cct) {}

	KADIWrapper *get_adi() { return &kadi; }
	inline int open(const std::string &devpath) {return kadi.open(devpath, 0);	}
	inline int close() { return kadi.close(); }
	inline int close_iterators() { return kadi.close_iterators(); }
	inline bool is_opened() { return kadi.is_opened(); }

	inline int poll_completion(uint32_t &num_events, uint32_t timeout_us) {	return kadi.poll_completion(num_events, timeout_us); }

	//const std::function< void (int, int, uint64_t, const char*, int) >
	template<typename Func>
	inline uint64_t list_oplog(const uint8_t spaceid, const uint32_t prefix, Func &&key_listener) {
		return kadi.adi.list_oplog(spaceid, prefix, key_listener);
	}

	bool rm_journal(const int index);
	bool read_journal(int index, kv_value *value);
	uint64_t compact();
	int read_sb(bufferlist &bl);
	int read_onode(const ghobject_t &oid, bufferlist &bl);
	int read_data(const ghobject_t &oid, bufferlist &bl, bool&ispartial);
	int read_block(const ghobject_t &oid, const int blockindex, char *data, uint32_t &nread);
	int read_block(const ghobject_t &oid, const int blockindex, bufferlist &bl, uint32_t &nread);
	int read_data(const ghobject_t &oid, int offset, int length, bufferlist &bl, bool&ispartial);
	int read_coll(const char *name, const int namelen, bufferlist &bl);
	int read_omap(const ghobject_t& oid, const uint64_t index, const std::string &strkey, bufferlist &bl);
    int read_kvkey(kv_key *key, bufferlist &bl);
	int write_sb(bufferlist &bl);

    void add_coll(KvsIoContext *ctx, const coll_t &cid, bufferlist &bl);
    void add_onode(KvsIoContext *ctx,const ghobject_t &oid, bufferlist &bl);
    void add_userdata(KvsIoContext *ctx,const ghobject_t& oid, char *page, int length, int pageid);
    void add_userdata(KvsIoContext *ctx,const ghobject_t& oid, bufferlist &bl, int pageid);
    void add_omap(KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, const std::string &strkey, bufferlist &bl);

    void rm_coll(KvsIoContext *ctx,const coll_t &cid);
    void rm_onode(KvsIoContext *ctx,const ghobject_t& oid);
    void rm_data(KvsIoContext *ctx,const ghobject_t& oid, int pageid);
    void rm_omap(KvsIoContext *ctx,const ghobject_t& oid, uint64_t index, const std::string &strkey);

    int _read_impl(uint8_t space_id, int offset, int length, bufferlist &bl, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill, bool &ispartial, bool retry);
	int _read_impl(uint8_t space_id, bufferlist &bl, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill, bool &ispartial, bool retry = false);
	int _write_impl(uint8_t space_id, char *data, int len, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill);
	int _async_write_impl(uint8_t space_id, char *data, int len, const std::function< void (struct nvme_passthru_kv_cmd&)> &fill, const kv_cb &cb);

	inline int get_freespace(uint64_t &bytesused, uint64_t &capacity, double &utilization) {
		return kadi.get_freespace(bytesused, capacity, utilization);
	}

	int aio_submit(KvsTransContext *txc);
	int write_journal(KvsTransContext *txc);
	KvsIterator *get_iterator(uint32_t prefix);

	// >=
	inline bool is_key_ge(const kv_key &key1, const kv_key &key2) {
		char *k1 = (char *)key1.key;
		char *k2 = (char *)key2.key;
		const int k1_length = key1.length;
		const int k2_length = key2.length;
		return (kvkey_lex_compare(k1, k1+k1_length, k2, k2+k2_length) >= 0);
	}

private:
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

};


#endif /* SRC_OS_KVSSTORE_KVSSTORE_KVCMDS_H_ */
