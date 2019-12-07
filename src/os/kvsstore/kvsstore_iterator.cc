#include "kvsstore_iterator.h"

KvsBptreeIterator::KvsBptreeIterator(KADI *adi, int ksid_skp, uint32_t prefix):
	tree(adi,ksid_skp, prefix)
{
	iter = tree.get_iterator();
}

int KvsBptreeIterator::begin() {
	iter->begin();
	return 0;
}
int KvsBptreeIterator::end() {
	iter->end();
	return 0;
}

int KvsBptreeIterator::upper_bound(const kv_key &key) {
	iter->upper_bound((char*)key.key, key.length);
	return 0;

}
int KvsBptreeIterator::lower_bound(const kv_key &key) {
	iter->lower_bound((char*)key.key, key.length);
	return 0;
}

bool KvsBptreeIterator::valid() {
	return !iter->is_end();
}

int KvsBptreeIterator::next() {
	iter->move_next(1);
	return 0;
}
int KvsBptreeIterator::prev() {
	iter->move_next(-1);
	return 0;
}

kv_key KvsBptreeIterator::key() {
	char *key = 0;
	int   len = 0;
	iter->get_key((char**)&key, len);
    TR << print_kvssd_key(key, len) << TREND;
	return { key, (uint8_t)len };
}

