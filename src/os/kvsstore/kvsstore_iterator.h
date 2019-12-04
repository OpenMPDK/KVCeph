/*
 * kvsstore_iterator_impl.h
 *
 *  Created on: Nov 20, 2019
 *      Author: root
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_ITERATOR_IMPL_H_
#define SRC_OS_KVSSTORE_KVSSTORE_ITERATOR_IMPL_H_

#include "indexer_hint.h"
#include "kvsstore_types.h"
#include "kvio/kadi/kadi_bptree.h"

class KvsBptreeIterator: public KvsIterator {
	bptree tree;
	bptree_iterator *iter;

public:
	KvsBptreeIterator(KADI *adi, int ksid_skp, uint32_t prefix);

	int begin() override;
	int end() override;
	int upper_bound(const kv_key &key) override;
	int lower_bound(const kv_key &key) override;

	bool valid() override;

	int next() override;
	int prev() override;

	kv_key key() override;
};
#endif /* SRC_OS_KVSSTORE_KVSSTORE_ITERATOR_IMPL_H_ */
