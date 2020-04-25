#if 0

static inline bool is_aligned(const int offset , int block ) {
	return (offset % block) == 0;
}


inline void get_page_ids(uint64_t offset, size_t length, std::vector<int> &list) {
	const uint64_t enddata_off = offset + length;
	const int start_block_id = offset / KVS_OBJECT_SPLIT_SIZE;
	const int last_block_id  = (enddata_off - 1) / KVS_OBJECT_SPLIT_SIZE + 1;
	const int num_blocks     = last_block_id - start_block_id +1;
	int i = start_block_id;
	while (i != last_block_id) {
		list.push(i);
		i++;
	}
}
static inline KvsDataBlock * lookup_or_create_datablock(unordered_map<int /* blockid */, KvsDataBlock*> &datamap, int blockid) {
	KvsDataBlock *block;
	auto it = datamap.find(blockid);
	if (it == datamap.end()) {
		block = new KvsDataBlock();
		// reserve block space for contiguous append
		block->data.reserve(KVS_OBJECT_SPLIT_SIZE);
		datamap.insert(it, std::pair<int, KvsDataBlock*>(blockid, block));
	}
	else {
		block = it->second;
	}
	return block;
}

	//
	// load first or last block if needed
	//
	unordered_map<int /* blockid */, KvsDataBlock*> &datamap = txc->databuffers[o->oid];
	KvsDataBlock *startblock = lookup_or_create_datablock(datamap, start_block_id);

	unsigned b_off = (offset - (start_block_id << KVS_OBJECT_SPLIT_SHIFT));
	unsigned b_len = std::min(length, (size_t)KVS_OBJECT_SPLIT_SIZE - b_off);

	if (b_off > startblock->pos || std::min((uint64_t)KVS_OBJECT_SPLIT_SIZE, o->onode.size) > (b_off + b_len) )
	{
		int r = db.read_block(o->oid, start_block_id, startblock->data, startblock->pos);
		if (r != 0 && r != 784 /* not found */) return r;	// I/O error
	}

	startblock->write_data(b_off, b_len, data + data_pos); data_pos+= b_len;

	if (num_blocks > 1) {
		//
		// middle blocks
		//
		for (int i = start_block_id+1 ; i <  last_block_id; i++) {
			KvsDataBlock *block = lookup_or_create_datablock(datamap, i);
			block->write_data(0, KVS_OBJECT_SPLIT_SIZE, data + data_pos); data_pos+= KVS_OBJECT_SPLIT_SIZE;
		}

		//
		// last block
		//

		// partial write: enddata_off < object size
		if (num_blocks > 1 && !is_aligned(enddata_off, KVS_OBJECT_SPLIT_SIZE) && o->onode.size > enddata_off) {
			KvsDataBlock *lastblock = lookup_or_create_datablock(datamap, last_block_id);
			int r = db.read_block(o->oid, last_block_id, lastblock->data, lastblock->pos);
			if (r != 0 && r != 784 /* not found */) return r;	// I/O error
		}

		KvsDataBlock *lastblock = lookup_or_create_datablock(datamap, last_block_id);
		b_len = enddata_off - (last_block_id << KVS_OBJECT_SPLIT_SHIFT);
		lastblock->write_data(0, b_len, data + data_pos); data_pos+= b_len;
	}

	txc->write_onode(o);

	{
		// invalidate the read cache
		KvsCollection *kc = static_cast<KvsCollection*>(c->get());
		kc->data_map.rm_data(o->oid);
	}

	dout(10) << __func__ << " " << c->cid << " " << o->oid << " 0x"
						<< std::hex << offset << "~" << length << std::dec
						<< " = " << r << dendl;

	struct iter_param temp;
	struct iter_param other;

	{
		std::shared_lock l(c->lock);
		r = _collection_list(c, start, end, max, ls, pnext);

		//r = _prep_collection_list(c, start, temp, other);
	}

	/*if (r == 1) {
		r = _load_and_search_collection_list(start, end, temp, other, max, ls,
				pnext);
	}*/


	int KvsStore::iterate_objects_in_device(uint64_t poolid, int8_t shardid,
			std::set<ghobject_t> &data, uint8_t space_id) {
		int ret;
		void *key;
		int length;
		kv_iter_context iter_ctx;

		std::list<std::pair<malloc_unique_ptr<char>, int> > buflist;

		const uint32_t prefix = get_object_group_id(GROUP_PREFIX_ONODE, shardid,
				poolid);

		iter_ctx.prefix = prefix;
		iter_ctx.bitmask = 0xFFFFFFFF;
		iter_ctx.buflen = ITER_BUFSIZE;

		long total_keys = 0;
		long all_keys = 0;
		double total_iterate_time = 0;
		double sort_time = 0;
		// read collections
		utime_t start_itertime = ceph_clock_now();

		// commented for batch driver fix
		ret = db.iter_readall_aio(&iter_ctx, buflist, (uint8_t) space_id);
		//remove after batch driver fix
		// ret = db.iter_readall(&iter_ctx, buflist, (uint8_t) space_id);
		utime_t end_itertime = ceph_clock_now();

		logger->tinc(l_kvsstore_iterate_latency, end_itertime - start_itertime);

		if (ret != 0) {
			ret = -1;
			goto out;
		}
		// parse the key buffers
		for (const auto &p : buflist) {

			iterbuf_reader reader(cct, p.first.get(), p.second);

			total_keys += reader.numkeys_ret();
			while (reader.nextkey(&key, &length)) {
				if (length > 255)
					break;

				kvs_onode_key *collkey = (kvs_onode_key*) key;

				// check for hash collisions
				if (collkey->group == GROUP_PREFIX_ONODE
						&& collkey->shardid == shardid
						&& collkey->poolid == poolid) {
					ghobject_t oid;
					construct_onode_ghobject_t(cct, (const char*) key, length,
							&oid);
					utime_t stime = ceph_clock_now();
					data.insert(oid);
					utime_t etime = ceph_clock_now();
					sort_time += (double) (etime - stime);
				}
			}
		}

		all_keys += total_keys;
		total_iterate_time += (double) (end_itertime - start_itertime);

		derr << __func__ << ", # of keys returned = " << total_keys
							<< ", IO time = "
							<< (double) (end_itertime - start_itertime) << dendl;
		out:

		derr << __func__ << " All keys = " << all_keys << " data.size = "
							<< data.size() << " total Iterate time = " << total_iterate_time
							<< " sort time = " << sort_time << dendl;
		return ret;
	}
#endif
