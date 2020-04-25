#include "kadi_btree.h"
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <random>
#include <set>

NodePool::NodePool():seq(1), adi(0), prefix(0), ksid_skp(6),
	temp(new SKNode(this, 0, prefix, malloc(SKNode::DEFAULT_SKNODE_BUFFER_SIZE), SKNode::DEFAULT_SKNODE_BUFFER_SIZE)) {}

void NodePool::replace(uint32_t id, SKNode *node) {
	auto obj_a = lookup_erase(id);
	auto obj_b = lookup_erase(node->id);
	obj_b->id = id;
	pool.insert(std::make_pair(obj_b->id, std::move(obj_b)));
}

void NodePool::swap_id(uint32_t a, uint32_t b) {
	auto obj_a = lookup_erase(a);
	auto obj_b = lookup_erase(b);

	int temp  = obj_a->id;
	obj_a->id = obj_b->id;
	obj_b->id = temp;

	pool.insert(std::make_pair(obj_a->id, std::move(obj_a)));
	pool.insert(std::make_pair(obj_b->id, std::move(obj_b)));
}


void keyseq_callback(kv_io_context &op, void* private_data)
{
	NodePool::meta_io_context *ctx = (NodePool::meta_io_context *)private_data;
	std::lock_guard<std::mutex> lg(ctx->load_mutex);

	ctx->num_finished++;
	if (op.retcode == 0) {
		ctx->seq = *(uint64_t*)(ctx->value_seq.value);

		//std::cout << "last seq = " << ctx->seq <<std::endl;
	} else {
		ctx->seq = 1;
	}

	if (ctx->num_finished == ctx->total_ops) {
		ctx->done = true;
		ctx->load_cond.notify_one();
	}
};

void metactx_callback(kv_io_context &op, void* private_data)
{
	NodePool::meta_io_context *ctx = (NodePool::meta_io_context *)private_data;
	std::lock_guard<std::mutex> lg(ctx->load_mutex);

	ctx->num_finished++;

	if (ctx->num_finished == ctx->total_ops) {
		ctx->found = (op.retcode == 0);
		ctx->error = (!ctx->found && op.retcode != 784);
		ctx->done = true;
		ctx->load_cond.notify_one();
	}
};

SKNode* NodePool::wait_for_meta() {
	this->seq = metactx.wait();
	this->temp->parse();
	SKNode* root = this->temp.get();

	//std::cout << "init - new seq = " << this->seq  << std::endl;
	root->print_keys("root");
	root->print_children();
	pool.insert(std::make_pair(this->temp->id, std::move(this->temp)));
	return root;
}

int NodePool::load_metadata_lazy() {
	//std::cout << "load_metadata_lazy"<< std::endl;
	int ret;
	ret = adi->kv_retrieve_aio(ksid_skp, &metactx.key_seq, &metactx.value_seq, { keyseq_callback, &metactx } );
	if (ret != 0) {
		throw new runtime_error("io issue eror");
	}

	//std::cout << "loading root id " << temp->skpkey_body.id << ", prefix " << temp->skpkey_body.prefix <<  std::endl;
	ret = adi->kv_retrieve_aio(ksid_skp, &temp->key, &temp->value, { metactx_callback, &metactx } );
	if (ret != 0) {
		throw new runtime_error("io issue eror");
	}
	return ret;
}

void sknode_io_callback(kv_io_context &op, void* private_data)
{
	//std::cerr << "sknode_io_callback: retcode = " << op.retcode << std::endl;
	struct NodePool::flush_context* ctx= (struct NodePool::flush_context*)private_data;
	ctx->iodone(op.retcode);
}

int NodePool::erase_node(SKNode *node) {
	return adi->kv_delete_sync(ksid_skp, &node->key);
}

int NodePool::flush_sknode_async(SKNode *node, bool erase, struct flush_context* ctx) {

	if (erase) {
		return adi->kv_delete_aio(ksid_skp, &node->key, { sknode_io_callback, ctx });
	}
	else {
		node->finalize();
		node->skpkey_body.id = node->id;
		//std::cout << "flushing id " << node->skpkey_body.id << ", prefix " << node->skpkey_body.prefix <<  std::endl;
		node->print_children();
		return adi->kv_store_aio(ksid_skp, &node->key, &node->value, { sknode_io_callback, ctx });
	}
}

SKNode* NodePool::load_sknode_sync(uint32_t id) {
	SKNode* node;
	std::unique_ptr<SKNode> ptr(new SKNode(this, id, prefix, malloc(SKNode::DEFAULT_SKNODE_BUFFER_SIZE), SKNode::DEFAULT_SKNODE_BUFFER_SIZE));

	node = ptr.get();
	//std::cout << "loading id " << node->skpkey_body.id << ", prefix " << node->skpkey_body.prefix <<  std::endl;
	if (this->adi->kv_retrieve_sync(ksid_skp, &node->key, &node->value) != 0)
	{
		throw new runtime_error("sknode is not found");
	}

	ptr->parse();

	pool.insert(std::make_pair(id, std::move(ptr)));
	//std::cout << ">> sknode with id = " << id << " is loaded " << std::endl;

	return node;
}

SKNode* NodePool::create_sknode(uint32_t id, bool isleaf) {
	std::unique_ptr<SKNode> ptr(new SKNode(this, id, prefix, isleaf, malloc(SKNode::DEFAULT_SKNODE_BUFFER_SIZE), SKNode::DEFAULT_SKNODE_BUFFER_SIZE));
	SKNode *n = ptr.get();
	pool.insert(std::make_pair(id, std::move(ptr)));
	return n;
}

SKNode* NodePool::create_sknode(bool isleaf) {
	uint32_t id = seq++;
	std::unique_ptr<SKNode> ptr(new SKNode(this, id, prefix, isleaf, malloc(SKNode::DEFAULT_SKNODE_BUFFER_SIZE), SKNode::DEFAULT_SKNODE_BUFFER_SIZE));
	SKNode *n = ptr.get();
	pool.insert(std::make_pair(id, std::move(ptr)));
	return n;
}

bool NodePool::erase(uint32_t id) {
	auto it = pool.find(id);
	if (it != pool.end()) {
		std::unique_ptr<SKNode> s = std::move(it->second);
		pool.erase(it);
		if (s->lazy_init)
			erased_pages.push_back(std::move(s));
		return true;
	}
	return 0;
}

int NodePool::erase_meta() {
	return adi->kv_delete_sync(ksid_skp, &metactx.key_seq);
}
void NodePool::flush() {
	flush_ctx.status   = -1;
	flush_ctx.num_ios  = 0;
	flush_ctx.num_done = 0;

	for (const auto &p : pool) {
		SKNode *node = p.second.get();
		int id = p.first;

		//std::cout << "CHECK ID " << id << ", " << node->id << std::endl;
		if (id != node->id) exit(1);
	}



	for (const auto &p : pool) {
		if (p.second.get()->dirty) flush_ctx.num_ios++;
	}

	flush_ctx.num_ios++;

	*(uint64_t*)metactx.value_seq.value = this->seq;

	adi->kv_store_aio(ksid_skp, &metactx.key_seq, &metactx.value_seq, { sknode_io_callback, &flush_ctx });

	for (const auto &p : pool) {
		SKNode *n = p.second.get();

		//std::cout << "checking id = " << p.first << ", dirty ? " << n->dirty << std::endl;

		if (n->dirty) {
			//std::cout << "flushing node id = " << n->id << ", leaf ? " << n->isLeaf() << std::endl;
			flush_sknode_async(n, false, &flush_ctx);
		}

		//std::cout << "-----------" << std::endl;
	}
	flush_ctx.status++;

	if (!flush_ctx.wait()) {

		throw new runtime_error("IO error - flush context");
	}

	flush_ctx.status   = -1;
	flush_ctx.num_ios  = erased_pages.size();
	flush_ctx.num_done = 0;

	for (const auto &p : erased_pages) {
		flush_sknode_async(p.get(), true, &flush_ctx);
	}

	if (!flush_ctx.wait()) {
		throw new runtime_error("IO error - flush context");
	}

}


