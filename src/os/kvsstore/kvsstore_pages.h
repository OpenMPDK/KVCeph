/*
 * kvsstore_pages.h
 *
 * Page-based I/O implementation - mostly borrowed from MemStore
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_PAGES_H_
#define SRC_OS_KVSSTORE_KVSSTORE_PAGES_H_


///--------------------------------------------------------
/// Page
///--------------------------------------------------------

#include <algorithm>
#include <atomic>
#include <cassert>
#include <mutex>
#include <vector>
#include <boost/intrusive/avl_set.hpp>
#include <boost/intrusive_ptr.hpp>
#include <include/encoding.h>
#include "kvio/kvio_options.h"
#include "kvsstore_debug.h"

struct KvsPage {

  char *const data;
  uint64_t offset;

  boost::intrusive::avl_set_member_hook<> hook;

  std::atomic<uint16_t> nrefs;
  void get() { ++nrefs; }
  void put() { if (--nrefs == 0) delete this; }

  typedef boost::intrusive_ptr<KvsPage> Ref;
  friend void intrusive_ptr_add_ref(KvsPage *p) { p->get(); }
  friend void intrusive_ptr_release(KvsPage *p) { p->put(); }

  // key-value comparison functor for avl
  struct Less {
    bool operator()(uint64_t offset, const KvsPage &page) const {
      return offset < page.offset;
    }
    bool operator()(const KvsPage &page, uint64_t offset) const {
      return page.offset < offset;
    }
    bool operator()(const KvsPage &lhs, const KvsPage &rhs) const {
      return lhs.offset < rhs.offset;
    }
  };

  // place holder for futher read
  static Ref create(uint64_t offset = 0) {
	return new KvsPage(0, offset);
  }

  static Ref create(size_t page_size, uint64_t offset = 0) {
	char *buffer = (char*)malloc(page_size);
    return new KvsPage(buffer, offset);
  }

  // copy disabled
  KvsPage(const KvsPage&) = delete;
  const KvsPage& operator=(const KvsPage&) = delete;

 private: // private constructor, use create() instead
  KvsPage(char *data, uint64_t offset) : data(data), offset(offset), nrefs(1) {}

  static void operator delete(void *p) {
      if (p) {
          KvsPage *page = reinterpret_cast<KvsPage *>(p);
          if (page->data) free(page->data);
      }
  }
};

class KvsPageSet {
 public:
  // alloc_range() and get_range() return page refs in a vector
  typedef std::vector<KvsPage::Ref> page_vector;

 private:
  // store pages in a boost intrusive avl_set
  typedef KvsPage::Less page_cmp;
  typedef boost::intrusive::member_hook<KvsPage,
          boost::intrusive::avl_set_member_hook<>,
          &KvsPage::hook> member_option;
  typedef boost::intrusive::avl_set<KvsPage,
          boost::intrusive::compare<page_cmp>, member_option> page_set;

  typedef typename page_set::iterator iterator;

  page_set pages;
  uint64_t page_size;

  typedef std::mutex lock_type;
  lock_type mutex;

  void free_pages(iterator cur, iterator end) {
    while (cur != end) {
      KvsPage *page = &*cur;
      cur = pages.erase(cur);
      page->put();
    }
  }

  int count_pages(uint64_t offset, uint64_t len) const {
    // count the overlapping pages
    int count = 0;
    if (offset % page_size) {
      count++;
      size_t rem = page_size - offset % page_size;
      len = len <= rem ? 0 : len - rem;
    }
    count += len / page_size;
    if (len % page_size)
      count++;
    return count;
  }

 public:
  explicit KvsPageSet(size_t page_size) : page_size(page_size) {}
  KvsPageSet(KvsPageSet &&rhs)
    : pages(std::move(rhs.pages)), page_size(rhs.page_size) {}
  ~KvsPageSet() {
    free_pages(pages.begin(), pages.end());
  }

  // disable copy
  KvsPageSet(const KvsPageSet&) = delete;
  const KvsPageSet& operator=(const KvsPageSet&) = delete;

  bool empty() const { return pages.empty(); }
  size_t size() const { return pages.size(); }
  size_t get_page_size() const { return page_size; }

  // allocate all pages that intersect the range [offset,length)
  template <typename Functor>
  void alloc_range(uint64_t offset, uint64_t length, page_vector &range, Functor &&page_loader) {
    // loop in reverse so we can provide hints to avl_set::insert_check()
    //	and get O(1) insertions after the first
    uint64_t position = offset + length - 1;

    range.resize(count_pages(offset, length));
    auto out = range.rbegin();

    std::lock_guard<lock_type> lock(mutex);
    iterator cur = pages.end();
    while (length) {
      const uint64_t page_offset = position & ~(page_size-1); // start offset of the page, 0, 8192, ... if pagesize = 8k

      typename page_set::insert_commit_data commit;
      auto insert = pages.insert_check(cur, page_offset, page_cmp(), commit);
      if (insert.second) {
    	auto page = KvsPage::create(page_size, page_offset);
        cur = pages.insert_commit(*page, commit);

        // assume that the caller will write to the range [offset,length),
        //  so we only need to read data offset
        //offset + length < page->offset + page_size
        if (offset > page->offset) {
        	int ret = page_loader(page->data, page_offset / page_size);
        	if (ret != 0) throw "page load failed";
        }
      } else { // exists
        cur = insert.first;
      }
      // add a reference to output vector
      out->reset(&*cur);
      ++out;

      auto c = std::min(length, (position & (page_size-1)) + 1);
      position -= c;
      length -= c;
    }
    // make sure we sized the vector correctly
    ceph_assert(out == range.rend());
  }


  bool alloc_range_for_read(uint64_t offset, uint64_t length, const std::function< int (char *, int) > &page_loader) {
      uint64_t position = offset + length - 1;

      std::lock_guard<lock_type> lock(mutex);
      iterator cur = pages.end();
      while (length) {
        const uint64_t page_offset = position & ~(page_size-1); // start offset of the page, 0, 8192, ... if pagesize = 8k

        typename page_set::insert_commit_data commit;
        auto insert = pages.insert_check(cur, page_offset, page_cmp(), commit);
        if (insert.second) {
          // load the page
          auto page = KvsPage::create(page_size, page_offset);

          int ret = page_loader(page->data, page_offset / page_size);
          if (ret != 0) { page->put(); return false; }

          cur = pages.insert_commit(*page, commit);

        } else { // exists
          cur = insert.first;
        }

        auto c = std::min(length, (position & (page_size-1)) + 1);
        position -= c;
        length -= c;
      }

      return true;
    }


  // return all allocated pages that intersect the range [offset,length)
  bool get_range(uint64_t offset, uint64_t length, page_vector &range, const std::function< int (char *, int) > &page_loader) {
	// ensure all pages are loaded
	if (!alloc_range_for_read(offset, length, page_loader)) {
		return false;
	}

    auto cur = pages.lower_bound(offset & ~(page_size-1), page_cmp());
    while (cur != pages.end() && cur->offset < offset + length) {
      range.push_back(&*cur++);
    }
    return true;
  }

  void free_pages_after(uint64_t offset) {
    std::lock_guard<lock_type> lock(mutex);
    auto cur = pages.lower_bound(offset & ~(page_size-1), page_cmp());
    if (cur == pages.end())
      return;
    if (cur->offset < offset)
      cur++;
    free_pages(cur, pages.end());
  }

  void list_pages(const std::function< void (int, char *, int )> &mypage) {
    std::lock_guard<lock_type> lock(mutex);
    auto cur = pages.begin();
    while (cur != pages.end()) {
    	mypage(cur->offset / page_size, cur->data, page_size);
    	cur++;
    }
  }

};

struct KvsStoreDataObject {
  KvsPageSet data;
  uint64_t data_len;
  size_t page_size;

  size_t get_size() const { return data_len; }

  // Functor (char* data, int pageid)->int (success?)
  template<typename Functor>
  int read(uint64_t offset, uint64_t len, bufferlist &bl, Functor &&page_loader);
  template<typename Functor>
  int write(uint64_t offset, bufferlist &bl, Functor &&page_loader);
  template<typename Functor>
  int zero(uint64_t offset, unsigned length, Functor &&page_loader);
  template<typename Functor>
  int clone(KvsStoreDataObject *src, uint64_t srcoff, unsigned len, uint64_t dstoff, Functor &&page_loader);
  template<typename Functor>
  int truncate(uint64_t offset, Functor &&page_loader);

  // void (int pageid, char *data, int length)
  template<typename Functor>
  void list_pages(Functor &&mypage) { data.list_pages(mypage); }

  //const std::function< int (int) >
  template<typename Functor>
  void remove_object(uint64_t size, Functor &&remover);

  //uint32_t get_onode_size(uint64_t size_before);
  KvsStoreDataObject() : data(KVS_OBJECT_SPLIT_SIZE), data_len(0), page_size(KVS_OBJECT_SPLIT_SIZE) {}
};

#include "kvsstore_pages.cc"

#endif /* SRC_OS_KVSSTORE_KVSSTORE_PAGES_H_ */
