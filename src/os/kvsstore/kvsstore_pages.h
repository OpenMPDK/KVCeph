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
/*
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
*/
struct KvsPage {

  char *   data;
  uint64_t offset;
  uint32_t length;


  // copy disabled
  KvsPage(const KvsPage&) = delete;
  const KvsPage& operator=(const KvsPage&) = delete;

 public:
  KvsPage(size_t page_size, uint64_t offset_): offset(offset_), length(page_size)  {
      data = (char*) malloc(page_size);
  }

  ~KvsPage() {
      free(data);
  }

};

class KvsPageSet {
 public:
  // alloc_range() and get_range() return page refs in a vector
  typedef std::vector<KvsPage *> page_vector;

 private:

  typedef std::map<uint64_t, KvsPage *> page_set;

  typedef typename page_set::iterator iterator;

  page_set pages;
  uint64_t page_size;

  typedef std::mutex lock_type;
  lock_type mutex;



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
      for (const auto &p : pages) {
          if (p.second) delete p.second;
      }
  }

  // disable copy
  KvsPageSet(const KvsPageSet&) = delete;
  const KvsPageSet& operator=(const KvsPageSet&) = delete;

  bool empty() const { return pages.empty(); }
  size_t size() const { return pages.size(); }
  size_t get_page_size() const { return page_size; }

  // allocate all pages that intersect the range [offset,length)
  template <typename Functor>
  inline KvsPage* prepare_page_for_write(const uint64_t offset, const uint64_t length, const uint64_t page_offset, Functor &&page_loader, bool last)
  {
      KvsPage* page = 0;
      auto it = pages.find(page_offset);

      if (it == pages.end()) {
          page = new KvsPage(page_size, page_offset);
          pages.insert(it, { page_offset, page });

          if (offset > page->offset) {
              int ret = page_loader(page->data, page_offset / page_size, page->length);

              if (ret != 0) {
                  // page does not exist. fill 0 from page offset to offset
                  std::fill(page->data, page->data + offset - page->offset, 0);
              }
              if (!last)
                assert(page->length == page_size);
          }
          if (last) {
              page->length = length;
          }
          if (!last && offset + length < page->offset + page->length)
              std::fill(page->data + offset + length - page->offset, page->data + page->length, 0);
      } else {
          page = it->second;
      }
      return page;
  }

    template <typename Functor>
    inline KvsPage* load_page(const uint64_t offset, const uint64_t page_offset, Functor &&page_loader, bool last)
    {
        auto it = pages.find(page_offset);

        if (it == pages.end()) {
            auto page = new KvsPage(page_size, page_offset);
            pages.insert(it, { page_offset, page });

            int ret = page_loader(page->data, page_offset / page_size, page->length);
            if (ret != 0) { return 0; }

            if (!last) {
                assert(page->length == page_size);
            }
            return page;
        } else {
            return it->second;
        }
    }

  template <typename Functor>
  void alloc_range(uint64_t offset, uint64_t length, page_vector &range, Functor &&page_loader) {
      FTRACE
    if (length == 0) return;

    int pgid;
    const int num_pages = count_pages(offset, length);
    uint64_t page_offset = (offset + length - 1) & ~(page_size-1);

    TR << "num pages = " << num_pages << TREND;
    range.reserve(num_pages);

    std::lock_guard<lock_type> lock(mutex);

    for (pgid = 0; pgid < num_pages -1 ; pgid++) {
        TR << "pgid " << pgid  << TREND;
        range[pgid] = prepare_page_for_write(offset, length, page_offset, page_loader, false);
        length      -= page_size;
        page_offset += page_size;
    }
    TR << "last pgid " << pgid  << TREND;

    // last page
    range[pgid] = prepare_page_for_write(offset, length, page_offset, page_loader, true);

  }

  // return all allocated pages that intersect the range [offset,length)
  bool get_range(uint64_t offset, uint64_t length, page_vector &range, const std::function< int (char *, int, uint32_t&) > &page_loader) {
      FTRACE
      if (length == 0) return true;

      int pgid = 0;
      const int num_pages = count_pages(offset, length);
      uint64_t page_offset = (offset + length - 1) & ~(page_size-1);

      std::lock_guard<lock_type> lock(mutex);

      for (pgid = 0; pgid < num_pages -1 ; pgid++) {
          KvsPage *p = load_page(offset, page_offset, page_loader, false);
          if (p == 0) { range.clear(); return false; }

          range.push_back(p);
          length      -= page_size;
          page_offset += page_size;
      }

      // last page
      KvsPage *p = load_page(offset, page_offset, page_loader, true);
      if (p == 0) { range.clear(); return false; }

      TR << "last page: length = " << p->length << ", offset = " << p->offset << ", hash = " << ceph_str_hash_linux(p->data, p->length) << TREND;
      range.push_back(p);
      TR << "last page: length = " << range[pgid]->length << ", offset = " << range[pgid]->offset << ", hash = " << ceph_str_hash_linux(range[pgid]->data, range[pgid]->length) << TREND;
      TR << "range = " << range.size() << TREND;
      return true;

  }

  void free_pages_after(uint64_t offset) {
    std::lock_guard<lock_type> lock(mutex);
    auto cur = pages.lower_bound(offset & ~(page_size-1));
    if (cur == pages.end())
      return;

    if (cur->second->offset < offset) cur++;

    while (cur != pages.end()) {
        delete cur->second;
        cur++;
    }
  }

  void list_pages(const std::function< void (int, char *, int )> &mypage) {
    std::lock_guard<lock_type> lock(mutex);
    auto cur = pages.begin();
    while (cur != pages.end()) {
        auto *page = cur->second;
    	mypage(page->offset / page_size, page->data, page->length);
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
