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

#include <sstream>
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
struct KvsPageSet
{

};


struct KvsStoreDataObject {
    KvsPageSet data;
    uint64_t data_len;
    size_t page_size;
    bool persistent= false;
    int readers = 0;
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
*/




//#include "kvsstore_pages.cc"

#endif /* SRC_OS_KVSSTORE_KVSSTORE_PAGES_H_ */
