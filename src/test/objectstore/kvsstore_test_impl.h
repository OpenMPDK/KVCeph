#ifndef CEPH_KVSSTORE_TEST_IMPL_H
#define CEPH_KVSSTORE_TEST_IMPL_H

#include <glob.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <sys/mount.h>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <gtest/gtest.h>

#include "os/ObjectStore.h"
#include "os/kvsstore/KvsStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/coredumpctl.h"

#include "include/unordered_map.h"
#include "store_test_fixture.h"

using namespace std::placeholders;

typedef boost::mt11213b gen_type;

const uint64_t DEF_STORE_TEST_BLOCKDEV_SIZE = 10240000000;

#define dout_context g_ceph_context

// Helpers

static bool bl_eq(bufferlist& expected, bufferlist& actual)
{
  if (expected.contents_equal(actual))
    return true;

  unsigned first = 0;
  if(expected.length() != actual.length()) {
    cout << "--- buffer lengths mismatch " << std::hex
         << "expected 0x" << expected.length() << " != actual 0x"
         << actual.length() << std::dec << std::endl;
    derr << "--- buffer lengths mismatch " << std::hex
         << "expected 0x" << expected.length() << " != actual 0x"
         << actual.length() << std::dec << dendl;
  }
  auto len = std::min(expected.length(), actual.length());
  while ( first<len && expected[first] == actual[first])
    ++first;
  unsigned last = len;
  while (last > 0 && expected[last-1] == actual[last-1])
    --last;
  if(len > 0) {
    cout << "--- buffer mismatch between offset 0x" << std::hex << first
         << " and 0x" << last << ", total 0x" << len << std::dec
         << std::endl;
    derr << "--- buffer mismatch between offset 0x" << std::hex << first
         << " and 0x" << last << ", total 0x" << len << std::dec
         << dendl;
    cout << "--- expected:\n";
    expected.hexdump(cout);
    cout << "--- actual:\n";
    actual.hexdump(cout);
  }
  return false;
}


template <typename T>
int queue_transaction(
  T &store,
  ObjectStore::CollectionHandle ch,
  ObjectStore::Transaction &&t) {
  if (rand() % 2) {
    ObjectStore::Transaction t2;
    t2.append(t);
    return store->queue_transaction(ch, std::move(t2));
  } else {
    return store->queue_transaction(ch, std::move(t));
  }
}


bool sorted(const vector<ghobject_t> &in) {
  ghobject_t start;
  for (vector<ghobject_t>::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (start > *i) {
      cout << start << " should follow " << *i << std::endl;
      return false;
    }
    start = *i;
  }
  return true;
}

void colsplittest(
  ObjectStore *store,
  unsigned num_objects,
  unsigned common_suffix_size,
  bool clones
  ) {
  coll_t cid(spg_t(pg_t(0,52),shard_id_t::NO_SHARD));
  coll_t tid(spg_t(pg_t(1<<common_suffix_size,52),shard_id_t::NO_SHARD));
  auto ch = store->create_new_collection(cid);
  auto tch = store->create_new_collection(tid);
  int r = 0;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, common_suffix_size);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist small;
  small.append("small");
  {
    ObjectStore::Transaction t;
    for (uint32_t i = 0; i < (2 - (int)clones)*num_objects; ++i) {
      stringstream objname;
      objname << "obj" << i;
      ghobject_t a(hobject_t(
         objname.str(),
         "",
         CEPH_NOSNAP,
         i<<common_suffix_size,
         52, ""));
      t.write(cid, a, 0, small.length(), small,
        CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
      if (clones) {
  objname << "-clone";
  ghobject_t b(hobject_t(
           objname.str(),
           "",
           CEPH_NOSNAP,
           i<<common_suffix_size,
           52, ""));
  t.clone(cid, a, b);
      }
      if (i % 100) {
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
  t = ObjectStore::Transaction();
      }
    }
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(tid, common_suffix_size + 1);
    t.split_collection(cid, common_suffix_size+1, 1<<common_suffix_size, tid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ch->flush();

  // check
  vector<ghobject_t> objects;
  r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
           INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
 // ASSERT_EQ(objects.size(), num_objects);
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
  //  ASSERT_EQ(!!(i->hobj.get_hash() & (1<<common_suffix_size)), 0u);
  }

  objects.clear();
  r = store->collection_list(tch, ghobject_t(), ghobject_t::get_max(),
           INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
 // ASSERT_EQ(objects.size(), num_objects);
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
  //  ASSERT_EQ(!(i->hobj.get_hash() & (1<<common_suffix_size)), 0u);
  }

  // merge them again!
  {
    ObjectStore::Transaction t;
    t.merge_collection(tid, cid, common_suffix_size);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // check and clean up
  ObjectStore::Transaction t;
  {
    vector<ghobject_t> objects;
    r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
             INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(objects.size(), num_objects * 2); // both halves
    unsigned size = 0;
    for (vector<ghobject_t>::iterator i = objects.begin();
   i != objects.end();
   ++i) {
      t.remove(cid, *i);
      if (++size > 100) {
  size = 0;
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
  t = ObjectStore::Transaction();
      }
    }
  }
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);

  ch->flush();
  ASSERT_TRUE(!store->collection_exists(tid));
}
//End of Helpers

// KvsStoreTest class

class KvsStoreTest : public StoreTestFixture,
                     public ::testing::WithParamInterface<const char*>{

public:
    KvsStoreTest()
            : StoreTestFixture(GetParam())
    {}
    };

// KvsStoreTest DeferredSetup
class KvsStoreTestDeferredSetup: public KvsStoreTest {
    void SetUp() override {
        // do nothing
    }

protected:
    void DeferredSetup(){
        KvsStoreTest::SetUp();
    }

public:
};

// KvsStore specific Tests

class KvsStoreTestSpecificAUSize : public KvsStoreTestDeferredSetup{

public:
    typedef std::function<void(
        uint64_t num_ops,
        uint64_t max_obj,
        uint64_t max_wr,
        uint64_t align)>
        MatrixTest;

    void StartDeferred(size_t min_alloc_size)
    {
        SetVal(g_conf(), "bluestore_min_alloc_size", stringify(min_alloc_size).c_str());
        DeferredSetup();
    }

private:
    // bluestore matrix testing
    uint64_t max_write = 40 * 1024;
    uint64_t max_size = 400 * 1024;
    uint64_t alignment = 0;
    uint64_t num_ops = 10000;

protected:
    string matrix_get(const char *k)
    {
        if (string(k) == "max_write")
        {
            return stringify(max_write);
        }
        else if (string(k) == "max_size")
        {
            return stringify(max_size);
        }
        else if (string(k) == "alignment")
        {
            return stringify(alignment);
        }
        else if (string(k) == "num_ops")
        {
            return stringify(num_ops);
        }
        else
        {
            char *buf;
            g_conf().get_val(k, &buf, -1);
            string v = buf;
            free(buf);
            return v;
        }
    }

    void matrix_set(const char *k, const char *v)
    {
        if (string(k) == "max_write")
        {
            max_write = atoll(v);
        }
        else if (string(k) == "max_size")
        {
            max_size = atoll(v);
        }
        else if (string(k) == "alignment")
        {
            alignment = atoll(v);
        }
        else if (string(k) == "num_ops")
        {
            num_ops = atoll(v);
        }
        else
        {
            SetVal(g_conf(), k, v);
        }
    }

    void do_matrix_choose(const char *matrix[][10],
                          int i, int pos, int num,
                          MatrixTest fn)
    {
        if (matrix[i][0])
        {
            int count;
            for (count = 0; matrix[i][count + 1]; ++count)
                ;
            for (int j = 1; matrix[i][j]; ++j)
            {
                matrix_set(matrix[i][0], matrix[i][j]);
                do_matrix_choose(matrix,
                                 i + 1,
                                 pos * count + j - 1,
                                 num * count,
                                 fn);
            }
        }
        else
        {
            cout << "---------------------- " << (pos + 1) << " / " << num
                 << " ----------------------" << std::endl;
            for (unsigned k = 0; matrix[k][0]; ++k)
            {
                cout << "  " << matrix[k][0] << " = " << matrix_get(matrix[k][0])
                     << std::endl;
            }
            g_ceph_context->_conf.apply_changes(nullptr);
            fn(num_ops, max_size, max_write, alignment);
        }
    }

    void do_matrix(const char *matrix[][10],
                   MatrixTest fn)
    {

        if (strcmp(matrix[0][0], "bluestore_min_alloc_size") == 0)
        {
            int count;
            for (count = 0; matrix[0][count + 1]; ++count)
                ;
            for (size_t j = 1; matrix[0][j]; ++j)
            {
                if (j > 1)
                {
                    TearDown();
                }
                StartDeferred(strtoll(matrix[0][j], NULL, 10));
                do_matrix_choose(matrix, 1, j - 1, count, fn);
            }
        }
        else
        {
            StartDeferred(0);
            do_matrix_choose(matrix, 0, 0, 1, fn);
        }
    }
};

// End of specifc tests

// ObjectGenerator class
class ObjectGenerator
{
public:
    virtual ghobject_t create_object(gen_type *gen) = 0;
    virtual ~ObjectGenerator() {}
};

class MixedGenerator : public ObjectGenerator
{
public:
    unsigned seq;
    int64_t poolid;
    explicit MixedGenerator(int64_t p) : seq(0), poolid(p) {}
    ghobject_t create_object(gen_type *gen) override
    {
        char buf[100];
        snprintf(buf, sizeof(buf), "OBJ_%u", seq);
        string name(buf);
        if (seq % 2)
        {
            for (unsigned i = 0; i < 300; ++i)
            {
                name.push_back('a');
            }
        }
        ++seq;
        return ghobject_t(
            hobject_t(
                name, string(), rand() & 2 ? CEPH_NOSNAP : rand(),
                (((seq / 1024) % 2) * 0xF00) +
                    (seq & 0xFF),
                poolid, ""));
    }
};
// End of class ObjectGenerator

#endif //CEPH_KVSSTORE_TEST_IMPL_H

