#include "kvsstore_test_impl.h"

//// --------------------------------------------------------------------
//// Unittest starts
//// --------------------------------------------------------------------


TEST_P(KvsStoreTest, ColSplitTest1) {
//    colsplittest(store.get(), 100, 7, false);
  }


TEST_P(KvsStoreTest, OMapIterator) {
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  int count = 0;
  auto ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_clear(cid, hoid); // new feature 
    map<string, bufferlist> start_set;
    t.omap_setkeys(cid, hoid, start_set);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectMap::ObjectMapIterator iter;
  bool correct;
  //basic iteration
  for (int i = 0; i < 100; i++) {
    if (!(i%5)) {
      std::cout << "On iteration " << i << std::endl;
    }
    bufferlist bl;

    // FileStore may deadlock two active iterators over the same data
    iter = ObjectMap::ObjectMapIterator();
    std::cout << " after OMapIterator inititation " << std::endl;
    iter = store->get_omap_iterator(ch, hoid);
    std::cout << " after OMapIterator get_omap_iterator inititation " << std::endl;
    iter->seek_to_first();
    std::cout << " after seek_to_first()" << std::endl;
    //std::cout << " after iter->valid() = " << iter->valid()<< std::endl;
    //std::cout << " omap key = " << iter->key() << std::endl;
    //iter->next();
    std::cout << " after iter->next()" << std::endl;
    for (iter->seek_to_first(), count=0; iter->valid(); iter->next(), count++) {
      string key = iter->key();
      bufferlist value = iter->value();
      correct = attrs.count(key) && (string(value.c_str()) == string(attrs[key].c_str()));
      if (!correct) {
  if (attrs.count(key) > 0) {
    std::cout << "key " << key << "in omap , " << value.c_str() << " : " << attrs[key].c_str() << std::endl;
  }
  else
    std::cout << "key " << key << "should not exists in omap" << std::endl;
      }
      ASSERT_EQ(correct, true);
    }
    std::cout << " # of omaps count = " << count << std::endl;
    //ASSERT_EQ((int)attrs.size(), count);

    // FileStore may deadlock an active iterator vs queue_transaction
    iter = ObjectMap::ObjectMapIterator();

    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    bl.clear();
    bufferptr bp(buf, strlen(buf) + 1);
    bl.append(bp);
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    attrs.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    ObjectStore::Transaction t;
    t.omap_setkeys(cid, hoid, to_add);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  iter = store->get_omap_iterator(ch, hoid);
  //lower bound
  string bound_key = "key-5";
  iter->lower_bound(bound_key);
  correct = bound_key <= iter->key();
  if (!correct) {
    std::cout << "lower bound, bound key is " << bound_key << " < iter key is " << iter->key() << std::endl;
  }
  ASSERT_EQ(correct, true);
  //upper bound
  iter->upper_bound(bound_key);
  correct = iter->key() > bound_key;
  if (!correct) {
    std::cout << "upper bound, bound key is " << bound_key << " >= iter key is " << iter->key() << std::endl;
  }
  ASSERT_EQ(correct, true);

  // FileStore may deadlock an active iterator vs queue_transaction
  iter = ObjectMap::ObjectMapIterator();
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, Trivial) {
}

TEST_P(KvsStoreTest, TrivialRemount) {
  int r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
}

TEST_P(KvsStoreTest, SimpleRemount) {
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  bufferlist bl;
  bl.append("1234512345");
  int r;
  auto ch = store->create_new_collection(cid);
  {
    cerr << "create collection + write" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.write(cid, hoid, 0, bl.length(), bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ch.reset();
  r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
  ch = store->open_collection(cid);
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid2, 0, bl.length(), bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ch.reset();
  r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
  ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, OmapSimple) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("omap_obj", CEPH_NOSNAP),
          "key", 123, -1, ""));
  bufferlist small;
  small.append("small");
  map<string,bufferlist> km;
  km["foo"] = small;
  km["bar"].append("asdfjkasdkjdfsjkafskjsfdj");
  bufferlist header;
  header.append("this is a header");
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_setkeys(cid, hoid, km);
    t.omap_setheader(cid, hoid, header);
    cerr << "Creating object and set omap " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // get header, keys
  {
    bufferlist h;
    map<string,bufferlist> r;
    store->omap_get(ch, hoid, &h, &r);
    ASSERT_TRUE(bl_eq(header, h));
    ASSERT_EQ(r.size(), km.size());
    cout << "r: " << r << std::endl;
  }
  // test iterator with seek_to_first
  {
    map<string,bufferlist> r;
    ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(ch, hoid);
    for (iter->seek_to_first(); iter->valid(); iter->next()) {
      r[iter->key()] = iter->value();
    }
    cout << "r: " << r << std::endl;
    ASSERT_EQ(r.size(), km.size());
  }
  // test iterator with initial lower_bound
  {
    map<string,bufferlist> r;
    ObjectMap::ObjectMapIterator iter = store->get_omap_iterator(ch, hoid);
    for (iter->lower_bound(string()); iter->valid(); iter->next()) {
      r[iter->key()] = iter->value();
    }
    cout << "r: " << r << std::endl;
    ASSERT_EQ(r.size(), km.size());
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, OMapTest) {
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  auto ch = store->create_new_collection(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_clear(cid, hoid);
    map<string, bufferlist> start_set;
    t.omap_setkeys(cid, hoid, start_set);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  for (int i = 0; i < 100; i++) {
    //if (!(i%5)) {
      std::cout << "On iteration " << i << std::endl;
    //}
    ObjectStore::Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(ch, hoid, &bl, &cur_attrs);
    ASSERT_EQ(r, 0);
    for (map<string, bufferlist>::iterator j = attrs.begin();
   j != attrs.end();
   ++j) {
      bool correct = cur_attrs.count(j->first) && string(cur_attrs[j->first].c_str()) == string(j->second.c_str());
      std::cout << " curr_attrs.count() = " 
                << cur_attrs.count(j->first) 
                << " attrs.size() = " << attrs.size()
                << std::endl;

      if (!correct) {
        std::cout << j->first << " is present in cur_attrs " 
                  << cur_attrs.count(j->first) << " times " << std::endl;
      if (cur_attrs.count(j->first) > 0) {
        std::cout << j->second.c_str() << " : "
                  << cur_attrs[j->first].c_str() << std::endl;
        }
      }
      ASSERT_EQ(correct, true);
    }
    ASSERT_EQ(attrs.size(), cur_attrs.size());

    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    bl.clear();
    bufferptr bp(buf, strlen(buf) + 1);
    bl.append(bp);
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    attrs.insert(pair<string, bufferlist>("key-" + string(buf), bl));
    t.omap_setkeys(cid, hoid, to_add);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  int i = 0;
  while (attrs.size()) {
    if (!(i%5)) {
      std::cout << "removal: On iteration " << i << std::endl;
    }
    ObjectStore::Transaction t;
    bufferlist bl;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(ch, hoid, &bl, &cur_attrs);
    ASSERT_EQ(r, 0);
    for (map<string, bufferlist>::iterator j = attrs.begin();
   j != attrs.end();
   ++j) {
      bool correct = cur_attrs.count(j->first) && string(cur_attrs[j->first].c_str()) == string(j->second.c_str());
      if (!correct) {
  std::cout << j->first << " is present in cur_attrs " << cur_attrs.count(j->first) << " times " << std::endl;
  if (cur_attrs.count(j->first) > 0) {
    std::cout << j->second.c_str() << " : " << cur_attrs[j->first].c_str() << std::endl;
  }
      }
      ASSERT_EQ(correct, true);
    }

    string to_remove = attrs.begin()->first;
    set<string> keys_to_remove;
    keys_to_remove.insert(to_remove);
    t.omap_rmkeys(cid, hoid, keys_to_remove);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    attrs.erase(to_remove);

    ++i;
  }

  {
    bufferlist bl1;
    bl1.append("omap_header");
    ObjectStore::Transaction t;
    t.omap_setheader(cid, hoid, bl1);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    t = ObjectStore::Transaction();
 
    bufferlist bl2;
    bl2.append("value");
    map<string, bufferlist> to_add;
    to_add.insert(pair<string, bufferlist>("key", bl2));
    t.omap_setkeys(cid, hoid, to_add);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist bl3;
    map<string, bufferlist> cur_attrs;
    r = store->omap_get(ch, hoid, &bl3, &cur_attrs);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(cur_attrs.size(), size_t(1));
    ASSERT_TRUE(bl_eq(bl1, bl3));
 
    set<string> keys;
    r = store->omap_get_keys(ch, hoid, &keys);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(keys.size(), size_t(1));
  }

  // test omap_clear, omap_rmkey_range
  {
    {
      map<string,bufferlist> to_set;
      for (int n=0; n<10; ++n) {
  to_set[stringify(n)].append("foo");
      }
      bufferlist h;
      h.append("header");
      ObjectStore::Transaction t;
      t.remove(cid, hoid);
      t.touch(cid, hoid);
      t.omap_setheader(cid, hoid, h);
      t.omap_setkeys(cid, hoid, to_set);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      ObjectStore::Transaction t;
      t.omap_rmkeyrange(cid, hoid, "3", "7");
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      bufferlist hdr;
      map<string,bufferlist> m;
      store->omap_get(ch, hoid, &hdr, &m);
      ASSERT_EQ(6u, hdr.length());
      ASSERT_TRUE(m.count("2"));
      ASSERT_TRUE(!m.count("3"));
      ASSERT_TRUE(!m.count("6"));
      ASSERT_TRUE(m.count("7"));
      ASSERT_TRUE(m.count("8"));
      //cout << m << std::endl;
      ASSERT_EQ(6u, m.size());
    }
    {
      ObjectStore::Transaction t;
      t.omap_clear(cid, hoid);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      bufferlist hdr;
      map<string,bufferlist> m;
      store->omap_get(ch, hoid, &hdr, &m);
      ASSERT_EQ(0u, hdr.length());
      ASSERT_EQ(0u, m.size());
    }
  }

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(KvsStoreTest, OmapCloneTest) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP),
          "key", 123, -1, ""));
  bufferlist small;
  small.append("small");
  map<string,bufferlist> km;
  km["foo"] = small;
  km["bar"].append("asdfjkasdkjdfsjkafskjsfdj");
  bufferlist header;
  header.append("this is a header");
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    t.omap_setkeys(cid, hoid, km);
    t.omap_setheader(cid, hoid, header);
    cerr << "Creating object and set omap " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP),
           "key", 123, -1, ""));
  {
    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    cerr << "Clone object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    map<string,bufferlist> r;
    bufferlist h;
    store->omap_get(ch, hoid2, &h, &r);
    ASSERT_TRUE(bl_eq(header, h));
    ASSERT_EQ(r.size(), km.size());
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, SimpleCloneRangeTest) {
  int r;
  coll_t cid;
  auto ch = store->create_new_collection(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  hoid.hobj.pool = -1;
  bufferlist small, newdata;
  small.append("small");
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 10, 5, small);
    cerr << "Creating object and write bl " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  hoid2.hobj.pool = -1;
  {
    ObjectStore::Transaction t;
    t.clone_range(cid, hoid, hoid2, 10, 5, 10);
    cerr << "Clone range object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    r = store->read(ch, hoid2, 10, 5, newdata);
    ASSERT_EQ(r, 5);
    ASSERT_TRUE(bl_eq(small, newdata));
  }
  {
    ObjectStore::Transaction t;
    t.truncate(cid, hoid, 1024*1024);
    t.clone_range(cid, hoid, hoid2, 0, 1024*1024, 0);
    cerr << "Clone range object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
    struct stat stat, stat2;
    r = store->stat(ch, hoid, &stat);
    r = store->stat(ch, hoid2, &stat2);
    ASSERT_EQ(stat.st_size, stat2.st_size);
    ASSERT_EQ(1024*1024, stat2.st_size);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}




// instantiation
INSTANTIATE_TEST_CASE_P(
    ObjectStore,
    KvsStoreTest,
    ::testing::Values(
        "kvsstore"));

// end of instatiation

// main() function

int main(int argc, char **argv)
{
    vector<const char *> args;
    argv_to_vec(argc, (const char **)argv, args);

    auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                           CODE_ENVIRONMENT_UTILITY,
                           CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    common_init_finish(g_ceph_context);

    // make sure we can adjust any config settings
    g_ceph_context->_conf._clear_safe_to_start_threads();

    g_ceph_context->_conf.set_val_or_die("osd_journal_size", "400");
    g_ceph_context->_conf.set_val_or_die("filestore_index_retry_probability", "0.5");
    g_ceph_context->_conf.set_val_or_die("filestore_op_thread_timeout", "1000");
    g_ceph_context->_conf.set_val_or_die("filestore_op_thread_suicide_timeout", "10000");
    //g_ceph_context->_conf.set_val_or_die("filestore_fiemap", "true");
    g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_mkfs", "false");
    g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_mount", "false");
    g_ceph_context->_conf.set_val_or_die("bluestore_fsck_on_umount", "false");
    g_ceph_context->_conf.set_val_or_die("bluestore_debug_misc", "true");
    g_ceph_context->_conf.set_val_or_die("bluestore_debug_small_allocations", "4");
    g_ceph_context->_conf.set_val_or_die("bluestore_debug_freelist", "true");
    g_ceph_context->_conf.set_val_or_die("bluestore_clone_cow", "true");
    g_ceph_context->_conf.set_val_or_die("bluestore_max_alloc_size", "196608");

    // set small cache sizes so we see trimming during Synthetic tests
    g_ceph_context->_conf.set_val_or_die("bluestore_cache_size_hdd", "4000000");
    g_ceph_context->_conf.set_val_or_die("bluestore_cache_size_ssd", "4000000");

    // very short *_max prealloc so that we fall back to async submits
    g_ceph_context->_conf.set_val_or_die("bluestore_blobid_prealloc", "10");
    g_ceph_context->_conf.set_val_or_die("bluestore_nid_prealloc", "10");
    g_ceph_context->_conf.set_val_or_die("bluestore_debug_randomize_serial_transaction",
                                         "10");

    g_ceph_context->_conf.set_val_or_die("bdev_debug_aio", "true");

    // specify device size
    g_ceph_context->_conf.set_val_or_die("bluestore_block_size",
                                         stringify(DEF_STORE_TEST_BLOCKDEV_SIZE));

    g_ceph_context->_conf.set_val_or_die(
        "enable_experimental_unrecoverable_data_corrupting_features", "*");

    g_ceph_context->_conf.apply_changes(nullptr);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

//end of main()
