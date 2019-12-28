#include "kvsstore_test_impl.h"


//// --------------------------------------------------------------------
//// Unittest starts
//// --------------------------------------------------------------------

#define DEBUG 0

template<typename T, typename FUNC>
inline void assert_eq(T a, T b, const FUNC &c) { if (a != b) return c(a, b); }

template<typename T, typename O>
inline void list_objects_in_collection(O &store, T ch){
    vector<ghobject_t> objects;
    int r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    for (vector<ghobject_t>::iterator i = objects.begin(); i != objects.end(); ++i) {
        TR << "found: " << (*i) ;
    }
}
template<class InputIt1, class InputIt2>
inline int kvkey_lex_compare2(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2)
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

TEST_P(KvsStoreTest, MergeCollectionTest){
    int num_objects = 1;
    int common_suffix_size = 5;

    coll_t cid(spg_t(pg_t(0,52),shard_id_t::NO_SHARD));
    coll_t tid(spg_t(pg_t(1<<common_suffix_size,52),shard_id_t::NO_SHARD));
    auto ch = store->create_new_collection(cid);
    auto tch = store->create_new_collection(tid);
    int r = 0;
    {
        cerr << " create Collection 1" << std::endl;
        ObjectStore::Transaction t;
        t.create_collection(cid, common_suffix_size);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
        cerr << " created Collection 1" << std::endl;
    }
    bufferlist small;
    small.append("small");

// Write objects to collection 1
    {
        ObjectStore::Transaction t;
        //for (uint32_t i = 0; i < num_objects; ++i) {
        stringstream objname;
        objname << "obja" << 0;
        ghobject_t hoid_a(hobject_t(objname.str(), "", CEPH_NOSNAP, 0, 52, ""));
        t.write(cid, hoid_a, 0, small.length(), small, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);

        // check for object
        bufferlist in;
        r = store->read(ch, hoid_a, 0, small.length(), in);
        cerr << " read oid: " << hoid_a << ",  retcode: " << r
             << " written bl length: " << in.length()
             << " read bl length: " << small.length()
             << ", written hash: " << ceph_str_hash_linux(small.c_str(), small.length())
             << ", read hash: " << ceph_str_hash_linux(in.c_str(), in.length())
             << std::endl;

        ASSERT_EQ(r, small.length());
        ASSERT_EQ(r, in.length());
        ASSERT_EQ(ceph_str_hash_linux(in.c_str(), in.length()), ceph_str_hash_linux(small.c_str(), small.length()));
        ASSERT_EQ(small.length(),in.length());
        ASSERT_TRUE(bl_eq(in, small));

        in.clear();
        // }
    }

//Collection 2
    {
        cerr << " create Collection 2 " << std::endl;
        ObjectStore::Transaction t;
        t.create_collection(tid, common_suffix_size + 1);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
        cerr << " created Collection 2" << std::endl;
    }


// Write objects to collection 2
    {
        ObjectStore::Transaction t;
        stringstream objname1;
        objname1 << "objb" << 1;
        ghobject_t hoid_b(hobject_t( objname1.str(), "", CEPH_NOSNAP, 1<<common_suffix_size, 52, ""));
        t.write(tid, hoid_b, 0, small.length(), small, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
        r = queue_transaction(store, tch, std::move(t));
        ASSERT_EQ(r, 0);
        cerr << " object 2 written " << std::endl;
        // check for object
        bufferlist in;
        r = store->read(tch, hoid_b, 0, small.length(), in);
        cerr << " read oid: " << hoid_b << ",  retcode: " << r
             << " written bl length: " << in.length()
             << " read bl length: " << small.length()
             << ", written hash: " << ceph_str_hash_linux(small.c_str(), small.length())
             << ", read hash: " << ceph_str_hash_linux(in.c_str(), in.length())
             << std::endl;

        ASSERT_EQ(r, small.length());
        ASSERT_EQ(r, in.length());
        ASSERT_EQ(ceph_str_hash_linux(in.c_str(), in.length()), ceph_str_hash_linux(small.c_str(), small.length()));
        ASSERT_EQ(small.length(),in.length());
        ASSERT_TRUE(bl_eq(in, small));

        in.clear();
        //}
    }

// Merge them tid->cid
    {
        ObjectStore::Transaction t;
        t.merge_collection(tid, cid, common_suffix_size);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
        cerr << " Merge Collection done 2->1" << std::endl;
    }

// check for final merged
    {
        ObjectStore::Transaction t;
        vector<ghobject_t> objects;
        r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
        cerr << " objects.size = " << objects.size() << std::endl;
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
        t.remove_collection(cid);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
    }
}


TEST_P(KvsStoreTest, OpenCollectionTest2) {
    coll_t cid(spg_t(pg_t(0, 3), shard_id_t(-1)));
    ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
    ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
    bufferlist bl;
    bl.append("1234512345");
    int r;

    TR << "test starts";
    auto ch = open_collection_safe(cid);
    {
        cerr << "create collection + write" << std::endl;
        ObjectStore::Transaction t;
        // t.create_collection(cid, 0);
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

    ch = open_collection_safe(cid);
    {
        ObjectStore::Transaction t;
        //t.create_collection(cid, 0);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
        bool exists = store->exists(ch, hoid);
        ASSERT_TRUE(!exists);
    }
    {
        ObjectStore::Transaction t;
        t.remove_collection(cid);
        cerr << "remove collection 2" << std::endl;
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
    }
}


TEST_P(KvsStoreTest, SimpleWriteCollectionList2)
{
    int r;
    coll_t cid(spg_t(pg_t(0, 1), shard_id_t(-1)));
    auto cha = open_collection_safe(cid);
    int NUM_OBJS = 10;
    int WRITE_SIZE = 10;
    set<ghobject_t> created;
    string base = "";

    bufferlist bl1;
    for (int i = 0; i < WRITE_SIZE; i++)
        bl1.append("a");

    {

        cerr << " write objects to collection" << std::endl;
        ObjectStore::Transaction t;
        for (int i = 0; i < NUM_OBJS; i++)
        {
            // cerr << "Object " << i << std::endl;
            char buf[100];
            snprintf(buf, sizeof(buf), "%d", i);
            ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));
            TR << "create object: shard id = " << (int) hoid.shard_id;
            created.insert(hoid);

            t.write(cid, hoid, 0, bl1.length(), bl1);
            if (i % 100)
            {
                r = queue_transaction(store, cha, std::move(t));
                ASSERT_EQ(r, 0);
                t = ObjectStore::Transaction();
            }
        }
        r = queue_transaction(store, cha, std::move(t));
        ASSERT_EQ(r, 0);
    }
    ASSERT_EQ(created.size(), NUM_OBJS);
    cerr << " ALL objects written to collection" << std::endl;
    cha->flush();

    cerr << " Checking all objects" << std::endl;
    vector<ghobject_t> objects;
    r = store->collection_list(cha, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    std::cerr << "Number of objects read = " << objects.size() << ", actual number = " << NUM_OBJS << std::endl;
    for (vector<ghobject_t>::iterator hoid_iter = objects.begin();
         hoid_iter != objects.end();
         ++hoid_iter)
    {
        cerr << "  Object returned = " << *hoid_iter << std::endl;
    }
    ASSERT_EQ(objects.size(), created.size());

    {
        cerr << " reading all objects " << std::endl;

        for (vector<ghobject_t>::iterator hoid_iter = objects.begin();
             hoid_iter != objects.end();
             ++hoid_iter)
        {
            bufferlist in;
            cerr << " Reading Object = " << *hoid_iter << std::endl;
            r = store->read(cha, *hoid_iter, 0, bl1.length(), in);
            ASSERT_EQ(bl1.length(), r);
            ASSERT_EQ(bl1.length(), in.length());
            ASSERT_EQ(ceph_str_hash_linux(bl1.c_str(), bl1.length()), ceph_str_hash_linux(in.c_str(), in.length()));
            in.clear();
        }
        bl1.clear();
        cerr << " ALL OBJECTS READ " << std::endl;
    }
}


TEST_P(KvsStoreTest, SimpleWriteCollectionList) {
    int r;
    coll_t cid;//(spg_t(pg_t(0, 5), shard_id_t(1)));
    auto cha = open_collection_safe(cid);
    int NUM_OBJS = 1;
    int WRITE_SIZE = 10;
    set<ghobject_t> created;
    string base = "";

    bufferlist bl1;
    for (int i = 0; i<WRITE_SIZE; i++)
        bl1.append("a");

    {

        cerr << " write objects to collection" << std::endl;
        ObjectStore::Transaction t;
        for (int i = 0; i < NUM_OBJS; i++){
            cerr << "Object " << i << std::endl;
            char buf[100];
            snprintf(buf, sizeof(buf), "%d", i);
            ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));
            created.insert(hoid);

            t.write(cid, hoid, 0, bl1.length(), bl1);
            if (i % 100) {
                r = queue_transaction(store, cha, std::move(t));
                ASSERT_EQ(r, 0);
                t = ObjectStore::Transaction();
            }
        }
        r = queue_transaction(store, cha, std::move(t));
        ASSERT_EQ(r, 0);
    }
    ASSERT_EQ(created.size(), NUM_OBJS);
    cerr << " ALL objects written to collection" << std::endl;

    cerr << " Checking all objects" << std::endl;
    vector<ghobject_t> objects;
    r = store->collection_list(cha, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(objects.size(), created.size());


    {
        cerr << " reading all objects " << std::endl;

        for(vector<ghobject_t>::iterator hoid_iter = objects.begin();
            hoid_iter != objects.end();
            ++hoid_iter)
        {
            bufferlist in;
            cerr << " Reading Object = " << *hoid_iter << std::endl;
            r = store->read(cha, *hoid_iter, 0, bl1.length(), in);
            ASSERT_EQ(bl1.length(),r);
            ASSERT_EQ(bl1.length(),in.length());
            ASSERT_EQ(ceph_str_hash_linux(bl1.c_str(), bl1.length()), ceph_str_hash_linux(in.c_str(), in.length()));
            in.clear();

        }
        bl1.clear();
        cerr << " ALL OBJECTS READ " << std::endl;
    }
}


TEST_P(KvsStoreTest, PartialWriteTest){
    coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));// Added for iterator bug in FW
    int r;
    bufferlist bl1;
    bl1.append_zero(8192*2);
    memset(bl1.c_str(), '1', 8192*2);
    bl1.append("helloworld");

    TR << "original hash = " << ceph_str_hash_linux(bl1.c_str(), bl1.length());
    TR << "original content 1st page = " << std::string(bl1.c_str(), 14);
    TR << "original content 2nd page = " << std::string(bl1.c_str()+8192, 14);
    TR << "original content 3rd page = " << std::string(bl1.c_str()+16384, 14);




    // open collection
    auto ch = open_collection_safe(cid);

    ghobject_t hoid(hobject_t(sobject_t("large object", CEPH_NOSNAP)));

    { std::cerr << "test: remove " << std::endl;

        // write a multi-page object
        ObjectStore::Transaction t;
        t.remove(cid, hoid);
        r = queue_transaction(store, ch, std::move(t));
    }

    { std::cerr << "test: multi-page write" << std::endl;

        // write a multi-page object
        ObjectStore::Transaction t;
        t.write(cid, hoid, 0, bl1.length(), bl1);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);

        list_objects_in_collection(store, ch);

        bufferlist out;
        r = store->read(ch, hoid, 0, bl1.length(), out);
        ASSERT_EQ(r, 16394);
        TR << "original " << std::string(bl1.c_str(), bl1.length());
        TR << "content  " << std::string(out.c_str(), out.length());
        ASSERT_EQ(ceph_str_hash_linux(bl1.c_str(), bl1.length()), ceph_str_hash_linux(out.c_str(), out.length()));
        TR << "read content = " << std::string(out.c_str()+16384, 10);
    }
/*
    { std::cerr << "test: partial write 0 ~ 4096" << std::endl;

        bufferlist bl2;
        bl2.append_zero(4096);
        // partial update 0 - 4096
        ObjectStore::Transaction t;
        t.write(cid, hoid, 0, 4096, bl2);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);

        bufferlist out;
        r = store->read(ch, hoid, 0, 4096, out);
        ASSERT_EQ(r, 4096);
        ASSERT_EQ(out.length(), 4096);
        ASSERT_EQ(ceph_str_hash_linux(bl1.c_str(), out.length()), ceph_str_hash_linux(out.c_str(), out.length()));

        bufferlist out2;
        r = store->read(ch, hoid, 4096, 8192, out2);
        ASSERT_EQ(r, 8192);
        ASSERT_EQ(out2.length(), 8192);
        ASSERT_EQ(ceph_str_hash_linux(bl1.c_str()+4096, out2.length()), ceph_str_hash_linux(out2.c_str(), out2.length()));
    }*/
    { std::cerr << "test: append 4B" << std::endl;

        int originallength = bl1.length();
        TR << "original hash = " << ceph_str_hash_linux(bl1.c_str(), bl1.length());
        TR << "original content = " << std::string(bl1.c_str()+16384, 14);

        bufferlist bl2;
        bl2.append("test");

        bufferlist out;
        r = store->read(ch, hoid, 0, bl1.length(), out);
        ASSERT_EQ(r, 16394);
        ASSERT_TRUE(ceph_str_hash_linux(out.c_str(), out.length()) != 0);
        TR << "original read hash = " << ceph_str_hash_linux(out.c_str(), out.length());
        TR << "original read content = " << std::string(out.c_str()+16384, 10);

        bl1.append(bl2);

        // append 4B
        ObjectStore::Transaction t;
        t.write(cid, hoid, originallength, 4, bl2);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);

        out.clear();
        r = store->read(ch, hoid, 0, bl1.length(), out);
        ASSERT_EQ(r, bl1.length());
        ASSERT_EQ(out.length(), bl1.length());
        TR << "written content = " << std::string(out.c_str()+16384, 14);


        /*for (int i =0 ;i < originallength; i++) {
            , originallength)
        }
*/
        ASSERT_EQ(ceph_str_hash_linux(bl1.c_str(), bl1.length()), ceph_str_hash_linux(out.c_str(), out.length()));
    }

    std::cerr << "done" << std::endl;

}





TEST_P(KvsStoreTest, BasicWriteCloneTest) {
  int r;
  coll_t cid(spg_t(pg_t(0, 2), shard_id_t(1)));
 
  auto ch = store->create_new_collection(cid);
   {
    cerr << "create collection" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP), "key", 123, -1, ""));
  bufferlist small;
  small.append("small");
  map<string,bufferlist> km;
  km["foo"] = small;
  km["bar"].append("asdfjkasdkjdfsjkafskjsfdj");
  bufferlist header;
  header.append("this is a header");
  {
    ObjectStore::Transaction t;
    header.clear();
   // t.touch(cid,hoid);
    t.write(cid, hoid, 0, header.length(), header);
//    t.omap_setkeys(cid, hoid, km);
//    t.omap_setheader(cid, hoid, header);
    cerr << "Creating object and set omap " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP), "key", 123, -1, ""));
  {
    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    cerr << "Clone object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    cerr << "Clone object -- return = " << r  << std::endl;
    ASSERT_EQ(r, 0);
  }

  {
   cerr << " Read the cloned object " << std::endl;
   bufferlist in;
   r = store->read(ch, hoid, 0, 0, in);
   ASSERT_EQ(header.length(), r);
   ASSERT_EQ(header, in.length());
   in.clear();
  // header.clear();
   cerr << " Cloned object read " << std::endl;
  }
#if 0
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
#endif

}


TEST_P(KvsStoreTest, QuickReadWriteTest){
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));// Added for iterator bug in FW
  bufferlist bl;
  bl.append("1234512345");
  int r;
  int NUM_OBJS = 1;
  int NUM_UPDATES = 1;
  set<ghobject_t> created;
  string base = "";
  //for (int i = 0; i < 20; ++i) base.append("a");
  auto ch = store->create_new_collection(cid);
  {
    cerr << "create collection" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  } 
 
  {
  
  cerr << " write objects to collection" << std::endl;

  for (int i = 0; i < NUM_OBJS; i++){
        cerr << "Object " << i << std::endl;
        
        char buf[100];
        snprintf(buf, sizeof(buf), "%d", i);
        
        bufferlist bl1;
        for (int i = 0; i<425; i++)
          bl1.append("12");

        ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));  
        created.insert(hoid);

        for (int k = 0; k < NUM_UPDATES; k++) {
            cerr << "[" << k << "]  Object = " << hoid << " writing " << bl1.length() << " bytes" << std::endl;
            
            ObjectStore::Transaction t;
            t.write(cid, hoid, 0, bl1.length(), bl1);
            r = queue_transaction(store, ch, std::move(t));
            ASSERT_EQ(r, 0);

            bufferlist in;
            r = store->read(ch, hoid, 0, bl1.length(), in);
            cerr << " read oid: " << hoid << ",  retcode: " << r
                 << " written bl length: " << in.length()
                 << " read bl length: " << bl1.length() 
                 << ", written hash: " << ceph_str_hash_linux(bl1.c_str(), bl1.length())
                 << ", read hash: " << ceph_str_hash_linux(in.c_str(), in.length())
                 << std::endl;

            ASSERT_EQ(r, bl1.length());
            ASSERT_EQ(r, in.length());
            ASSERT_EQ(ceph_str_hash_linux(in.c_str(), in.length()), ceph_str_hash_linux(bl1.c_str(), bl1.length()));
            ASSERT_EQ(bl1.length(),in.length());            
            ASSERT_TRUE(bl_eq(in, bl1));
           
            in.clear();
      }
        bl1.clear();
  }

  cerr << " ALL clear" << std::endl;
 
  }

}


TEST_P(KvsStoreTest, BasicCloneTest) {
  int r;
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));

  auto ch = open_collection_safe(cid);

  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP), "key", 123, -1, ""));
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object and set omap " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP), "key", 123, -1, ""));
  {
    ObjectStore::Transaction t;
    t.clone(cid, hoid, hoid2);
    cerr << "Clone object" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    cerr << "Clone object -- return = " << r  << std::endl;
    ASSERT_EQ(r, 0);
  }

  {
   cerr << " Read the cloned object " << std::endl;
   bufferlist in;
   r = store->read(ch, hoid, 0, 0, in);
   ASSERT_EQ(0, r);
   ASSERT_EQ(0, in.length());
   in.clear();
   cerr << " Cloned object read " << std::endl;
  }
 #if 0 
   {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.remove(cid, hoid2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
#endif  
}



#if 0
TEST_P(KvsStoreTest, SimpleWriteReadTest1){
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));// Added for iterator bug in FW
  bufferlist bl;
  bl.append("1234512345");
  int r;
  int NUM_OBJS = 1000;
  set<ghobject_t> created;
  string base = "";
  //for (int i = 0; i < 20; ++i) base.append("a");
  auto ch = store->create_new_collection(cid);
   {
    cerr << "create collection" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
  
  cerr << " write objects to collection" << std::endl;

  for (int i = 0; i < NUM_OBJS; i++){
        cerr << "Object " << i << std::endl;
        
        char buf[100];
        snprintf(buf, sizeof(buf), "%d", i);
        
        bufferlist bl1;
        for (int i = 0; i<425; i++)
          bl1.append("12");

        ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));  
        created.insert(hoid);
        
      
            
        ObjectStore::Transaction t;
        t.write(cid, hoid, 0, bl1.length(), bl1);
       // cerr << " Object " << i << ", hoid " << hoid << " writing"<< std::endl;
        r = queue_transaction(store, ch, std::move(t));
       // cerr << " Object " << i << ", hoid " << hoid << " write status r =  "<< r << std::endl;
        ASSERT_EQ(r, 0);
            

        cerr << " read object " << hoid << std::endl;
        bufferlist in;
        r = store->read(ch, hoid, 0, bl1.length(), in);
        cerr << " read oid: " << hoid 
              #if 0
                << ",  retcode: " << r
                 << " written bl length: " << in.length()
                 << " read bl length: " << bl1.length() 
                 << ", written hash: " << ceph_str_hash_linux(bl1.c_str(), bl1.length())
                 << ", read hash: " << ceph_str_hash_linux(in.c_str(), in.length())
               #endif  
                 << std::endl;
        ASSERT_EQ(bl1.length(),r);
        ASSERT_EQ(bl1.length(),in.length());
        ASSERT_EQ(ceph_str_hash_linux(bl1.c_str(), bl1.length()), ceph_str_hash_linux(in.c_str(), in.length()));
        in.clear();
        bl1.clear();
  }
  //cerr << " ALL objects written to collection" << std::endl;
 
  }


  {
    //cerr << " reading all objects " << std::endl;
   /*
    for(set<ghobject_t>::iterator hoid_iter = created.begin();
         hoid_iter != created.end();
         ++hoid_iter) 
    {
        bufferlist in;
        cerr << " Object = " << *hoid_iter << std::endl;
        r = store->read(ch, *hoid_iter, 0, bl.length(), in);
        cerr << " read object length = " << r << std::endl;
       // ASSERT_EQ(bl.length(), r);
        in.clear();
    }
    */
  }

}



TEST_P(KvsStoreTest, WriteTransactionTest){
    coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));// Added for iterator bug in FW
    bufferlist bl1;
    bl1.append_zero(4096);
    // open collection
    auto ch = open_collection_safe(cid);

    for (int i =0 ; i < 10; i++) {
        ObjectStore::Transaction t;
        for (int j = 0 ;j < 10; j++) {
            ghobject_t hoid(hobject_t(sobject_t("o" + std::to_string(i)+ "#"+ std::to_string(j), CEPH_NOSNAP)));
            t.write(cid, hoid, 0, bl1.length(), bl1);
        }
        int r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
    }

}



TEST_P(KvsStoreTest, MergeCollection) {
    int common_suffix_size = 5;
    coll_t cid1(spg_t(pg_t(0,52),shard_id_t::NO_SHARD));
    coll_t cid2(spg_t(pg_t(1<<common_suffix_size,52),shard_id_t::NO_SHARD));
    auto c1 = open_collection_safe(cid1);
    auto c2 = open_collection_safe(cid2);

    std::function<void(int, int)> cleanup = [&] (int r, int e) {
        if (r != e)
            std::cerr << "assert failure " << r << " != " << e << std::endl;
        {
            ObjectStore::Transaction t;
            t.remove_collection(cid1);
            r = queue_transaction(store, c1, std::move(t));
        }
        {
            ObjectStore::Transaction t;
            t.remove_collection(cid2);
            r = queue_transaction(store, c2, std::move(t));
        }

    };

    cleanup(0,0);
    TR << "a1" ;
    {
        ObjectStore::Transaction t;
        t.create_collection(cid1, common_suffix_size);
        int r = queue_transaction(store, c1, std::move(t));

        assert_eq(r, 0, cleanup);
    }
    TR << "a2" ;
    {
        ObjectStore::Transaction t;
        t.create_collection(cid2, common_suffix_size);
        int r = queue_transaction(store, c2, std::move(t));

        assert_eq(r, 0, cleanup);
    }
    TR << "a3" ;
    {
        ObjectStore::Transaction t;
        t.merge_collection(cid1, cid2, common_suffix_size);
        int r = queue_transaction(store, c1, std::move(t));
        assert_eq(r, 0, cleanup);
    }
    TR << "a4" ;
    cleanup(0,0);
}

TEST_P(KvsStoreTest, MergeCollectionTest){
    int num_objects = 1;
    int common_suffix_size = 5;

    coll_t cid(spg_t(pg_t(0,52),shard_id_t::NO_SHARD));
    coll_t tid(spg_t(pg_t(1<<common_suffix_size,52),shard_id_t::NO_SHARD));
    auto ch = open_collection_safe(cid);
    auto tch = open_collection_safe(tid);
    int r = 0;
    {
        cerr << " create Collection 1" << std::endl;
        ObjectStore::Transaction t;
        t.create_collection(cid, common_suffix_size);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
        cerr << " created Collection 1" << std::endl;
    }
    bufferlist small;
    small.append("small");

// Write objects to collection 1
    {
        ObjectStore::Transaction t;
        //for (uint32_t i = 0; i < num_objects; ++i) {
        stringstream objname;
        objname << "obja" << 0;
        ghobject_t hoid_a(hobject_t(objname.str(), "", CEPH_NOSNAP, 0, 52, ""));
        t.write(cid, hoid_a, 0, small.length(), small, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);

        // check for object
        bufferlist in;
        r = store->read(ch, hoid_a, 0, small.length(), in);
        cerr << " read oid: " << hoid_a << ",  retcode: " << r
             << " written bl length: " << in.length()
             << " read bl length: " << small.length()
             << ", written hash: " << ceph_str_hash_linux(small.c_str(), small.length())
             << ", read hash: " << ceph_str_hash_linux(in.c_str(), in.length())
             << std::endl;

        ASSERT_EQ(r, small.length());
        ASSERT_EQ(r, in.length());
        ASSERT_EQ(ceph_str_hash_linux(in.c_str(), in.length()), ceph_str_hash_linux(small.c_str(), small.length()));
        ASSERT_EQ(small.length(),in.length());
        ASSERT_TRUE(bl_eq(in, small));

        in.clear();
        // }
    }

//Collection 2
    {
        cerr << " create Collection 2 " << std::endl;
        ObjectStore::Transaction t;
        t.create_collection(tid, common_suffix_size + 1);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
        cerr << " created Collection 2" << std::endl;
    }


// Write objects to collection 2
    {
        ObjectStore::Transaction t;
        stringstream objname1;
        objname1 << "objb" << 1;
        ghobject_t hoid_b(hobject_t( objname1.str(), "", CEPH_NOSNAP, 1<<common_suffix_size, 52, ""));
        t.write(tid, hoid_b, 0, small.length(), small, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
        r = queue_transaction(store, tch, std::move(t));
        ASSERT_EQ(r, 0);
        cerr << " object 2 written " << std::endl;
        // check for object
        bufferlist in;
        r = store->read(tch, hoid_b, 0, small.length(), in);
        cerr << " read oid: " << hoid_b << ",  retcode: " << r
             << " written bl length: " << in.length()
             << " read bl length: " << small.length()
             << ", written hash: " << ceph_str_hash_linux(small.c_str(), small.length())
             << ", read hash: " << ceph_str_hash_linux(in.c_str(), in.length())
             << std::endl;

        ASSERT_EQ(r, small.length());
        ASSERT_EQ(r, in.length());
        ASSERT_EQ(ceph_str_hash_linux(in.c_str(), in.length()), ceph_str_hash_linux(small.c_str(), small.length()));
        ASSERT_EQ(small.length(),in.length());
        ASSERT_TRUE(bl_eq(in, small));

        in.clear();
        //}
    }

// Merge them tid->cid
    {
        ObjectStore::Transaction t;
        t.merge_collection(tid, cid, common_suffix_size);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
        cerr << " Merge Collection done 2->1" << std::endl;
    }

// check for final merged
    {
        ObjectStore::Transaction t;
        vector<ghobject_t> objects;
        r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
        cerr << " objects.size = " << objects.size() << std::endl;
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
        t.remove_collection(cid);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
    }
}


template<typename T, typename O>
inline void check_objects_in_collection(O &store, T ch, int expected, const std::string &name){
    vector<ghobject_t> objects;
    int r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    for (vector<ghobject_t>::iterator i = objects.begin(); i != objects.end(); ++i) {
        TR << name << " - found: " << (*i) ;
    }
    if (expected != -1) {
        ASSERT_EQ(objects.size(), expected);
    }
}
#endif
#if 0
TEST_P(KvsStoreTest, MergeCollectionTestVariation){

  int r;
  unsigned base = 0xf;
  unsigned bits = 4;

  coll_t a(spg_t(pg_t(base, 0), shard_id_t::NO_SHARD));
  coll_t b(spg_t(pg_t(base | (1<<bits), 0), shard_id_t::NO_SHARD));

  auto cha = store->create_new_collection(a);
  auto chb = store->create_new_collection(b);
  {
    ObjectStore::Transaction t;
    t.create_collection(a, bits + 1);
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(b, bits + 1);
    r = queue_transaction(store, chb, std::move(t));
    ASSERT_EQ(r, 0);
  }

  bufferlist small;
  small.append("small");
  string suffix = "ooaaaaa";
  set<ghobject_t> aobjects, bobjects;
  {
    // fill a
    ObjectStore::Transaction t;
    for (unsigned i = 0; i < 1; ++i) {
      string objname = "a" + stringify(i) + suffix;
      ghobject_t o(hobject_t(
         objname,
         "",
         CEPH_NOSNAP,
         i<<(bits+1) | base,
         52, ""));
      aobjects.insert(o);
      t.write(a, o, 0, small.length(), small, 0);
    if (i % 100) {
  r = queue_transaction(store, cha, std::move(t));
  ASSERT_EQ(r, 0);
  t = ObjectStore::Transaction();
     }
    }
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // fill b
    ObjectStore::Transaction t;
    for (unsigned i = 0; i < 1; ++i) {
      string objname = "b" + stringify(i) + suffix;
      ghobject_t o(hobject_t(
         objname,
         "",
         CEPH_NOSNAP,
         (i<<(base+1)) | base | (1<<bits),
         52, ""));
      bobjects.insert(o);
      t.write(b, o, 0, small.length(), small, 0);
      if (i % 100) {
  r = queue_transaction(store, chb, std::move(t));
  ASSERT_EQ(r, 0);
  t = ObjectStore::Transaction();
      }
    }
    r = queue_transaction(store, chb, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // merge b->a
  {
    ObjectStore::Transaction t;
    t.merge_collection(b, a, bits);
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }

  // verify
  {
    vector<ghobject_t> got;
    store->collection_list(cha, ghobject_t(), ghobject_t::get_max(), INT_MAX,
         &got, 0);
    set<ghobject_t> gotset;
    for (auto& o : got) {
      ASSERT_TRUE(aobjects.count(o) || bobjects.count(o));
      gotset.insert(o);
    }
  }

  // clean up
  {
    ObjectStore::Transaction t;
    for (auto &o : aobjects) {
      t.remove(a, o);
    }
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    for (auto &o : bobjects) {
      t.remove(a, o);
    }
    t.remove_collection(a);
    r = queue_transaction(store, cha, std::move(t));
    ASSERT_EQ(r, 0);
  }

}



TEST_P(KvsStoreTest, MergeCollection) {
    int common_suffix_size = 5;
    coll_t cid1(spg_t(pg_t(0,52),shard_id_t::NO_SHARD));
    coll_t cid2(spg_t(pg_t(1<<common_suffix_size,52),shard_id_t::NO_SHARD));
    auto c1 = store->create_new_collection(cid1);
    auto c2 = store->create_new_collection(cid2);

    std::function<void(int, int)> cleanup = [&] (int r, int e) {
        if (r != e)
            std::cerr << "assert failure " << r << " != " << e << std::endl;
        {
            ObjectStore::Transaction t;
            t.remove_collection(cid1);
            r = queue_transaction(store, c1, std::move(t));
        }
        {
            ObjectStore::Transaction t;
            t.remove_collection(cid2);
            r = queue_transaction(store, c2, std::move(t));
        }

    };

    cleanup(0,0);
    TR << "a1" ;
    {
        ObjectStore::Transaction t;
        t.create_collection(cid1, common_suffix_size);
        int r = queue_transaction(store, c1, std::move(t));

        assert_eq(r, 0, cleanup);
    }
    TR << "a2" ;
    {
        ObjectStore::Transaction t;
        t.create_collection(cid2, common_suffix_size);
        int r = queue_transaction(store, c2, std::move(t));

        assert_eq(r, 0, cleanup);
    }
    TR << "a3" ;
    {
        ObjectStore::Transaction t;
        t.merge_collection(cid1, cid2, common_suffix_size);
        int r = queue_transaction(store, c1, std::move(t));
        assert_eq(r, 0, cleanup);
    }
    TR << "a4" ;
    cleanup(0,0);
}
#endif


template<typename T, typename O>
inline void check_objects_in_collection(O &store, T ch, int expected, const std::string &name){
    vector<ghobject_t> objects;
    int r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    for (vector<ghobject_t>::iterator i = objects.begin(); i != objects.end(); ++i) {
        TR << name << " - found: " << (*i) ;
    }
    if (expected != -1) {
        ASSERT_EQ(objects.size(), expected);
    }
}

TEST_P(KvsStoreTest, MergeSplitCollection) {
    int num_objects = 10;
    int common_suffix_size = 5;
    coll_t cid(spg_t(pg_t(0, 52), shard_id_t::NO_SHARD));
    coll_t tid(spg_t(pg_t(1 << common_suffix_size, 52), shard_id_t::NO_SHARD));
    auto ch = open_collection_safe(cid);
    auto tch =open_collection_safe(tid);

    // create collection
    int r = 0;
    {
        ObjectStore::Transaction t;
        t.create_collection(cid, common_suffix_size);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
    }

    // create 2 * num_objects
    {
        bufferlist small;
        small.append("small");
        ObjectStore::Transaction t;
        for (int i = 0; i < 2 * num_objects; ++i) {
            stringstream objname;
            objname << "obj" << i;
            ghobject_t a(hobject_t(
                    objname.str(),
                    "",
                    CEPH_NOSNAP,
                    0,
                    52, ""));
            t.write(cid, a, 0, small.length(), small,
                    CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);

            if (i % 100) {
                r = queue_transaction(store, ch, std::move(t));
                ASSERT_EQ(r, 0);
                t = ObjectStore::Transaction();
            }
        }
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
    }
    ch->flush();

    check_objects_in_collection(store, ch, num_objects*2, "after write" );

    // split collections
    {
        ObjectStore::Transaction t;
        t.create_collection(tid, common_suffix_size + 1);
        t.split_collection(cid, common_suffix_size + 1, 1 << common_suffix_size, tid);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
    }
    ch->flush();

    check_objects_in_collection(store, ch, -1, "split-ch" );
    check_objects_in_collection(store, tch,-1, "split-tch");

    // merge them again!
    {
        ObjectStore::Transaction t;
        t.merge_collection(tid, cid, common_suffix_size);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
    }

    check_objects_in_collection(store, ch, num_objects*2, "merge-ch" );

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


TEST_P(KvsStoreTest, OpenCollectionTest) {
    coll_t cid;
    ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
    ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
    bufferlist bl;
    bl.append("1234512345");
    int r;
    auto ch = open_collection_safe(cid);
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

    ch = open_collection_safe(cid);
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

TEST_P(KvsStoreTest, RemoveCollection) {
    coll_t cid(spg_t(pg_t(0, 1), shard_id_t(2)));// Added for iterator bug in FW
    ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
    ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
    bufferlist bl;
    bl.append("1234512345");
    int r;
    auto ch = open_collection_safe(cid);
    {
        cerr << "create collection + write" << std::endl;
        ObjectStore::Transaction t;
        t.create_collection(cid, 0);
        t.write(cid, hoid, 0, bl.length(), bl);
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
}


TEST_P(KvsStoreTest, Trivial) {
}

TEST_P(KvsStoreTest, TrivialRemount) {
  int r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
}


TEST_P(KvsStoreTest, QuickReadWriteTest1){
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));// Added for iterator bug in FW
  bufferlist bl;
  bl.append("1234512345");
  int r;
  int NUM_OBJS = 1;
  int NUM_UPDATES = 1;
  set<ghobject_t> created;
  string base = "";
  //for (int i = 0; i < 20; ++i) base.append("a");
  auto ch = open_collection_safe(cid);
  {
    cerr << "create collection" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  } 
 
  {
  
  cerr << " write objects to collection" << std::endl;

  for (int i = 0; i < NUM_OBJS; i++){
        cerr << "Object " << i << std::endl;
        
        char buf[100];
        snprintf(buf, sizeof(buf), "%d", i);
        
        bufferlist bl1;
        for (int i = 0; i<425; i++)
          bl1.append("12");

        ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));  
        created.insert(hoid);

        for (int k = 0; k < NUM_UPDATES; k++) {
            cerr << "[" << k << "]  Object = " << hoid << " writing " << bl1.length() << " bytes" << std::endl;
            
            ObjectStore::Transaction t;
            t.write(cid, hoid, 0, bl1.length(), bl1);
            r = queue_transaction(store, ch, std::move(t));
            ASSERT_EQ(r, 0);

            bufferlist in;
            r = store->read(ch, hoid, 0, bl1.length(), in);
            cerr << " read oid: " << hoid << ",  retcode: " << r
                 << " written bl length: " << in.length()
                 << " read bl length: " << bl1.length() 
                 << ", written hash: " << ceph_str_hash_linux(bl1.c_str(), bl1.length())
                 << ", read hash: " << ceph_str_hash_linux(in.c_str(), in.length())
                 << std::endl;

            ASSERT_EQ(r, bl1.length());
            ASSERT_EQ(r, in.length());
            ASSERT_EQ(ceph_str_hash_linux(in.c_str(), in.length()), ceph_str_hash_linux(bl1.c_str(), bl1.length()));
            ASSERT_EQ(bl1.length(),in.length());            
            ASSERT_TRUE(bl_eq(in, bl1));
           
            in.clear();
      }
        bl1.clear();
  }

  cerr << " ALL clear" << std::endl;
 
  }

}





TEST_P(KvsStoreTest, SimpleRemount) {
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(2)));// Added for iterator bug in FW
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t hoid2(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  bufferlist bl;
  bl.append("1234512345");
  int r;
  auto ch = open_collection_safe(cid);
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

  ch = open_collection_safe(cid);
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

TEST_P(KvsStoreTest, IORemount) {
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(3)));// Added for iterator bug in FW
  bufferlist bl;
  bl.append("1234512345");
  int r;

  auto ch = open_collection_safe(cid);
  {
    cerr << "create collection + objects" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    for (int n=1; n<=100; ++n) {
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.write(cid, hoid, 0, bl.length(), bl);
    }
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  // overwrites
  {
    cout << "overwrites" << std::endl;
    for (int n=1; n<=100; ++n) {
      ObjectStore::Transaction t;
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.write(cid, hoid, 1, bl.length(), bl);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
  }
  ch.reset();
  r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
  {
    ObjectStore::Transaction t;
    for (int n=1; n<=100; ++n) {
      ghobject_t hoid(hobject_t(sobject_t("Object " + stringify(n), CEPH_NOSNAP)));
      t.remove(cid, hoid);
    }
    t.remove_collection(cid);
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, UnprintableCharsName) {
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(4)));// Added for iterator bug in FW
  string name = "funnychars_";
  for (unsigned i = 0; i < 150; ++i) {
    name.push_back(i);
  }
  ghobject_t oid(hobject_t(sobject_t(name, CEPH_NOSNAP)));
  int r;
  auto ch = open_collection_safe(cid);
  {
    cerr << "create collection + object" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ch.reset();
  r = store->umount();
  ASSERT_EQ(0, r);
  r = store->mount();
  ASSERT_EQ(0, r);
  {
    cout << "removing" << std::endl;
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, SimpleMetaColTest) {
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(5)));// Added for iterator bug in FW
  int r = 0;
  {

    auto ch = open_collection_safe(cid);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "create collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    auto ch = open_collection_safe(cid);
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "add collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


TEST_P(KvsStoreTest, SimplePGColTest) {
  coll_t cid(spg_t(pg_t(1,2), shard_id_t::NO_SHARD));
  int r = 0;
  {
    ObjectStore::Transaction t;
    auto ch = open_collection_safe(cid);
    t.create_collection(cid, 4);
    cerr << "create collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 4);
    cerr << "add collection" << std::endl;
    auto ch = open_collection_safe(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    auto ch = store->open_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, SimpleColPreHashTest) {
  // Firstly we will need to revert the value making sure
  // collection hint actually works
  int merge_threshold = g_ceph_context->_conf->filestore_merge_threshold;
  std::ostringstream oss;
  if (merge_threshold > 0) {
    oss << "-" << merge_threshold;
    SetVal(g_conf(), "filestore_merge_threshold", oss.str().c_str());
  }

  uint32_t pg_num = 128;

  boost::uniform_int<> pg_id_range(0, pg_num);
  gen_type rng(time(NULL));
  int pg_id = pg_id_range(rng);

  int objs_per_folder = abs(merge_threshold) * 16 * g_ceph_context->_conf->filestore_split_multiple;
  boost::uniform_int<> folders_range(5, 256);
  uint64_t expected_num_objs = (uint64_t)objs_per_folder * (uint64_t)folders_range(rng);

  coll_t cid(spg_t(pg_t(pg_id, 15), shard_id_t::NO_SHARD));
  int r;
  auto ch = open_collection_safe(cid);
  {
    // Create a collection along with a hint
    ObjectStore::Transaction t;
    t.create_collection(cid, 5);
    cerr << "create collection" << std::endl;
    bufferlist hint;
    encode(pg_num, hint);
    encode(expected_num_objs, hint);
    t.collection_hint(cid, ObjectStore::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS, hint);
    cerr << "collection hint" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    // Remove the collection
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    cerr << "remove collection" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


TEST_P(KvsStoreTest, SmallBlockWrites) {
  int r;
  coll_t cid;
  auto ch = open_collection_safe(cid);
  ghobject_t hoid(hobject_t(sobject_t("foo", CEPH_NOSNAP)));
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist a;
  bufferptr ap(0x1000);
  memset(ap.c_str(), 'a', 0x1000);
  a.append(ap);
  bufferlist b;
  bufferptr bp(0x1000);
  memset(bp.c_str(), 'b', 0x1000);
  b.append(bp);
  bufferlist c;
  bufferptr cp(0x1000);
  memset(cp.c_str(), 'c', 0x1000);
  c.append(cp);
  bufferptr zp(0x1000);
  zp.zero();
  bufferlist z;
  z.append(zp);
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, 0x1000, a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x1000, r);
    exp.append(a);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0x1000, 0x1000, b);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x2000, r);
    exp.append(a);
    exp.append(b);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0x3000, 0x1000, c);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x4000, r);
    exp.append(a);
    exp.append(b);
    exp.append(z);
    exp.append(c);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0x2000, 0x1000, a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x4000, r);
    exp.append(a);
    exp.append(b);
    exp.append(a);
    exp.append(c);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    t.write(cid, hoid, 0, 0x1000, c);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist in, exp;
    r = store->read(ch, hoid, 0, 0x4000, in);
    ASSERT_EQ(0x4000, r);
    exp.append(c);
    exp.append(b);
    exp.append(a);
    exp.append(c);
    ASSERT_TRUE(bl_eq(exp, in));
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


TEST_P(KvsStoreTest, BufferCacheReadTest) {
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    auto ch = store->open_collection(cid);
    ASSERT_FALSE(ch);
  }
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);

    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    exists = store->exists(ch, hoid);
    ASSERT_EQ(true, exists);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append("abcde");
    t.write(cid, hoid, 0, 5, bl);
    t.write(cid, hoid, 10, 5, bl);
    cerr << "TwinWrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 15, newdata);
    ASSERT_EQ(r, 15);
    {
      bufferlist expected;
      expected.append(bl);
      expected.append_zero(5);
      expected.append(bl);
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  //overwrite over the same extents
  {
    ObjectStore::Transaction t;
    bufferlist bl, newdata;
    bl.append("edcba");
    t.write(cid, hoid, 0, 5, bl);
    t.write(cid, hoid, 10, 5, bl);
    cerr << "TwinWrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 15, newdata);
    ASSERT_EQ(r, 15);
    {
      bufferlist expected;
      expected.append(bl);
      expected.append_zero(5);
      expected.append(bl);
      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  //additional write to an unused region of some blob
  {
    ObjectStore::Transaction t;
    bufferlist bl2, newdata;
    bl2.append("1234567890");

    t.write(cid, hoid, 20, bl2.length(), bl2);
    cerr << "Append" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 30, newdata);
    ASSERT_EQ(r, 30);
    {
      bufferlist expected;
      expected.append("edcba");
      expected.append_zero(5);
      expected.append("edcba");
      expected.append_zero(5);
      expected.append(bl2);

      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
  //additional write to an unused region of some blob and partial owerite over existing extents
  {
    ObjectStore::Transaction t;
    bufferlist bl, bl2, bl3, newdata;
    bl.append("DCB");
    bl2.append("1234567890");
    bl3.append("BA");

    t.write(cid, hoid, 30, bl2.length(), bl2);
    t.write(cid, hoid, 1, bl.length(), bl);
    t.write(cid, hoid, 13, bl3.length(), bl3);
    cerr << "TripleWrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    r = store->read(ch, hoid, 0, 40, newdata);
    ASSERT_EQ(r, 40);
    {
      bufferlist expected;
      expected.append("eDCBa");
      expected.append_zero(5);
      expected.append("edcBA");
      expected.append_zero(5);
      expected.append(bl2);
      expected.append(bl2);

      ASSERT_TRUE(bl_eq(expected, newdata));
    }
  }
}


TEST_P(KvsStoreTest, SimpleObjectTest) {
  int r;
  coll_t cid;
  ghobject_t hoid(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  {
    auto ch = store->open_collection(cid);
    ASSERT_FALSE(ch);
  }
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bool exists = store->exists(ch, hoid);
    ASSERT_TRUE(!exists);

    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    exists = store->exists(ch, hoid);
    ASSERT_EQ(true, exists);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    t.touch(cid, hoid);
    cerr << "Remove then create" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, orig;
    bl.append("abcde");
    orig = bl;
    t.remove(cid, hoid);
    t.write(cid, hoid, 0, 5, bl);
    cerr << "Remove then create" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, 5, in);
    ASSERT_EQ(5, r);
    ASSERT_TRUE(bl_eq(orig, in));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, exp;
    bl.append("abcde");
    exp = bl;
    exp.append(bl);
    t.write(cid, hoid, 5, 5, bl);
    cerr << "Append" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl, exp;
    bl.append("abcdeabcde");
    exp = bl;
    t.write(cid, hoid, 0, 10, bl);
    cerr << "Full overwrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("abcde");
    t.write(cid, hoid, 3, 5, bl);
    cerr << "Partial overwrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in, exp;
    exp.append("abcabcdede");
    r = store->read(ch, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    in.hexdump(cout);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    {
      ObjectStore::Transaction t;
      bufferlist bl;
      bl.append("fghij");
      t.truncate(cid, hoid, 0);
      t.write(cid, hoid, 5, 5, bl);
      cerr << "Truncate + hole" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    {
      ObjectStore::Transaction t;
      bufferlist bl;
      bl.append("abcde");
      t.write(cid, hoid, 0, 5, bl);
      cerr << "Reverse fill-in" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }

    bufferlist in, exp;
    exp.append("abcdefghij");
    r = store->read(ch, hoid, 0, 10, in);
    ASSERT_EQ(10, r);
    in.hexdump(cout);
    ASSERT_TRUE(bl_eq(exp, in));
  }
  {
    ObjectStore::Transaction t;
    bufferlist bl;
    bl.append("abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234");
    t.write(cid, hoid, 0, bl.length(), bl);
    cerr << "larger overwrite" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

    bufferlist in;
    r = store->read(ch, hoid, 0, bl.length(), in);
    ASSERT_EQ((int)bl.length(), r);
    in.hexdump(cout);
    ASSERT_TRUE(bl_eq(bl, in));
  }
  {
    bufferlist bl;
    bl.append("abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234abcde01234012340123401234");

    //test: offset=len=0 mean read all data
    bufferlist in;
    r = store->read(ch, hoid, 0, 0, in);
    ASSERT_EQ((int)bl.length(), r);
    in.hexdump(cout);
    ASSERT_TRUE(bl_eq(bl, in));
  }
  {
    //verifying unaligned csums
    std::string s1("1"), s2(0x1000, '2'), s3("00");
    {
      ObjectStore::Transaction t;
      bufferlist bl;
      bl.append(s1);
      bl.append(s2);
      t.truncate(cid, hoid, 0);
      t.write(cid, hoid, 0x1000-1, bl.length(), bl);
      cerr << "Write unaligned csum, stage 1" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }

    bufferlist in, exp1, exp2, exp3;
    exp1.append(s1);
    exp2.append(s2);
    exp3.append(s3);
    r = store->read(ch, hoid, 0x1000-1, 1, in);
    ASSERT_EQ(1, r);
    ASSERT_TRUE(bl_eq(exp1, in));
    in.clear();
    r = store->read(ch, hoid, 0x1000, 0x1000, in);
    ASSERT_EQ(0x1000, r);
    ASSERT_TRUE(bl_eq(exp2, in));

    {
      ObjectStore::Transaction t;
      bufferlist bl;
      bl.append(s3);
      t.write(cid, hoid, 1, bl.length(), bl);
      cerr << "Write unaligned csum, stage 2" << std::endl;
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    in.clear();
    r = store->read(ch, hoid, 1, 2, in);
    ASSERT_EQ(2, r);
    ASSERT_TRUE(bl_eq(exp3, in));
    in.clear();
    r = store->read(ch, hoid, 0x1000-1, 1, in);
    ASSERT_EQ(1, r);
    ASSERT_TRUE(bl_eq(exp1, in));
    in.clear();
    r = store->read(ch, hoid, 0x1000, 0x1000, in);
    ASSERT_EQ(0x1000, r);
    ASSERT_TRUE(bl_eq(exp2, in));

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


TEST_P(KvsStoreTest, ManySmallWrite) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  ghobject_t b(hobject_t(sobject_t("Object 2", CEPH_NOSNAP)));
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bufferptr bp(4096);
  bp.zero();
  bl.append(bp);
  for (int i=0; i<100; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, a, i*4096, 4096, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int i=0; i<100; ++i) {
    ObjectStore::Transaction t;
    t.write(cid, b, (rand() % 1024)*4096, 4096, bl, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove(cid, b);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


TEST_P(KvsStoreTest, MultiSmallWriteSameBlock) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  bufferlist bl;
  bl.append("short");
  C_SaferCond c, d;
  // touch same block in both same transaction, tls, and pipelined txns
  {
    ObjectStore::Transaction t, u;
    t.write(cid, a, 0, 5, bl, 0);
    t.write(cid, a, 5, 5, bl, 0);
    t.write(cid, a, 4094, 5, bl, 0);
    t.write(cid, a, 9000, 5, bl, 0);
    u.write(cid, a, 10, 5, bl, 0);
    u.write(cid, a, 7000, 5, bl, 0);
    t.register_on_commit(&c);
    vector<ObjectStore::Transaction> v = {t, u};
    store->queue_transactions(ch, v);
  }
  {
    ObjectStore::Transaction t, u;
    t.write(cid, a, 40, 5, bl, 0);
    t.write(cid, a, 45, 5, bl, 0);
    t.write(cid, a, 4094, 5, bl, 0);
    t.write(cid, a, 6000, 5, bl, 0);
    u.write(cid, a, 610, 5, bl, 0);
    u.write(cid, a, 11000, 5, bl, 0);
    t.register_on_commit(&d);
    vector<ObjectStore::Transaction> v = {t, u};
    store->queue_transactions(ch, v);
  }
  c.wait();
  d.wait();
  {
    bufferlist bl2;
    r = store->read(ch, a, 0, 16000, bl2);
    ASSERT_GE(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, SmallSkipFront) {
  int r;
  coll_t cid;
  ghobject_t a(hobject_t(sobject_t("Object 1", CEPH_NOSNAP)));
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, a);
    t.truncate(cid, a, 3000);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    bufferptr bp(4096);
    memset(bp.c_str(), 1, 4096);
    bl.append(bp);
    ObjectStore::Transaction t;
    t.write(cid, a, 4096, 4096, bl);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    bufferlist bl;
    ASSERT_EQ(8192, store->read(ch, a, 0, 8192, bl));
    for (unsigned i=0; i<4096; ++i)
      ASSERT_EQ(0, bl[i]);
    for (unsigned i=4096; i<8192; ++i)
      ASSERT_EQ(1, bl[i]);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, a);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


TEST_P(KvsStoreTest, OMapIterator) {
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  int count = 0;
  auto ch = open_collection_safe(cid);
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



TEST_P(KvsStoreTest, SimpleCloneRangeTest) {
  int r;
  coll_t cid;
  auto ch = open_collection_safe(cid);
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

TEST_P(KvsStoreTest, OmapSimple) {
  int r;
  coll_t cid;
  auto ch = open_collection_safe(cid);
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
  auto ch = open_collection_safe(cid);
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
  auto ch = open_collection_safe(cid);
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


TEST_P(KvsStoreTest, SimpleObjectLongnameTest) {
  int r;
  coll_t cid;
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ghobject_t hoid(hobject_t(sobject_t("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaObjectaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1" , CEPH_NOSNAP)));
 
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
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

ghobject_t generate_long_name(unsigned i)
{
  stringstream name;
  name << "object id " << i << " ";
  for (unsigned j = 0; j < 200; ++j) name << 'a';
  ghobject_t hoid(hobject_t(sobject_t(name.str(), CEPH_NOSNAP)));
  hoid.hobj.set_hash(i % 2);
  return hoid;
}

TEST_P(KvsStoreTest, LongnameSplitTest) {
  int r;
  coll_t cid;
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    cerr << "Creating collection " << cid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }
  for (unsigned i = 0; i < 320; ++i) {
    ObjectStore::Transaction t;
    ghobject_t hoid = generate_long_name(i);
    t.touch(cid, hoid);
    cerr << "Creating object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }

  ghobject_t test_obj = generate_long_name(319);
  ghobject_t test_obj_2 = test_obj;
  test_obj_2.generation = 0;
  {
    ObjectStore::Transaction t;
    // should cause a split
    t.collection_move_rename(
      cid, test_obj,
      cid, test_obj_2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }

  for (unsigned i = 0; i < 319; ++i) {
    ObjectStore::Transaction t;
    ghobject_t hoid = generate_long_name(i);
    t.remove(cid, hoid);
    cerr << "Removing object " << hoid << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, test_obj_2);
    t.remove_collection(cid);
    cerr << "Cleaning" << std::endl;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(0, r);
  }

}


TEST_P(KvsStoreTest, ManyObjectTest) {
  int NUM_OBJS = 2000;
  int r = 0;
  coll_t cid;
  string base = "";
  for (int i = 0; i < 100; ++i) base.append("aaaaa");
  set<ghobject_t> created;
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  for (int i = 0; i < NUM_OBJS; ++i) {
    if (!(i % 5)) {
      cerr << "Object " << i << std::endl;
    }
    ObjectStore::Transaction t;
    char buf[100];
    snprintf(buf, sizeof(buf), "%d", i);
    ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));
    t.touch(cid, hoid);
    created.insert(hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    struct stat buf;
    ASSERT_TRUE(!store->stat(ch, *i, &buf));
  }

  set<ghobject_t> listed, listed2;
  vector<ghobject_t> objects;
  r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);

  cerr << "objects.size() is " << objects.size() << std::endl;
  for (vector<ghobject_t> ::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    listed.insert(*i);
    ASSERT_TRUE(created.count(*i));
  }
  ASSERT_TRUE(listed.size() == created.size());

  ghobject_t start, next;
  objects.clear();
  r = store->collection_list(
    ch,
    ghobject_t::get_max(),
    ghobject_t::get_max(),
    50,
    &objects,
    &next
    );
  ASSERT_EQ(r, 0);
  ASSERT_TRUE(objects.empty());

  objects.clear();
  listed.clear();
  ghobject_t start2, next2;
  while (1) {
    r = store->collection_list(ch, start, ghobject_t::get_max(),
             50,
             &objects,
             &next);
    ASSERT_TRUE(sorted(objects));
    ASSERT_EQ(r, 0);
    listed.insert(objects.begin(), objects.end());
    if (objects.size() < 50) {
      ASSERT_TRUE(next.is_max());
      break;
    }
    objects.clear();

    start = next;
  }
  cerr << "listed.size() is " << listed.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  if (listed2.size()) {
    ASSERT_EQ(listed.size(), listed2.size());
  }
  for (set<ghobject_t>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  cerr << "cleaning up" << std::endl;
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, Synthetic) {
  
  cerr << " kvsstoreTest == > Synthetic test start < == " << std::endl;
  doSyntheticTest(store, 10000, 400*1024, 40*1024, 0);
  cerr << " kvsstoreTest == > Synthetic test completed < == " << std::endl;
}

TEST_P(KvsStoreTest, AttrSynthetic){
  MixedGenerator gen(447);
  gen_type rng(time(NULL));
  coll_t cid(spg_t(pg_t(0,447),shard_id_t::NO_SHARD));

  SyntheticWorkloadState test_obj(store.get(), &gen, &rng, cid, 40*1024, 4*1024, 0);
  test_obj.init();
  for (int i = 0; i < 500; ++i) {
    if (!(i % 10)) cerr << "seeding object " << i << std::endl;
    test_obj.touch();
  }
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 100)) {
      cerr << "Op " << i << std::endl;
      test_obj.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 97) {
      test_obj.scan();
    } else if (val > 93) {
      test_obj.stat();
    } else if (val > 75) {
      test_obj.rmattr();
    } else if (val > 47) {
      test_obj.setattrs();
    } else if (val > 45) {
      test_obj.clone();
    } else if (val > 37) {
      test_obj.stash();
    } else if (val > 30) {
      test_obj.getattrs();
    } else {
      test_obj.getattr();
    }
  }
  test_obj.wait_for_done();
  test_obj.shutdown();
}


TEST_P(KvsStoreTest, HashCollisionTest) {
  int64_t poolid = 11;
  coll_t cid(spg_t(pg_t(0,poolid),shard_id_t::NO_SHARD));
  int r;
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  string base = "";
  for (int i = 0; i < 100; ++i) base.append("aaaaa");
  set<ghobject_t> created;
  for (int n = 0; n < 10; ++n) {
    char nbuf[100];
    sprintf(nbuf, "n%d", n);
  for (int i = 0; i < 1000; ++i) {
    char buf[100];
    sprintf(buf, "%d", i);
    if (!(i % 100)) {
      cerr << "Object n" << n << " "<< i << std::endl;
    }
    ghobject_t hoid(hobject_t(string(buf) + base, string(), CEPH_NOSNAP, 0, poolid, string(nbuf)));
    {
      ObjectStore::Transaction t;
      t.touch(cid, hoid);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    created.insert(hoid);
  }
  }
  vector<ghobject_t> objects;
  r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(), INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  set<ghobject_t> listed(objects.begin(), objects.end());
  cerr << "listed.size() is " << listed.size() << " and created.size() is " << created.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  objects.clear();
  listed.clear();
  ghobject_t current, next;
  while (1) {
    r = store->collection_list(ch, current, ghobject_t::get_max(), 60,
             &objects, &next);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(sorted(objects));
    for (vector<ghobject_t>::iterator i = objects.begin();
   i != objects.end();
   ++i) {
      if (listed.count(*i))
  cerr << *i << " repeated" << std::endl;
      listed.insert(*i);
    }
    if (objects.size() < 50) {
      ASSERT_TRUE(next.is_max());
      break;
    }
    objects.clear();
    current = next;
  }
  cerr << "listed.size() is " << listed.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  for (set<ghobject_t>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}


TEST_P(KvsStoreTest, ScrubTest) {
  int64_t poolid = 111;
  coll_t cid(spg_t(pg_t(0, poolid),shard_id_t(1)));
  int r;
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  string base = "aaaaa";
  set<ghobject_t> created;
  for (int i = 0; i < 1000; ++i) {
    char buf[100];
    sprintf(buf, "%d", i);
    if (!(i % 5)) {
      cerr << "Object " << i << std::endl;
    }
    ghobject_t hoid(hobject_t(string(buf) + base, string(), CEPH_NOSNAP, i,
            poolid, ""),
        ghobject_t::NO_GEN, shard_id_t(1));
    {
      ObjectStore::Transaction t;
      t.touch(cid, hoid);
      r = queue_transaction(store, ch, std::move(t));
      ASSERT_EQ(r, 0);
    }
    created.insert(hoid);
  }

  // Add same hobject_t but different generation
  {
    ghobject_t hoid1(hobject_t("same-object", string(), CEPH_NOSNAP, 0, poolid, ""),
         ghobject_t::NO_GEN, shard_id_t(1));
    ghobject_t hoid2(hobject_t("same-object", string(), CEPH_NOSNAP, 0, poolid, ""), (gen_t)1, shard_id_t(1));
    ghobject_t hoid3(hobject_t("same-object", string(), CEPH_NOSNAP, 0, poolid, ""), (gen_t)2, shard_id_t(1));
    ObjectStore::Transaction t;
    t.touch(cid, hoid1);
    t.touch(cid, hoid2);
    t.touch(cid, hoid3);
    r = queue_transaction(store, ch, std::move(t));
    created.insert(hoid1);
    created.insert(hoid2);
    created.insert(hoid3);
    ASSERT_EQ(r, 0);
  }

  vector<ghobject_t> objects;
  r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
           INT_MAX, &objects, 0);
  ASSERT_EQ(r, 0);
  set<ghobject_t> listed(objects.begin(), objects.end());
  cerr << "listed.size() is " << listed.size() << " and created.size() is " << created.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  objects.clear();
  listed.clear();
  ghobject_t current, next;
  while (1) {
    r = store->collection_list(ch, current, ghobject_t::get_max(), 60,
             &objects, &next);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(sorted(objects));
    for (vector<ghobject_t>::iterator i = objects.begin();
         i != objects.end(); ++i) {
      if (listed.count(*i))
        cerr << *i << " repeated" << std::endl;
      listed.insert(*i);
    }
    if (objects.size() < 50) {
      ASSERT_TRUE(next.is_max());
      break;
    }
    objects.clear();
    current = next.get_boundary();
  }
  cerr << "listed.size() is " << listed.size() << std::endl;
  ASSERT_TRUE(listed.size() == created.size());
  for (set<ghobject_t>::iterator i = listed.begin();
       i != listed.end();
       ++i) {
    ASSERT_TRUE(created.count(*i));
  }

  for (set<ghobject_t>::iterator i = created.begin();
       i != created.end();
       ++i) {
    ObjectStore::Transaction t;
    t.remove(cid, *i);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}

TEST_P(KvsStoreTest, XattrTest) {
  coll_t cid;
  ghobject_t hoid(hobject_t("tesomap", "", CEPH_NOSNAP, 0, 0, ""));
  bufferlist big;
  for (unsigned i = 0; i < 10000; ++i) {
    big.append('\0');
  }
  bufferlist small;
  for (unsigned i = 0; i < 10; ++i) {
    small.append('\0');
  }
  int r;
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferlist> attrs;
  {
    ObjectStore::Transaction t;
    t.setattr(cid, hoid, "attr1", small);
    attrs["attr1"] = small;
    t.setattr(cid, hoid, "attr2", big);
    attrs["attr2"] = big;
    t.setattr(cid, hoid, "attr3", small);
    attrs["attr3"] = small;
    t.setattr(cid, hoid, "attr1", small);
    attrs["attr1"] = small;
    t.setattr(cid, hoid, "attr4", big);
    attrs["attr4"] = big;
    t.setattr(cid, hoid, "attr3", big);
    attrs["attr3"] = big;
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  map<string, bufferptr> aset;
  store->getattrs(ch, hoid, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  {
    ObjectStore::Transaction t;
    t.rmattr(cid, hoid, "attr2");
    attrs.erase("attr2");
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  aset.clear();
  store->getattrs(ch, hoid, aset);
  ASSERT_EQ(aset.size(), attrs.size());
  for (map<string, bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.push_back(i->second);
    ASSERT_TRUE(attrs[i->first] == bl);
  }

  bufferptr bp;
  r = store->getattr(ch, hoid, "attr2", bp);
  ASSERT_EQ(r, -ENODATA);

  r = store->getattr(ch, hoid, "attr3", bp);
  ASSERT_EQ(r, 0);
  bufferlist bl2;
  bl2.push_back(bp);
  ASSERT_TRUE(bl2 == attrs["attr3"]);

  ObjectStore::Transaction t;
  t.remove(cid, hoid);
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}


// collection split tests 

TEST_P(KvsStoreTest, ColSplitTest0) {
  colsplittest(store.get(), 10, 5, false);
}
TEST_P(KvsStoreTest, ColSplitTest1) {
  colsplittest(store.get(), 10000, 11, false);
}
TEST_P(KvsStoreTest, ColSplitTest1Clones) {
  colsplittest(store.get(), 10000, 11, true);
}
TEST_P(KvsStoreTest, ColSplitTest2) {
  colsplittest(store.get(), 100, 7, false);
}
TEST_P(KvsStoreTest, ColSplitTest2Clones) {
  colsplittest(store.get(), 100, 7, true);
}

/**
 * This test tests adding two different groups
 * of objects, each with 1 common prefix and 1
 * different prefix.  We then remove half
 * in order to verify that the merging correctly
 * stops at the common prefix subdir.  See bug
 * #5273 */
TEST_P(KvsStoreTest, TwoHash) {
  coll_t cid;
  int r;
  auto ch = open_collection_safe(cid);
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  std::cout << "Making objects" << std::endl;
  for (int i = 0; i < 360; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.pool = -1;
    if (i < 8) {
      o.hobj.set_hash((i << 16) | 0xA1);
      t.touch(cid, o);
    }
    o.hobj.set_hash((i << 16) | 0xB1);
    t.touch(cid, o);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  std::cout << "Removing half" << std::endl;
  for (int i = 1; i < 8; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.pool = -1;
    o.hobj.set_hash((i << 16) | 0xA1);
    t.remove(cid, o);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  std::cout << "Checking" << std::endl;
  for (int i = 1; i < 8; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.set_hash((i << 16) | 0xA1);
    o.hobj.pool = -1;
    bool exists = store->exists(ch, o);
    ASSERT_EQ(exists, false);
  }
  {
    ghobject_t o;
    o.hobj.set_hash(0xA1);
    o.hobj.pool = -1;
    bool exists = store->exists(ch, o);
    ASSERT_EQ(exists, true);
  }
  std::cout << "Cleanup" << std::endl;
  for (int i = 0; i < 360; ++i) {
    ObjectStore::Transaction t;
    ghobject_t o;
    o.hobj.set_hash((i << 16) | 0xA1);
    o.hobj.pool = -1;
    t.remove(cid, o);
    o.hobj.set_hash((i << 16) | 0xB1);
    t.remove(cid, o);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ObjectStore::Transaction t;
  t.remove_collection(cid);
  r = queue_transaction(store, ch, std::move(t));
  ASSERT_EQ(r, 0);
}


TEST_P(KvsStoreTest, Rename) {
  coll_t cid(spg_t(pg_t(0, 2122),shard_id_t::NO_SHARD));
  ghobject_t srcoid(hobject_t("src_oid", "", CEPH_NOSNAP, 0, 0, ""));
  ghobject_t dstoid(hobject_t("dest_oid", "", CEPH_NOSNAP, 0, 0, ""));
  bufferlist a, b;
  a.append("foo");
  b.append("bar");
  auto ch = open_collection_safe(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.write(cid, srcoid, 0, a.length(), a);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, srcoid));
  {
    ObjectStore::Transaction t;
    t.collection_move_rename(cid, srcoid, cid, dstoid);
    t.write(cid, srcoid, 0, b.length(), b);
    t.setattr(cid, srcoid, "attr", b);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, srcoid));
  ASSERT_TRUE(store->exists(ch, dstoid));
  {
    bufferlist bl;
    store->read(ch, srcoid, 0, 3, bl);
    ASSERT_TRUE(bl_eq(b, bl));
    store->read(ch, dstoid, 0, 3, bl);
    ASSERT_TRUE(bl_eq(a, bl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, dstoid);
    t.collection_move_rename(cid, srcoid, cid, dstoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, dstoid));
  ASSERT_FALSE(store->exists(ch, srcoid));
  {
    bufferlist bl;
    store->read(ch, dstoid, 0, 3, bl);
    ASSERT_TRUE(bl_eq(b, bl));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, dstoid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


TEST_P(KvsStoreTest, MoveRename) {
  coll_t cid(spg_t(pg_t(0, 212),shard_id_t::NO_SHARD));
  ghobject_t temp_oid(hobject_t("tmp_oid", "", CEPH_NOSNAP, 0, 0, ""));
  ghobject_t oid(hobject_t("dest_oid", "", CEPH_NOSNAP, 0, 0, ""));
  auto ch = open_collection_safe(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, oid));
  bufferlist data, attr;
  map<string, bufferlist> omap;
  data.append("data payload");
  attr.append("attr value");
  omap["omap_key"].append("omap value");
  {
    ObjectStore::Transaction t;
    t.touch(cid, temp_oid);
    t.write(cid, temp_oid, 0, data.length(), data);
    t.setattr(cid, temp_oid, "attr", attr);
    t.omap_setkeys(cid, temp_oid, omap);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, temp_oid));
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.collection_move_rename(cid, temp_oid, cid, oid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  ASSERT_TRUE(store->exists(ch, oid));
  ASSERT_FALSE(store->exists(ch, temp_oid));
  {
    bufferlist newdata;
    r = store->read(ch, oid, 0, 1000, newdata);
    ASSERT_GE(r, 0);
    ASSERT_TRUE(bl_eq(data, newdata));
    bufferlist newattr;
    r = store->getattr(ch, oid, "attr", newattr);
    ASSERT_EQ(r, 0);
    ASSERT_TRUE(bl_eq(attr, newattr));
    set<string> keys;
    keys.insert("omap_key");
    map<string, bufferlist> newomap;
    r = store->omap_get_values(ch, oid, keys, &newomap);
    ASSERT_GE(r, 0);
    ASSERT_EQ(1u, newomap.size());
    ASSERT_TRUE(newomap.count("omap_key"));
    ASSERT_TRUE(bl_eq(omap["omap_key"], newomap["omap_key"]));
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}


TEST_P(KvsStoreTest, BigRGWObjectName) {
  coll_t cid(spg_t(pg_t(0,12),shard_id_t::NO_SHARD));
  ghobject_t oid(
    hobject_t(
      "default.4106.50_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",      "",
      CEPH_NOSNAP,
      0x81920472,
      12,
      ""),

    15,
    shard_id_t::NO_SHARD);
  ghobject_t oid2(oid);
  oid2.generation = 17;
  ghobject_t oidhead(oid);
  oidhead.generation = ghobject_t::NO_GEN;

  auto ch = open_collection_safe(cid);


  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, oidhead);
    t.collection_move_rename(cid, oidhead, cid, oid);
    t.touch(cid, oidhead);
    t.collection_move_rename(cid, oidhead, cid, oid2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }

  {
    vector<ghobject_t> objects;
    r = store->collection_list(ch, ghobject_t(), ghobject_t::get_max(),
             INT_MAX, &objects, 0);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(objects.size(), 1u);
    ASSERT_EQ(objects[0], oid2);
  }

  ASSERT_FALSE(store->exists(ch, oid));

  {
    ObjectStore::Transaction t;
    t.remove(cid, oid2);
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);

  }
}

TEST_P(KvsStoreTest, SetAllocHint) {
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, 0, ""));
  auto ch = open_collection_safe(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    t.touch(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*4, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.set_alloc_hint(cid, hoid, 4*1024*1024, 1024*4, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.remove_collection(cid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
}

TEST_P(KvsStoreTest, TryMoveRename) {
  coll_t cid;
  ghobject_t hoid(hobject_t("test_hint", "", CEPH_NOSNAP, 0, -1, ""));
  ghobject_t hoid2(hobject_t("test_hint2", "", CEPH_NOSNAP, 0, -1, ""));
  auto ch = open_collection_safe(cid);
  int r;
  {
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.try_rename(cid, hoid, hoid2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.touch(cid, hoid);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  {
    ObjectStore::Transaction t;
    t.try_rename(cid, hoid, hoid2);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  }
  struct stat st;
  ASSERT_EQ(store->stat(ch, hoid, &st), -ENOENT);
  ASSERT_EQ(store->stat(ch, hoid2, &st), 0);
}


TEST_P(KvsStoreTest, JournalReplay){
  KvsStore *kvsstore = (KvsStore*) store.get();
  kvsstore->kvsb.is_uptodate = 0;
  coll_t cid;
  bufferlist bl;
  bl.append("1234512345123451234512345123451234512345");
  int r;
  int NUM_OBJS = 100;
  set<ghobject_t> created;
  string base = "";
  for (int i = 0; i < 20; ++i) base.append("a");
  auto ch = open_collection_safe(cid);
  {
    cerr << "create collection" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  } 


 {
  
  cerr << " touch objects to collection" << std::endl;

  for (int i = 0; i < NUM_OBJS; i++){
        //cerr << "Object " << i << std::endl;
        ObjectStore::Transaction t;
        char buf[100];
        snprintf(buf, sizeof(buf), "%d", i);
        ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));
       // t.write(cid, hoid, 0, bl.length(), bl);
        
        t.touch(cid,hoid);
 
        // Add OMAP
 #if 0
        bufferlist small;
        small.append("small");
        map<string,bufferlist> km;
        km["foo"] = small;
        km["bar"].append("asdfjkasdkjdfsjkafskjsfdj");
        bufferlist header;
        header.append("this is a header");
        {
          //t.touch(cid, hoid);
          t.omap_setkeys(cid, hoid, km);
          t.omap_setheader(cid, hoid, header);
          //cerr << "Set omap for object " << hoid << std::endl;
        }
        small.clear();
        header.clear();
#endif
        // END of ADD OMAP

        created.insert(hoid);
        if (i%1000 == 0) {
          cerr << " Object = " << hoid << " touch "<< std::endl;
        //  int status1 = kvsstore->_journal_replay();
        // ASSERT_EQ(status1, 0);
         //cerr << "  status of journal_replay = " << status1 << std::endl;
        }
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
      }

  }
// int status = kvsstore->_journal_replay();
 //ASSERT_EQ(status, 0);
 //cerr << " 1. status of journal_replay = " << status << std::endl;

#if 0
 {
 
 for(set<ghobject_t>::iterator hoid_iter = created.begin();
         hoid_iter != created.end();
         ++hoid_iter) 
    {
        bufferlist small;
        small.append("small");
        map<string,bufferlist> km;
        km["foo"] = small;
        km["bar"].append("asdfjkasdkjdfsjkafskjsfdj");
        bufferlist header;
        header.append("this is a header");
        {
          ObjectStore::Transaction t;
          //t.touch(cid, hoid);
          t.omap_setkeys(cid, *hoid_iter, km);
          t.omap_setheader(cid, *hoid_iter, header);
         // cerr << "Set omap for object " << *hoid_iter << std::endl;
          r = queue_transaction(store, ch, std::move(t));
          ASSERT_EQ(r, 0);
        }
      small.clear();
      header.clear();
    }
  //status = kvsstore->_journal_replay();  
 //ASSERT_EQ(status, 0);
 //cerr << " 2. status of journal_replay = " << status 
 //     << ", kvsb.is_uptodate = " << kvsstore->kvsb.is_uptodate << std::endl; 
 }
#endif
}


TEST_P(KvsStoreTest, SimpleWriteReadTest1){
  coll_t cid(spg_t(pg_t(0, 1), shard_id_t(1)));// Added for iterator bug in FW
  bufferlist bl;
  bl.append("1234512345");
  int r;
  int NUM_OBJS = 1000;
  set<ghobject_t> created;
  string base = "";
  //for (int i = 0; i < 20; ++i) base.append("a");
  auto ch = open_collection_safe(cid);
  {
    cerr << "create collection" << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(cid, 0);
    r = queue_transaction(store, ch, std::move(t));
    ASSERT_EQ(r, 0);
  } 
 
  {
  
  cerr << " write objects to collection" << std::endl;

  for (int i = 0; i < NUM_OBJS; i++){
        cerr << "Object " << i << std::endl;
        
        char buf[100];
        snprintf(buf, sizeof(buf), "%d", i);
        
        bufferlist bl1;
        for (int i = 0; i<425; i++)
          bl1.append("12");

        ghobject_t hoid(hobject_t(sobject_t(string(buf) + base, CEPH_NOSNAP)));  
        created.insert(hoid);
        
      
            
        ObjectStore::Transaction t;
        t.write(cid, hoid, 0, bl1.length(), bl1);
        r = queue_transaction(store, ch, std::move(t));
        ASSERT_EQ(r, 0);
           

        cerr << " read object " << hoid << std::endl;
        bufferlist in;
        r = store->read(ch, hoid, 0, bl1.length(), in);
        cerr << " read oid: " << hoid << ",  retcode: " << r
                 << " written bl length: " << in.length()
                 << " read bl length: " << bl1.length() 
                 << ", written hash: " << ceph_str_hash_linux(bl1.c_str(), bl1.length())
                 << ", read hash: " << ceph_str_hash_linux(in.c_str(), in.length())
                 << std::endl;
        in.clear();
        bl1.clear();
  }
  //cerr << " ALL objects written to collection" << std::endl;
 
  }


  {
    //cerr << " reading all objects " << std::endl;
   /*
    for(set<ghobject_t>::iterator hoid_iter = created.begin();
         hoid_iter != created.end();
         ++hoid_iter) 
    {
        bufferlist in;
        cerr << " Object = " << *hoid_iter << std::endl;
        r = store->read(ch, *hoid_iter, 0, bl.length(), in);
        cerr << " read object length = " << r << std::endl;
       // ASSERT_EQ(bl.length(), r);
        in.clear();
    }
    */
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
