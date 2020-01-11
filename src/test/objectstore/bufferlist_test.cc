#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <sys/mount.h>
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

class BufferListTest : public ::testing::TestWithParam<const char*> {
public:
    BufferListTest() {}
    void SetUp() override {
    }
    void TearDown() override {
    }
};

const int KVS_OBJECT_SPLIT_SIZE = 8192;



class kvs_stripe {
    int pos;
    int length;
    char *buffer;
public:
    kvs_stripe(): pos(0), length(0) {
        allocate();
    }

    void allocate() {
        buffer = (char*)malloc(KVS_OBJECT_SPLIT_SIZE);
    }

    void release() {
        free (buffer);
        buffer = 0;
    }

    void clear() {
        pos = 0;
    }

    void append_zero(int length) {
        if (length + pos > KVS_OBJECT_SPLIT_SIZE) throw "end of buffer";

        memset(buffer + pos, 0, length);
        pos += length;
    }

    void append(const bufferlist& other, unsigned off, unsigned len) {
        if (len + pos > KVS_OBJECT_SPLIT_SIZE) throw "end of buffer";
        other.copy(off, len, buffer + pos);  pos += len;
    }

    void substr_of(const bufferlist& other, unsigned off, unsigned len)
    {
        clear();
        append(other, off, len);
    }

};


TEST_P(BufferListTest, OpenClose) {
    std::cout << "!!!!!" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
        SimpleTest,
        BufferListTest,
        ::testing::Values("leveldb", "rocksdb", "memdb"));

int main(int argc, char **argv) {
    vector<const char*> args;
    argv_to_vec(argc, (const char **)argv, args);

    auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                           CODE_ENVIRONMENT_UTILITY,
                           CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    common_init_finish(g_ceph_context);
    g_ceph_context->_conf.apply_changes(nullptr);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
