#ifndef SRC_OS_KVSSTORE_KVSSTORE_DB_CC_
#define SRC_OS_KVSSTORE_KVSSTORE_DB_CC_

#include "../kvsstore_types.h"
#include "../kvsstore_debug.h"
#include "../KvsStore.h"

// set up dout_context and dout_prefix here
// -----------------------------------------
#define dout_context cct
#define dout_subsys ceph_subsys_kvs
#undef dout_prefix
#define dout_prefix *_dout << "[kvsstore] "


/// Callback functions
/// -----------------------------------------------------------------

void write_callback(kv_io_context &op, void* private_data) {
    KvsTransContext *txc= (KvsTransContext *)private_data;
    txc->aio_finish(&op);
}






#endif /* SRC_OS_KVSSTORE_KVSSTORE_DB_CC_ */
