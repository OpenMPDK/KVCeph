/*
 * kvsstore_debug.h
 *
 * handles debug and trace messages
 * FTRACE, KTRACE, DOUT, BACKTRACE
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_DEBUG_H_
#define SRC_OS_KVSSTORE_KVSSTORE_DEBUG_H_

#include "indexer_hint.h"
#include <string>
#include <mutex>
#include <fstream>
#include <iostream>
//#include "common/BackTrace.h"
// function traces: records enter and exit events
// ----------------------------------------------
#define ENABLE_IOTRACE
#define ENABLE_FTRACE
#ifdef ENABLE_FTRACE

struct FtraceFile {
    const char *name = "ftrace.txt";
    std::mutex lock;
    std::ofstream fp;
    FtraceFile() {
        fp.open(name, std::ofstream::out | std::ofstream::app);
    }

    ~FtraceFile() {
        fp.close();
    }

    std::ofstream &get_fp() {
        lock.lock();
        return fp;
    }

    void return_fp() {
        fp.flush();
        lock.unlock();
    }
};

extern FtraceFile kvs_ff;

struct FtraceObject {

    std::string func;

    FtraceObject(const char *f, int line) : func(f) {
        std::ofstream &fp = kvs_ff.get_fp();
        fp << pthread_self() << " [ENTR] " << func << "\n";
        fp.flush();
        kvs_ff.return_fp();
    }


    ~FtraceObject() {
        std::ofstream &fp = kvs_ff.get_fp();
        fp << pthread_self() << " [EXIT] " << func << "\n";
        fp.flush();
        kvs_ff.return_fp();
    }
};

#define FTRACE FtraceObject fobj(__FUNCTION__, __LINE__);
#define TRIO kvs_ff.get_fp() << pthread_self() << "      "
#define TRITER kvs_ff.get_fp() << pthread_self() << "      "
#define TR kvs_ff.get_fp() << pthread_self() << "  "
//#define TR std::cout << pthread_self() << " "
#define TREND "\n"; do { kvs_ff.return_fp(); } while(0)
#else
#define FTRACE
#define TR
#endif


// key traces: prints keys
// ----------------------------------------------------

template <typename T>
inline std::string print_kvssd_key(T* in_, unsigned length)
{
	unsigned i;
	std::string out;
    char buf[10];
    char *in = (char *)in_;

    for (i=0; i < length; ++i) {
    	snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
        out.append(buf);
    }
    snprintf(buf, sizeof(buf), "(%d B)", length);
    out.append(buf);
    return out;
}


inline std::string print_kvssd_key(const std::string &str)
{
	return print_kvssd_key(str.c_str(), str.length());
}


//#define KTRACE(cct, in, len) ({ typeof (in) in_ = (in); typeof (len) len_ = (len); lderr(cct) << "[" << __FUNC__ << ":" << __LINE__ << "] user key: " << print_kvssd_key(in_, len_) << dendl; })

// Backtrace
// -----------------------------------------------------------

#define BACKTRACE(cct) ({ ostringstream oss; oss << BackTrace(1); lderr(cct) << oss.str() << dendl; })


#define NOTSUPPORTED_EXIT do { std::string msg = std::string(__func__ ) + " is not implemented yet";  /* BACKTRACE(msg); */ derr << msg << dendl; return 0; } while (0)
#define NOTSUPPORTED do { std::string msg = std::string(__func__ ) + " is not implemented yet";  /*BACKTRACE(msg); */ derr << msg << dendl;  } while (0)


#endif /* SRC_OS_KVSSTORE_KVSSTORE_DEBUG_H_ */
