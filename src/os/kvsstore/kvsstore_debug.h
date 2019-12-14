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
#include <memory.h>
#include <gperftools/heap-checker.h>
//#include "common/BackTrace.h"
// function traces: records enter and exit events
// ----------------------------------------------
#define ENABLE_IOTRACE
#define ENABLE_FTRACE
#ifdef ENABLE_FTRACE

struct FtraceFile {
    std::mutex lock;
    std::ofstream fp;
    FtraceFile(const std::string &name) {
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
extern FtraceFile kvs_osd;

struct FtraceObject {

    std::string func;
    //HeapLeakChecker heap_checker;, heap_checker(f)
    FtraceObject(const char *f, int line) : func(f) {
#if 0
        std::ofstream &fp = kvs_ff.get_fp();
        fp << pthread_self() << " [ENTR] " << func << " ";
        fp.flush();
        fp << ", memcheck= " ;
        {
            std::vector<void *> malloc_p;
            for (int i = 0; i < 100; i++) {
                void *v = malloc(2 * 1024*1024);
                malloc_p.push_back(v);
            }
            for (void *p : malloc_p) {
                free(p);
            }
            fp << "malloc OK ";
        }
        fp.flush();
        {
            //fp << __FILE__ << "," << __LINE__ << ": MEMORY CORRUPTION TEST CODE" << "\n";
            uint64_t long_keyaddr = (uint64_t)malloc(8192);
            std::string str((char *)long_keyaddr, 50);
            if (str.length() == 0)
                fp << "test1 = " << (void *) &str << "\n";
            free((void*)long_keyaddr);
            fp << "access OK";
        }
        fp << "\n";
        fp.flush();
        kvs_ff.return_fp();
#endif
    }


    ~FtraceObject() {
#if 0
        std::ofstream &fp = kvs_ff.get_fp();
        fp << pthread_self() << " [EXIT] " << func << " ";
        fp.flush();
        {
            std::vector<void *> malloc_p;
            for (int i = 0; i < 100; i++) {
                void *v = malloc(2 * 1024*1024);
                malloc_p.push_back(v);
            }
            for (void *p : malloc_p) {
                free(p);
            }
            fp << "malloc OK ";
        }
        fp.flush();
        {
            uint64_t long_keyaddr = (uint64_t)malloc(8192);
            std::string str((char *)long_keyaddr, 50);
            if (str.length() == 0)
                fp << "test1 = " << (void *) &str << "\n";
            free((void*)long_keyaddr);
            fp << "access OK";
        }
        fp.flush();
        fp << "\n"; //<< ", Global Leaks " << HeapLeakChecker::NoGlobalLeaks() << "\n"; //<< ", no leaks = " << heap_checker.NoLeaks() << ", bytes leaked " << heap_checker.BytesLeaked() << "\n";        fp.flush();
        kvs_ff.return_fp();
#endif
    }
};
#define LOGOSD std::cerr
#define LOGEND std::endl
//#define LOGOSD kvs_ff.get_fp() << pthread_self() << "[OSD][" << __FUNCTION__ << ":" << __LINE__ <<  "] "
//#define LOGEND "\n"; do { kvs_osd.return_fp(); } while(0)

#define FTRACE FtraceObject fobj(__FUNCTION__, __LINE__);
#define TRIO kvs_ff.get_fp() << pthread_self() << "[KIO][" << __FUNCTION__ << ":" << __LINE__ <<  "] "
#define TRITER kvs_ff.get_fp() << pthread_self() << "[KIT][" << __FUNCTION__ << ":" <<__LINE__<< "] "
#define TR kvs_ff.get_fp() << " " << pthread_self() << "[KVS][" << __FUNCTION__ << ":"  << __LINE__ << "] "
//#define TR std::cout << pthread_self() << " "
#define TREND "\n"; do { kvs_ff.return_fp(); } while(0)
#else
#define FTRACE
#define TR
#endif


// key traces: prints keys
// ----------------------------------------------------
template<typename T>
inline void assert_equals(const T &t1, const T &t2, const std::string &msg) {
    if (t1 != t2) {
        TR << "ASSERT FAILURE: " << msg << TREND;
        TR << "NOT EQUAL: " << t1 << " != " << t2 << TREND;
        exit(1);
    }
}

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
//#define BACKTRACE ({ ostringstream oss; oss << BackTrace(1); lderr(cct) << oss.str() << dendl; })


#define NOTSUPPORTED_EXIT do { std::string msg = std::string(__func__ ) + " is not implemented yet";  /* BACKTRACE(msg); */ derr << msg << dendl; return 0; } while (0)
#define NOTSUPPORTED do { std::string msg = std::string(__func__ ) + " is not implemented yet";  /*BACKTRACE(msg); */ derr << msg << dendl;  } while (0)


#endif /* SRC_OS_KVSSTORE_KVSSTORE_DEBUG_H_ */
