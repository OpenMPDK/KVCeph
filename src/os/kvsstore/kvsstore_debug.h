/*
 * kvsstore_debug.h
 *
 * handles debug and trace messages
 * FTRACE, KTRACE, DOUT, BACKTRACE
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_DEBUG_H_
#define SRC_OS_KVSSTORE_KVSSTORE_DEBUG_H_

#include <map>
#include <string>
#include <mutex>
#include <fstream>
#include <sstream>
#include <iostream>
#include <memory.h>
#include <string.h>
#include <gperftools/heap-checker.h>
#include <include/ceph_hash.h>
//#include "common/BackTrace.h"
// function traces: records enter and exit events
// ----------------------------------------------

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

//#define ENABLE_FTRACE
//#define ENABLE_FUNCTION_TRACE
//#define ENABLE_IOTRACE
//#define ENABLE_IOTRACE_SUBMIT
//#define IOTRACE_MINIMAL


class FtraceFile {
public:
    template <typename T>
    static void log(T& message)
    {
        mutex.lock();
        if (!fp.is_open()) {
            fp.open("ftrace.txt", std::ofstream::out | std::ofstream::app);
        }
        fp << message.str();
        fp << "\n";
        if (!fp.bad())
            fp.flush();
        mutex.unlock();

    }
private:
    static std::mutex mutex;
    static std::ofstream fp;
};

extern FtraceFile FLOG;

struct SimpleLoggerBuffer{
    std::stringstream ss;

    SimpleLoggerBuffer() = default;
    SimpleLoggerBuffer(const SimpleLoggerBuffer&) = delete;
    SimpleLoggerBuffer& operator=(const SimpleLoggerBuffer&) = delete;
    SimpleLoggerBuffer& operator=(SimpleLoggerBuffer&&) = delete;
    SimpleLoggerBuffer(SimpleLoggerBuffer&& buf): ss(move(buf.ss)) {
    }
    template <typename T>
    SimpleLoggerBuffer& operator<<(T&& message)
    {
        ss << std::forward<T>(message);
        return *this;
    }

    ~SimpleLoggerBuffer() {
        FLOG.log(ss);
    }
};



template <typename T>
SimpleLoggerBuffer operator<<(FtraceFile &simpleLogger, T&& message)
{
    SimpleLoggerBuffer buf;
    buf.ss << std::forward<T>(message);
    return buf;
}

#ifdef ENABLE_FTRACE

#define MEMCHECK 0
#include <set>

extern std::mutex debug_threadname_lock;
extern std::map<uint64_t, int> debug_threadnames;




inline static std::string get_thread_name() {
    if (1)
    {
        return std::to_string((uint64_t)pthread_self());
    }
    /*
    uint64_t thread_id = pthread_self();
    std::string name;

    std::unique_lock<std::mutex> l(debug_threadname_lock);
    auto it = debug_threadnames.find(thread_id);
    if (it == debug_threadnames.end()) {
        uint64_t newid = debug_threadnames.size();
        debug_threadnames[thread_id] = newid;
    }

    std::string idstr = std::to_string(debug_threadnames[thread_id]);
    if (idstr.length() < 2) idstr = "0" + idstr;
    return "thread-" + idstr;
    */
}

struct FtraceObject {

    std::string func;
    int line;
    //HeapLeakChecker heap_checker;, heap_checker(f)
    FtraceObject(const char *f, int line_) : func(f), line(line_) {

#ifdef ENABLE_FUNCTION_TRACE

       FLOG <<  "[" << get_thread_name() << "] " << func << ":" << line << " - Enter";
#endif
#if MEMCHECK
        FLOG << ", memcheck= " ;
        {
            std::vector<void *> malloc_p;
            for (int i = 0; i < 100; i++) {
                void *v = malloc(2 * 1024*1024);
                malloc_p.push_back(v);
            }
            for (void *p : malloc_p) {
                free(p);
            }
            FLOG << "malloc OK ";
        }
        {
            //fp << __FILE__ << "," << __LINE__ << ": MEMORY CORRUPTION TEST CODE" << "\n";
            uint64_t long_keyaddr = (uint64_t)malloc(8192);
            std::string str((char *)long_keyaddr, 50);
            if (str.length() == 0)
                FLOG << "test1 = " << (void *) &str << "\n";
            free((void*)long_keyaddr);
            FLOG << "access OK";
        }
        FLOG << "\n";
#endif
    }


    ~FtraceObject() {
#ifdef ENABLE_FUNCTION_TRACE
        FLOG <<  "[" << get_thread_name() << "] " << func << ":" << line << " - Exit";
#endif
#if MEMCHECK
        {
            std::vector<void *> malloc_p;
            for (int i = 0; i < 100; i++) {
                void *v = malloc(2 * 1024*1024);
                malloc_p.push_back(v);
            }
            for (void *p : malloc_p) {
                free(p);
            }
            FLOG << "malloc OK ";
        }
        {
            uint64_t long_keyaddr = (uint64_t)malloc(8192);
            std::string str((char *)long_keyaddr, 50);
            if (str.length() == 0)
                FLOG << "test1 = " << (void *) &str << "\n";
            free((void*)long_keyaddr);
            FLOG << "access OK";
        }

#endif
    }
};

#define LOGOSD if (false) FLOG
#define LOGEND ""

#define FTRACE FtraceObject fobj(__FUNCTION__, __LINE__);
#define TR FLOG << "[" << get_thread_name() << "] " << __FUNCTION__ << ":" << __LINE__ << " - "
#define TRERR FLOG << "[" << get_thread_name() << "] " << __FUNCTION__ << ":" << __LINE__ << " - ERR "
#define TRBACKTRACE { ostringstream oss; oss << BackTrace(1); FLOG << "[" << __FILENAME__ << ":"  << __LINE__ << "] " << "Backtrace: " << oss.str(); }

#else
#define TRERR FLOG << pthread_self() << "[" << __FILENAME__ << ":"  << __LINE__ << "] ERR: "
#define FTRACE
#define TR if (false) std::cout
#define LOGOSD if (false) std::cout
#define LOGEND ""
#define TRBACKTRACE
#endif

#ifdef IOTRACE_MINIMAL
#define TRIO if (false) FLOG
#else
#define TRIO FLOG <<  "[" << get_thread_name() << "] " << __FUNCTION__ << ":" << __LINE__ << " - "
#endif




// key traces: prints keys
// ----------------------------------------------------
template<typename T>
inline void assert_equals(const T &t1, const T &t2, const std::string &msg) {
    if (t1 != t2) {
        //TR << "ASSERT FAILURE: " << msg ;
        //TR << "NOT EQUAL: " << t1 << " != " << t2 ;
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

#include <sstream>

inline void print_value(int r, int opcode, void *key, unsigned keylen, void *data, unsigned length)
{
    std::stringstream ss;
    std::string keystr = print_kvssd_key((const char*)key, keylen);

    const uint32_t prefix = *(uint32_t*)key;
    std::string type = "";
    if (prefix == 0x0) {
        type = "ONODE";
    } else if (prefix == 0x1) {
        type = "COLL";
    } else {
        type = "DATA";
    }

    if (opcode == 0x90 || opcode == 0x81) {
        unsigned hash = ceph_str_hash_linux((const char*)data, length);
        ss << ", value addr " << data << ", length = " << length << ", hash " << hash;
    }
    std::string cmd = "";
    if (opcode == 0x90) {
        cmd = "READ";
    } else if (opcode == 0x81) {
        cmd = "WRITE";
    } else if (opcode == 0xA1) {
        cmd = "DELETE";
    } else {
        cmd = "UNKNOWN";
    }

    std::string header = "[TEST] ";
    if(opcode != -1) {
        header = "[IO] ";
    };
    TR << header << type << " " << cmd << "- ret = " << r  << ", key " << keystr << ss.str();
}




inline std::string print_kvssd_key(const std::string &str)
{
	return print_kvssd_key(str.c_str(), str.length());
}

#define SMUTEX last_shared_loc[]

//#define KTRACE(cct, in, len) ({ typeof (in) in_ = (in); typeof (len) len_ = (len); lderr(cct) << "[" << __FUNC__ << ":" << __LINE__ << "] user key: " << print_kvssd_key(in_, len_) << dendl; })

// Backtrace
// -----------------------------------------------------------

//#define BACKTRACE(cct) ({ ostringstream oss; oss << BackTrace(1); lderr(cct) << oss.str() << dendl; })



#define NOTSUPPORTED_EXIT do { std::string msg = std::string(__func__ ) + " is not implemented yet";  /* BACKTRACE(msg); */ derr << msg << dendl; return 0; } while (0)
#define NOTSUPPORTED do { std::string msg = std::string(__func__ ) + " is not implemented yet";  /*BACKTRACE(msg); */ derr << msg << dendl;  } while (0)


#endif /* SRC_OS_KVSSTORE_KVSSTORE_DEBUG_H_ */
