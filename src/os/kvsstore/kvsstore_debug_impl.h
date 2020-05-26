/*
 * kvsstore_debug.h
 *
 * handles debug and trace messages
 * FTRACE, KTRACE, DOUT, BACKTRACE
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_DEBUG_IMPL_H_
#define SRC_OS_KVSSTORE_KVSSTORE_DEBUG_IMPL_H_
#include <set>
#include <mutex>
#include <map>
#include <string>
#include <iomanip>
#include <mutex>
#include <fstream>
#include <sstream>
#include <iostream>
#include <memory.h>
#include <string.h>
#include <gperftools/heap-checker.h>
#include <include/ceph_hash.h>
#include <common/BackTrace.h>

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define NOTSUPPORTED_EXIT do { std::string msg = std::string(__func__ ) + " is not implemented yet";  /* BACKTRACE(msg); */ derr << msg << dendl; return 0; } while (0)
#define NOTSUPPORTED do { std::string msg = std::string(__func__ ) + " is not implemented yet";  /*BACKTRACE(msg); */ derr << msg << dendl;  } while (0)

///
/// Logger Class
///

class FtraceFile {
public:
    template <typename T>
    void log(T& message)
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
    std::mutex mutex;
    std::ofstream fp;
};


class FtraceTerm {
public:
    template <typename T>
    void log(T& message)
    {
        mutex.lock();
        std::cout << message.str() << std::endl;
        std::cout.flush();
        mutex.unlock();

    }
private:
    std::mutex mutex;
};

extern FtraceFile FLOG;
extern FtraceTerm TLOG;

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
#ifdef _KVSFILELOGGING
        FLOG.log(ss);
#else
        TLOG.log(ss);
#endif
    }
};

inline std::string get_thread_name() {
    return std::to_string((uint64_t)pthread_self());
}

inline std::string get_fixed_func_name(const char *func) {
    std::string fname (func);
    if (fname.length() > 15) fname = fname.substr(0,15);
    return fname;
}

///
/// Key, Value Utils
///


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

inline std::string print_value(int r, int opcode, void *key, unsigned keylen, void *data, unsigned length)
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
    std::stringstream ss2;
    ss2 << header << type << " " << cmd << "- ret = " << r  << ", key " << keystr << ss.str();
    return ss2.str();
}

inline std::string print_kvssd_key(const std::string &str)
{
	return print_kvssd_key(str.c_str(), str.length());
}



#if 0

inline void add_live_object(void *addr) {
    std::unique_lock<std::mutex> l(live_object_lock);
    if (live_objects.find(addr) != live_objects.end()) {
        std::cout << "ERROR";
    };
    live_objects.insert(addr);
}

inline void remove_live_object(void *addr, const std::string &bt) {
    std::unique_lock<std::mutex> l(live_object_lock);
    if (live_objects.find(addr) == live_objects.end()) {
        std::cout << "ERROR";
    };
    removed_objects[addr] = bt;
    live_objects.erase(addr);
}

inline bool is_live_object(void *addr) {
    std::unique_lock<std::mutex> l(live_object_lock);
    return (live_objects.find(addr) != live_objects.end());
}
#endif

#endif /* SRC_OS_KVSSTORE_KVSSTORE_DEBUG_H_ */
