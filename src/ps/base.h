#pragma once
#include <sstream>
#include <string>
#include <limits>
#if USE_DMLC
#include "dmlc/logging."
#else
#include "glog/logging.h"
#endif  // USE_DMLC

namespace ps {

typedef signed char         int8;
typedef short               int16;  // NOLINT
typedef int                 int32;
#ifdef COMPILER_MSVC
typedef __int64             int64;  // NOLINT
#else
typedef long long           int64;  // NOLINT
#endif /* COMPILER_MSVC */

typedef unsigned char      uint8;
typedef unsigned short     uint16;  // NOLINT
typedef unsigned int       uint32;
#ifdef COMPILER_MSVC
typedef unsigned __int64   uint64;
#else
typedef unsigned long long uint64;  // NOLINT
#endif /* COMPILER_MSVC */

#if USE_KEY32
/// \brief Use unsigned 32-bit int as the key
typedef uint32 Key;
#else
/// \brief Use unsigned 64-bit int as the key
typedef uint64 Key;
#endif

/// \brief The maximal allowed key
static const Key kMaxKey = std::numeric_limits<Key>::max();

/*! \brief returns a short debug string */
template <typename V>
inline std::string DebugStr(const V* data, int n, int m = 5) {
  std::stringstream ss;
  ss << "[" << n << "]: ";
  if (n < 2 * m) {
    for (int i = 0; i < n; ++i) ss << data[i] << " ";
  } else {
    for (int i = 0; i < m; ++i) ss << data[i] << " ";
    ss << "... ";
    for (int i = n-m; i < n; ++i) ss << data[i] << " ";
  }
  return ss.str();
}

/*! \brief printf-style logging */
#define NOTICE(_fmt_, args...) do {                                     \
    struct timeval tv; gettimeofday(&tv, NULL);                         \
    time_t ts = (time_t)(tv.tv_sec);                                    \
    struct ::tm tm_time; localtime_r(&ts, &tm_time);                    \
    int n = strlen(__FILE__) - 1;                                       \
    for (; n > -1; --n) { if (n==-1 || __FILE__[n] == '/') break; }     \
    fprintf(stdout, "[%02d%02d %02d:%02d:%02d.%03d %s:%d] " _fmt_ "\n", \
            1+tm_time.tm_mon, tm_time.tm_mday, tm_time.tm_hour,         \
            tm_time.tm_min, tm_time.tm_sec, (int)tv.tv_usec/1000,       \
            __FILE__+n+1, __LINE__, ##args);                            \
} while (0)

#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#define SINGLETON(Typename)                     \
  static Typename& instance() {                 \
    static Typename e;                          \
    return e;                                   \
  }
}  // namespace ps
