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

#ifdef _MSC_VER
typedef signed char      int8_t;
typedef __int16          int16_t;
typedef __int32          int32_t;
typedef __int64          int64_t;
typedef unsigned char    uint8_t;
typedef unsigned __int16 uint16_t;
typedef unsigned __int32 uint32_t;
typedef unsigned __int64 uint64_t;
#else
#include <inttypes.h>
#endif

typedef int8_t           int8;
typedef int16_t          int16;
typedef int32_t          int32;
typedef int64_t          int64;

typedef uint8_t          uint8;
typedef uint16_t         uint16;
typedef uint32_t         uint32;
typedef uint64_t         uint64;


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
#ifdef _MSC_VER
/*! \brief printf-style logging */
#define NOTICE(_fmt_,...) do {                                          \
    struct timeval tv; gettimeofday(&tv, NULL);                         \
    time_t ts = (time_t)(tv.tv_sec);                                    \
    struct ::tm tm_time; localtime_r(&ts, &tm_time);                    \
    int n = strlen(__FILE__) - 1;                                       \
    for (; n > -1; --n) { if (n==-1 || __FILE__[n] == '/') break; }     \
    fprintf(stdout, "[%02d%02d %02d:%02d:%02d.%03d %s:%d] " _fmt_ "\n", \
            1+tm_time.tm_mon, tm_time.tm_mday, tm_time.tm_hour,         \
            tm_time.tm_min, tm_time.tm_sec, (int)tv.tv_usec/1000,       \
            __FILE__+n+1, __LINE__, __VA_ARGS__);                       \
} while (0)
#else
/*! \brief printf-style logging */
#define NOTICE(_fmt_, args...) do {                                    \
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
#endif


#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)
#endif

#define SINGLETON(Typename)                     \
  static Typename& instance() {                 \
    static Typename e;                          \
    return e;                                   \
  }
}  // namespace ps
