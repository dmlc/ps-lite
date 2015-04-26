#pragma once
#include <sstream>
#include "base/integral_types.h"
namespace ps {

typedef uint64 Key;
static const Key kMaxKey = kuint64max;


/**
 * \brief returns a short debug string
 */
template <typename V>
inline std::string DBSTR(const V* data, int n, int m = 5) {
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

/**
 * \brief printf-style logging
 */
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

}  // namespace ps
