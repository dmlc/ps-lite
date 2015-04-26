#pragma once

#include <stdlib.h>
#include <string>
#include "base/integral_types.h"
// #include "base/common.h"

namespace ps {

// return true if success

inline bool strtofloat(const char* str, float* num) {
  char* end;
  *num = strtof(str, &end);
  if (*end == '\0') return true;
  return false;
}


inline bool strtoi32(const char* str, int32* num) {
  char* end;
  *num = strtol(str, &end, 10);
  if (*end == '\0') return true;
  return false;
}

inline bool strtou64(const char* str, uint64* num) {
  char* end;
  *num = strtoull(str, &end, 10);
  if (*end == '\0') return true;
  return false;
}

// convinient wrapper
inline bool strtofloat(const std::string& str, float* num) {
  return strtofloat(str.c_str(), num);
}

inline bool strtoi32(const std::string& str, int32* num) {
  return strtoi32(str.c_str(), num);
}

inline bool strtou64(const std::string& str, uint64* num) {
  return strtou64(str.c_str(), num);
}

} // namespace ps
