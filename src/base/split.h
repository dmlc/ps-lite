#pragma once
#include "base/common.h"

namespace ps {

// split a std::string using a character delimiter. if skip_empty == true,
// split("one:two::three", ':'); will return 4 items

inline std::vector<std::string>
split(const std::string &s, char delim, bool skip_empty = false) {
  std::vector<std::string> elems;
  std::stringstream ss(s);
  string item;
  while (std::getline(ss, item, delim))
    if (!(skip_empty && item.empty()))
      elems.push_back(item);
  return elems;
}

// TODO support bool skip_empty = false
inline std::string join(const std::vector<std::string> &elems, const string& delim) {
  std::string str;
  for (int i = 0; i < elems.size() - 1; ++i) {
    str += elems[i] + delim;
  }
  str += elems.back();
  return str;
}

} // namespace ps
