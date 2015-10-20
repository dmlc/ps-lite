#pragma once
#include <vector>
#include "ps/sarray.h"
#include "ps/internal/meta_message.pb.h"
namespace ps {

enum DataType {
  CHAR, INT8, INT16, INT32, INT64,
  UINT8, UINT16, UINT32, UINT64,
  FLOAT, DOUBLE, OTHER
};

template<typename V, typename W>
inline bool SameType() {
  return std::is_same<typename std::remove_cv<V>::type, W>::value;
}

template<typename V>
DataType GetDataType() {
  if (SameType<V, int8_t>()) {
    return INT8;
  } else if (SameType<V, int16_t>()) {
    return INT16;
  } else if (SameType<V, int32_t>()) {
    return INT32;
  } else if (SameType<V, int64_t>()) {
    return INT64;
  } else if (SameType<V, uint8_t>()) {
    return UINT8;
  } else if (SameType<V, uint16_t>()) {
    return UINT16;
  } else if (SameType<V, uint32_t>()) {
    return UINT32;
  } else if (SameType<V, uint64_t>()) {
    return UINT64;
  } else if (SameType<V, float>()) {
    return FLOAT;
  } else if (SameType<V, double>()) {
    return DOUBLE;
  } else {
    return OTHER;
  }
}



/**
 * \brief messages that communicated amaong nodes.
 */
struct Message {
  static const int kInvalidNode = 0;
  Message() : sender(kInvalidNode), recver(kInvalidNode) { }
  MetaMessage meta;

  std::vector<SArray<char> > data;

  /**
   * \brief push array into data, and add the data type
   */
  template <typename V>
  void AddData(const SArray<V>& val) {
    CHECK_EQ(data.size(), (size_t)meta.data_type_size());
    meta.add_data_type(GetDataType<V>());
    data.push_back(SArray<char>(val));
  }

  int sender;
  int recver;

};
}  // namespace ps
