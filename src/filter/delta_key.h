#pragma once
#include "filter/filter.h"
namespace ps {

/// \brief Used delta encoding: `key[i] = keys[i+1]-key[i]`
class DeltaKeyFilter : public IFilter {
 public:
  void Encode(Message* msg) {
    CHECK_NOTNULL(Find(Filter::DELTA_KEY, msg));
    if (msg->key.empty()) return;
    if(msg->task.key_type() == DataType::UINT32) {
      msg->key = Encode<uint32>(msg->key);
    } else if (msg->task.key_type() == DataType::UINT64) {
      msg->key = Encode<uint64>(msg->key);
    }
  }

  void Decode(Message* msg) {
    CHECK_NOTNULL(Find(Filter::DELTA_KEY, msg));
    if (msg->key.empty()) return;
    if(msg->task.key_type() == DataType::UINT32) {
      Decode(SArray<uint32>(msg->key));
    } else if (msg->task.key_type() == DataType::UINT64) {
      Decode(SArray<uint64>(msg->key));
    }
  }
 private:

  template <typename K>
  SArray<K> Encode(const SArray<char>& orig) {
    SArray<K> key; key.CopyFrom(SArray<K>(orig));
    for (size_t i = key.size(); i > 1; --i) {
      key[i-1] -= key[i-2];
    }
    return key;
  }

  template <typename K>
  void Decode(SArray<K> key) {
    for (size_t i = 1; i < key.size(); ++i) {
      key[i] += key[i-1];
    }
  }
};
}  // namespace ps
