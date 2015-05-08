#pragma once
#include "kv/kv_store.h"
namespace ps {

// fixed length value
template<typename K, typename V, typename Handle, int val_len>
class KVStoreSparse : public KVStore {
 public:
  KVStoreSparse(int id, Handle handle, int sync_val_len)
      : KVStore(id), handle_(handle), k_(sync_val_len) {
    handle_.SetCaller(this);
  }
  virtual ~KVStoreSparse() { }

  void GetValue(Message* msg) {
    // parse data
    SArray<K> key(msg->key);
    size_t n = key.size();
    SArray<V> val(n * k_);
    int ts = msg->task.time();

    // handle this pull request
    handle_.Start(false, ts, msg->sender);
    V* val_data = val.data();
    for (size_t i = 0; i < n; ++i, val_data += k_) {
      K key_i = key[i];
      handle_.Pull(Blob<const K>(&key_i, 1),
                   Blob<const V>(FindValue(key_i), val_len),
                   Blob<V>(val_data, k_));
    }
    msg->add_value(val);
    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }

  void SetValue(const Message* msg) {
    // parse data
    SArray<K> key(msg->key);
    size_t n = key.size();
    CHECK_EQ(msg->value.size(), 1);
    SArray<V> val(msg->value[0]);
    CHECK_EQ(n * k_, val.size());
    int ts = msg->task.time();

    // handle this push request

    handle_.Start(true, ts, msg->sender);

    V* val_data = val.data();
    for (size_t i = 0; i < n; ++i, val_data += k_) {
      K key_i = key[i];
      handle_.Push(Blob<const K>(&key_i, 1), Blob<const V>(val_data, k_),
                   Blob<V>(FindValue(key_i), val_len));
    }
    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }

 private:
  inline V* FindValue(K key) {
    auto it = data_.find(key);
    if (it == data_.end()) {
      // init if necessary
      auto it2 = data_.insert(std::make_pair(key, std::array<V, val_len>()));
      CHECK(it2.second);
      it = it2.first;
      handle_.Init(Blob<const K>(&key, 1),
                   Blob<V>(it->second.data(), val_len));
    }
    return it->second.data();
  }
  std::unordered_map<K, std::array<V, val_len>> data_;
  Handle handle_;
  int k_;
};
}  // namespace ps
