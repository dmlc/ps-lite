#pragma once
#include "kv/kv_store.h"
#include "ps/blob.h"
namespace ps {

// dynamic length value
template<typename K, typename V, typename Handle>
class KVStoreSparseDynamic : public KVStore {
 public:
  KVStoreSparseDynamic(int id, Handle handle)
      : KVStore(id), handle_(handle) { }
  virtual ~KVStoreSparseDynamic() {
    for (auto& v : data_) delete [] v.second.data;
  }

  void GetValue(Message* msg) {
    // // parse keys
    // size_t n = 1;
    // K key = msg->task.key_channel();
    // K* key_ptr = &key;
    // if (msg->has_key()) {
    //   n = msg->key.size() / sizeof(V);
    //   key_ptr = (K*) msg->key.data();
    // } else {
    //   CHECK(msg->task.has_key_channel());
    // }

    // // parse values
    // CHECK_EQ(msg->task.dyn_val_len_size(), n);
    // size_t send_val_len = 0;
    // for (size_t i = 0; i < n; ++i) send_val_len += msg->task.dyn_val_len(i);
    // // TODO zero copy
    // SArray<V> send_val(send_val_len);

    // // fill values
    // int ts = msg->task.time();
    // size_t os = 0;
    // for (size_t i = 0; i < n; ++i) {
    //   int val_len = msg->task.dyn_val_len(i);
    //   Blob<V> send(send_val.data() + os, val_len);
    //   handle_.HandlePull(
    //       ts, Blob<const K>(key_ptr+i, 1),
    //       Blob<const V>(FindValue(key_ptr[i], ts, val_len), val_len), send);
    //   os += val_len;
    // }
    // msg->add_value(send_val);
  }

  void SetValue(const Message* msg) {
    // // parse keys
    // size_t n = 1;
    // K key = msg->task.key_channel();
    // K* key_ptr = &key;
    // if (msg->has_key()) {
    //   n = msg->key.size() / sizeof(V);
    //   key_ptr = (K*) msg->key.data();
    // } else {
    //   CHECK(msg->task.has_key_channel());
    // }

    // // parse values
    // CHECK_EQ(msg->task.dyn_val_len_size(), n);
    // CHECK_EQ(msg->value.size(), 1);
    // SArray<V> recv_val(msg->value[0]);

    // // get values
    // int ts = msg->task.time();
    // size_t os = 0;
    // for (size_t i = 0; i < n; ++i) {
    //   int val_len = msg->task.dyn_val_len(i);
    //   CHECK_GE(recv_val.size(), os + val_len);
    //   Blob<V> my_val(FindValue(key_ptr[i], ts, val_len), val_len);
    //   handle_.HandlePush(
    //       ts, Blob<const K>(key_ptr, 1), Blob<const V>(recv_val.data() + os, val_len),
    //       my_val);
    // }
  }

 private:
  V* FindValue(K key, int ts, int len) {
    // auto it = data_.find(key);
    // if (it == data_.end()) {
    //   // init if necessary
    //   V* data = new V[len];
    //   Blob<V> my_val(data, len);
    //   auto it2 = data_.insert(std::make_pair(key, my_val));
    //   CHECK(it2.second);
    //   it = it2.first;

    //   handle_.HandleInit(ts, Blob<const K>(&key, 1), my_val);
    // } else {
    //   CHECK_EQ(it->second.size, len);
    // }
    // return it->second.data;
  }

  std::unordered_map<K, Blob<V>> data_;
  Handle handle_;
};

}  // namespace ps
