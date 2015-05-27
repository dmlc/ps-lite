#pragma once
#include "kv/kv_store.h"
#include "dmlc/io.h"
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
    handle_.Start(false, ts, (void*)msg);
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
    CHECK_EQ(msg->value.size(), (size_t)1);
    SArray<V> val(msg->value[0]);
    CHECK_EQ(n * k_, val.size());
    int ts = msg->task.time();

    // handle this push request

    handle_.Start(true, ts, (void*)msg);

    V* val_data = val.data();
    for (size_t i = 0; i < n; ++i, val_data += k_) {
      K key_i = key[i];
      handle_.Push(Blob<const K>(&key_i, 1), Blob<const V>(val_data, k_),
                   Blob<V>(FindValue(key_i), val_len));
    }
    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }

  void SaveModel(const std::string& file) {
    std::string name = file + "_" + this->sys_.manager().van().my_node().id();
    dmlc::Stream *fs = dmlc::Stream::Create(name.c_str(), "w");
    {  // let os be destroied before delete fs
    dmlc::ostream os(fs);
    std::vector<V> val(k_);
    for (const auto& it : data_) {
      K key = it.first;
      handle_.Pull(Blob<const K>(&key, 1),
                   Blob<const V>(it.second, val_len),
                   Blob<V>(val.data(), k_));
      bool save = false;
      for (int i = 0; i < k_; ++i) if (val[i] != 0) { save = true; break; }
      if (!save) continue;
      os << key;
      for (int i = 0; i < k_; ++i) { os << "\t" << val[i]; }
      os << std::endl;
    }
    }
    delete fs;
    LOG(INFO) << "save model to " << name;
  }

 private:
  inline V* FindValue(K key) {
    auto it = data_.find(key);
    if (it == data_.end()) {
      V* val = data_[key];
      handle_.Init(Blob<const K>(&key, 1),
                   Blob<V>(val, val_len));
      return val;
    }
    return it->second;
  }
  // std::unordered_map<K, std::array<V, val_len>> data_;
  std::unordered_map<K, V[val_len]> data_;
  Handle handle_;
  int k_;
};
}  // namespace ps
