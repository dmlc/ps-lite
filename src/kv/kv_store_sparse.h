#pragma once
#include "kv/kv_store.h"
#include "dmlc/io.h"
namespace ps {

template<typename K, typename E, typename V, typename Handle>
class KVStoreSparse : public KVStore {
 public:
  KVStoreSparse(int id, Handle handle, int pull_val_len)
      : KVStore(id), handle_(handle),
        k_(abs(pull_val_len)), dyn_pull_(pull_val_len < 0) {
    CHECK_NE(k_, 0);
    handle_.SetCaller(this);
  }

  virtual ~KVStoreSparse() { }

  // process a pull message
  void GetValue(Message* msg) {
    int ts = msg->task.time();
    handle_.Start(false, ts, msg->task.cmd(), (void*)msg);
    SArray<K> key(msg->key);
    size_t n = key.size();
    SArray<V> val(n * k_);

    if (dyn_pull_) {
      SArray<int> val_size(n);
      size_t start = 0;
      for (size_t i = 0; i < n; ++i) {
        K key_i = key[i];
        size_t len = val.size() - start;
        V* val_data = val.data() + start;
        Blob<V> pull(val_data, len);
        handle_.Pull(key_i, FindValue(key_i), pull);
        if (pull.data != val_data) {
          while ((start + pull.size) < val.size()) val.resize(val.size()*2 + 5);
          memcpy(val.data()+start, pull.data, sizeof(V)*pull.size);
        } else {
          CHECK_LE(pull.size, len);
        }
        start += pull.size;
        val_size[i] = pull.size;
      }
      val.resize(start);
      msg->add_value(val);
      msg->add_value(val_size);
    } else {
      V* val_data = val.data();
      for (size_t i = 0; i < n; ++i, val_data += k_) {
        K key_i = key[i];
        Blob<V> pull(val_data, k_);
        handle_.Pull(key_i, FindValue(key_i), pull);
      }
      msg->add_value(val);
    }

    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }

  // process a push message
  void SetValue(const Message* msg) {
    int ts = msg->task.time();
    handle_.Start(true, ts, msg->task.cmd(), (void*)msg);

    SArray<K> key(msg->key);
    size_t n = key.size();

    if (dyn_pull_) {
      CHECK_EQ(msg->value.size(), (size_t)2);
      SArray<V> val(msg->value[0]);
      SArray<int> val_size(msg->value[1]);
      CHECK_EQ(val_size.size(), n);
      size_t len = 0;
      for (int i : val_size) len += i;
      CHECK_EQ(len, val.size());

      V* val_data = val.data();
      for (size_t i = 0; i < n; ++i) {
        K key_i = key[i];
        size_t k = val_size[i];
        handle_.Push(key_i, Blob<const V>(val_data, k), FindValue(key_i));
        val_data += k;
      }
    } else {
      CHECK_EQ(msg->value.size(), (size_t)1);
      SArray<V> val(msg->value[0]);
      size_t k = val.size() / n;
      CHECK_EQ(k * n, val.size());

      V* val_data = val.data();
      for (size_t i = 0; i < n; ++i, val_data += k) {
        K key_i = key[i];
        handle_.Push(key_i, Blob<const V>(val_data, k), FindValue(key_i));
      }
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
      for (auto& it : data_) {
        K key = it.first;
        Blob<V> pull(val.data(), k_);
        handle_.Pull(key, it.second, pull);

        bool save = false;
        for (size_t i = 0; i < pull.size; ++i) if (pull[i] != 0) { save = true; break; }
        if (!save) continue;
        os << key;
        if (dyn_pull_) os << "\t" << pull.size;
        for (int i = 0; i < k_; ++i) { os << "\t" << val[i]; }
        os << std::endl;
      }
    }
    delete fs;
    LOG(INFO) << "save model to " << name;
  }

 private:
  inline E& FindValue(K key) {
    auto it = data_.find(key);
    if (it == data_.end()) {
      E& val = data_[key];
      handle_.Init(key, val);
      return val;
    }
    return it->second;
  }

  std::unordered_map<K, E> data_;
  Handle handle_;
  int k_;
  bool dyn_pull_;
};
}  // namespace ps
