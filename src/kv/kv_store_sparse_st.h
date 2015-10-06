#pragma once
#include "kv/kv_store.h"
namespace ps {

template<typename K, typename E, typename V, typename Handle>
class KVStoreSparseST : public KVStore {
 public:
  KVStoreSparseST(int id, Handle handle, int pull_val_len)
      : KVStore(id), handle_(handle), k_(pull_val_len) {
    CHECK_GT(k_, 0);
  }

  virtual ~KVStoreSparseST() { }

  void Clear() override {
    data_.clear();
  }

  // process a pull message
  void HandlePull(Message* msg) {
    int ts = msg->task.time();
    handle_.Start(false, ts, msg->task.cmd(), (void*)msg);
    SArray<K> key(msg->key);
    size_t n = key.size();
    SArray<V> val(n * k_);
    bool dyn = msg->task.param().dyn_val_size();
    if (dyn) {
      SArray<int> val_size(n);
      size_t start = 0;
      for (size_t i = 0; i < n; ++i) {
        K key_i = key[i];
        size_t len = val.size() - start;
        while (len < (size_t)k_) {
          val.resize(val.size()*2 + 5); len = val.size() - start;
        }
        V* val_data = val.data() + start;
        Blob<V> pull(val_data, len);
        handle_.Pull(key_i, data_[key_i], pull);
        if (pull.data != val_data) {
          while ((start + pull.size) > val.size()) val.resize(val.size()*2 + 5);
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
        handle_.Pull(key_i, data_[key_i], pull);
        CHECK_EQ(pull.size, (size_t)k_) << "use dyanmic pull";
        if (pull.data != val_data) {
          memcpy(val_data, pull.data, sizeof(V)*k_);
        }
      }
      msg->add_value(val);
    }

    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }

  // process a push message
  void HandlePush(const Message* msg) {
    int ts = msg->task.time();
    handle_.Start(true, ts, msg->task.cmd(), (void*)msg);

    SArray<K> key(msg->key);
    size_t n = key.size();
    bool dyn = msg->task.param().dyn_val_size();

    if (dyn && n) {
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
        if (k == 0) continue;
        handle_.Push(key_i, Blob<const V>(val_data, k), data_[key_i]);
        val_data += k;
      }
    } else if (!dyn && n) {
      CHECK_EQ(msg->value.size(), (size_t)1);
      SArray<V> val(msg->value[0]);
      size_t k = val.size() / n;
      CHECK_EQ(k * n, val.size());

      V* val_data = val.data();
      for (size_t i = 0; i < n; ++i, val_data += k) {
        K key_i = key[i];
        handle_.Push(key_i, Blob<const V>(val_data, k), data_[key_i]);
      }
    }

    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }

  virtual void Load(dmlc::Stream *fi) {
    handle_.Load(fi);
    K key;
    while (true) {
      if (fi->Read(&key, sizeof(K)) != sizeof(K)) break;
      data_[key].Load(fi);
    }
    LOG(INFO) << "loaded " << data_.size() << " kv pairs";
  }

  virtual void Save(dmlc::Stream *fo) const {
    handle_.Save(fo);
    int saved = 0;
    for (const auto& it : data_) {
      if (it.second.Empty()) continue;
      fo->Write(&it.first, sizeof(K));
      it.second.Save(fo);
    }
    LOG(INFO) << "saved " << saved << " kv pairs";
  }

 private:
  std::unordered_map<K, E> data_;
  Handle handle_;
  int k_;
};
}  // namespace ps
