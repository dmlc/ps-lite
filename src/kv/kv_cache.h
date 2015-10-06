#pragma once
#include "ps/shared_array.h"
#include "ps/app.h"
#include "base/parallel_ordered_match.h"
namespace ps {

template <typename K, typename V>
class KVCache : public Customer {
 public:
  KVCache(int id) : Customer(id) { }
  virtual ~KVCache() { }

  /// called by users ///

  inline int Push(const Task& req, const SArray<K>& keys,
                  const SArray<V>& vals, const SArray<int>& vals_size,
                  const Message::Callback& cb) {
    CHECK(IsKeysOrderd(keys)) << "keys must in non-decreasing order";
    Message msg(req, kServerGroup);
    msg.set_key(keys);
    msg.add_value(vals);
    if (vals_size.size()) {
      CHECK_EQ(vals_size.size(), keys.size());
      msg.add_value(vals_size);
      msg.task.mutable_param()->set_dyn_val_size(true);
    }
    if (cb) msg.callback = cb;
    msg.task.mutable_param()->set_push(true);
    return Submit(&msg);
  }

  /**
   * vals should be pre-allocated with length len_vals. if you don't know the
   * length at begining, use the next Pull
   *
   * if (vals_size != NULL) then use dynamic value length, otherwise it should
   * be preallocated with length keys.size().
   *
   */
  inline int Pull(const Task& req, const SArray<K>& keys,
                  const Message::Callback& cb,
                  V* vals, size_t len_vals,
                  int* vals_size) {
    return Pull_(req, keys, cb, vals, len_vals, NULL, vals_size);
  }

  /**
   * if (vals_size != NULL) then use dynamic value length
   * both vals and vals_size can be empty, we will allocate the memory
   */
  inline int Pull(const Task& req, const SArray<K>& keys,
                  const Message::Callback& cb,
                  std::vector<V>* vals, std::vector<int>* vals_size) {

    if (vals_size && vals_size->empty()) {
      vals_size->resize(keys.size());
    }

    return Pull_(req, keys, cb, NULL, 0, vals,
                 (vals_size == NULL ? NULL : vals_size->data()));
  }

  /// called by system ///

  void Slice(const Message& request, const std::vector<Range<Key>>& krs,
             std::vector<Message*>* msgs) {
    SliceMessage<K>(request, krs, msgs, request.task.param().dyn_val_size());
  }

  void ProcessResponse(Message* msg) {
    // only need to process pull response
    if (msg->task.param().push()) return;

    // received kv
    mu_.lock();
    auto& kv = pull_data_[msg->task.key_channel()];
    mu_.unlock();

    if (msg->task.param().dyn_val_size()) {
      SArray<K> recv_key(msg->key);
      if (recv_key.size()) {
        CHECK_EQ(msg->value.size(), (size_t)2);
        SArray<int> recv_size(msg->value[1]);
        SArray<int> val_size(CHECK_NOTNULL(kv.val_size), kv.key.size(), EmptyDel<int>());
        size_t n = ParallelOrderedMatch(
            recv_key, recv_size, kv.key, &val_size, 1, AsOp::ASSIGN);
        CHECK_EQ(n, recv_size.size());
        kv.matched_num += n;
        kv.recv.push_back(std::make_pair(recv_key[0], SArray<V>(msg->value[0])));
      }

      kv.recv_num ++;
      if (kv.recv_num != sys_.manager().num_servers()) return;

      // CHECK_EQ(kv.matched_num, kv.key.size());
      std::sort(kv.recv.begin(), kv.recv.end(), [](
          const std::pair<K, SArray<V>>& a, const std::pair<K, SArray<V>>& b) {
                  return a.first < b.first;
                });
      size_t len = 0;
      for (const auto& l : kv.recv) len += l.second.size();
      V* ptr = NULL;
      if (kv.val_vec) {
        kv.val_vec->resize(len);
        ptr = kv.val_vec->data();
      } else {
        CHECK_EQ(kv.len_val, len);
        ptr = kv.val;
      }
      for (const auto& l : kv.recv) {
        memcpy(ptr, l.second.data(), l.second.size() * sizeof(V));
        ptr += l.second.size();
      }
      kv.recv.clear();
    } else {
      SArray<K> recv_key(msg->key);
      if (recv_key.empty()) return;

      CHECK_EQ(msg->value.size(), (size_t)1);
      SArray<V> recv_data(msg->value[0]);
      int k = recv_data.size() / recv_key.size();

      size_t n = 0;
      if (kv.val_vec) {
        n = ParallelOrderedMatch(
            recv_key, recv_data, kv.key, kv.val_vec, k, AsOp::ASSIGN);
      } else {
        CHECK_EQ(kv.key.size() * k, kv.len_val);
        SArray<V> my_val(kv.val, kv.len_val, EmptyDel<V>());
        n = ParallelOrderedMatch(
            recv_key, recv_data, kv.key, &my_val, k, AsOp::ASSIGN);
      }
      CHECK_EQ(n, recv_data.size());
    }
  }

 private:
  inline int Pull_(const Task& req, const SArray<K>& keys,
                   const Message::Callback& cb,
                   V* vals, size_t len_vals, std::vector<V>* vals_vec,
                   int* vals_size) {
    CHECK(IsKeysOrderd(keys)) << "keys must in non-decreasing order";
    Message msg(req, kServerGroup);

    mu_.lock();
    int chl = chl_ ++;
    auto& kv = pull_data_[chl];
    mu_.unlock();
    kv.key = keys;
    kv.val = vals;
    kv.len_val = len_vals;
    kv.val_vec = vals_vec;
    bool dyn_val = false;
    if (vals_size) {
      dyn_val = true;
      msg.task.mutable_param()->set_dyn_val_size(true);
      kv.val_size = vals_size;
    }
    // LL << ts << " " << pull_data_[ts].key << " " << kv.value;
    msg.callback = [chl, cb, dyn_val, this] {
      if (dyn_val) {
        // do a double check
        mu_.lock();
        auto& kv = pull_data_[chl];
        mu_.unlock();
        if (kv.matched_num != kv.key.size()) {
          LOG(WARNING) << "unmatched " << kv.matched_num << " vs " << kv.key.size();
        }
        CHECK(kv.val_size != NULL);
        size_t len = 0;
        for (size_t i = 0; i < kv.key.size(); ++i) len += kv.val_size[i];
        if (kv.val_vec) {
          CHECK_EQ(kv.val_vec->size(), len);
        } else {
          CHECK_EQ(kv.len_val, len);
        }
      }
      if (cb) cb();
      mu_.lock();
      pull_data_.erase(chl);
      mu_.unlock();
    };
    msg.set_key(keys);
    msg.task.set_key_channel(chl);
    msg.task.mutable_param()->set_push(false);
    return Submit(&msg);
  }

  inline bool IsKeysOrderd(const SArray<K>& keys) {
    for (size_t i = 0; i < keys.size() -1 ; ++i) {
      if (keys[i+1] < keys[i]) { return false; }
    }
    return true;
  }

  struct KVPair {
    // [key_0,  ..., key_n]
    SArray<K> key;
    // constant value size:
    //   [val_00, ..., val_0k, ..., val_n0, ..., val_nk]
    // dynamic value size:
    //   [val_00, ...val_0,val_size[0], ..., val_n0, ..., val_n,val_size[n]
    V* val = NULL ;
    size_t len_val = 0;  // length of val
    std::vector<V>* val_vec = NULL;  // allocator fo val

    int* val_size = NULL;

    // for match dynamic vals
    std::vector<std::pair<K, SArray<V>>> recv;

    int recv_num = 0;
    size_t matched_num = 0;
  };

  std::unordered_map<int, KVPair> pull_data_;
  std::mutex mu_;
  int chl_ = 0;
};

}  // namespace ps
