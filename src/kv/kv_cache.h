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

  inline int Pull(const Task& req, const SArray<K>& keys,
                  std::vector<V>* vals, std::vector<int>* vals_size,
                  const Message::Callback& cb) {
    Message msg(req, kServerGroup);
    mu_.lock();
    int chl = chl_ ++;
    auto& kv = pull_data_[chl];
    mu_.unlock();
    kv.key = keys;
    kv.val = CHECK_NOTNULL(vals);
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
          LOG(ERROR) << "invalid pull probably due to server failure..";
        }
        if (kv.val->size()) {
          size_t len = 0;
          for (int i : *(kv.val_size)) len += i;
          CHECK_EQ(len, kv.val->size());
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
      kv.recv_num ++;
      SArray<K> recv_key(msg->key);
      if (recv_key.size()) {
        CHECK_EQ(msg->value.size(), (size_t)2);
        SArray<int> recv_size(msg->value[1]);
        size_t n = ParallelOrderedMatch(
            recv_key, recv_size, kv.key, kv.val_size, 1, AssignOpType::ASSIGN);
        CHECK_EQ(n, recv_size.size());
        kv.matched_num += n;
        kv.recv.push_back(std::make_pair(recv_key[0], SArray<V>(msg->value[0])));
      }

      if (kv.recv_num != sys_.manager().num_servers()) return;

      CHECK_EQ(kv.matched_num, kv.key.size());
      std::sort(kv.recv.begin(), kv.recv.end(), [](
          const std::pair<K, SArray<V>>& a, const std::pair<K, SArray<V>>& b) {
                  return a.first < b.first;
                });
      size_t len = 0;
      for (const auto& l : kv.recv) len += l.second.size();
      kv.val->resize(len);
      V* ptr = kv.val->data();
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
      size_t n = ParallelOrderedMatch(
          recv_key, recv_data, kv.key, kv.val, k, AssignOpType::ASSIGN);
      CHECK_EQ(n, recv_data.size());
    }

  }

 private:
  struct KVPair {
    // [key_0,  ..., key_n]
    SArray<K> key;
    // constant value size:
    //   [val_00, ..., val_0k, ..., val_n0, ..., val_nk]
    // dynamic value size:
    //   [val_00, ...val_0,val_size[0], ..., val_n0, ..., val_n,val_size[n]
    std::vector<V>* val;
    std::vector<int>* val_size = NULL;

    // for match dynamic vals
    std::vector<std::pair<K, SArray<V>>> recv;

    int recv_num = 0;
    // std::vector<K> recv_key;
    size_t matched_num = 0;
  };

  std::unordered_map<int, KVPair> pull_data_;
  std::mutex mu_;
  int chl_ = 0;
};

}  // namespace ps
