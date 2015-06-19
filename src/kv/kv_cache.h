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
    if (cb) msg.callback = cb;
    msg.task.mutable_param()->set_push(true);
    sys_.manager().WaitServersReady();
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
    // kv.value = vals;
    // LL << ts << " " << pull_data_[ts].key << " " << kv.value;
    msg.set_key(kv.key);
    if (cb) {
      msg.callback = [chl, cb, this] {
        cb();
        mu_.lock();
        pull_data_.erase(chl);
        mu_.unlock();
      };
    } else {
      msg.callback = [chl, this] {
        mu_.lock();
        pull_data_.erase(chl);
        mu_.unlock();
      };
    }
    msg.task.set_key_channel(chl);
    msg.task.mutable_param()->set_push(false);
    sys_.manager().WaitServersReady();
    return Submit(&msg);
  }

  /// called by system ///

  void Slice(const Message& request, const std::vector<Range<Key>>& krs,
             std::vector<Message*>* msgs) {
    if (request.task.param().dyn_val_size()) {
      SliceDynValMessage<K>(request, krs, msgs);
    } else {
      SliceKOFVMessage<K>(request, krs, msgs);
    }
  }

  void ProcessResponse(Message* msg) {
    // only need to process pull response
    if (msg->task.param().push()) return;

    // received kv
    SArray<K> recv_key(msg->key);
    if (recv_key.empty()) return;
    CHECK_EQ(msg->value.size(), (size_t)1);
    SArray<V> recv_data(msg->value[0]);
    int k = recv_data.size() / recv_key.size();

    // local kv
    auto& kv = pull_data_[msg->task.key_channel()];
    CHECK_EQ(kv.value.size(), kv.key.size() * k);

    // match
    size_t n = ParallelOrderedMatch(
        recv_key, recv_data, kv.key, &kv.value, k, AssignOpType::ASSIGN);
    CHECK_EQ(n, recv_data.size());
  }

 private:
  struct KVPair {
    SArray<K> key;    // [key_0,  ..., key_n]
    SArray<V> value;  // [val_00, ..., val_0k, ..., val_n0, ..., val_nk]
  };
  std::unordered_map<int, KVPair> pull_data_;
  std::mutex mu_;
  int chl_ = 0;
  int split_val_ = 10000;
};
}  // namespace ps
