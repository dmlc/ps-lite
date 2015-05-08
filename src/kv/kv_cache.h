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
                  const SArray<V>& vals, const Message::Callback& cb) {
    Message msg(req, kServerGroup);
    if (!req.has_time()) msg.task.set_time(exec_.time());
    msg.set_key(keys);
    msg.add_value(vals);
    if (cb) msg.callback = cb;
    msg.task.mutable_param()->set_push(true);
    return Submit(&msg);
  }

  inline int Pull(const Task& req, const SArray<K>& keys,
                  const SArray<V>& vals, const Message::Callback& cb) {
    Message msg(req, kServerGroup);
    if (!req.has_time()) msg.task.set_time(exec_.time());
    int ts = msg.task.time();
    mu_.lock();
    auto& kv = pull_data_[ts];
    mu_.unlock();
    kv.key = keys;
    kv.value = vals;
    // LL << ts << " " << pull_data_[ts].key << " " << kv.value;
    msg.set_key(kv.key);
    if (cb) {
      msg.callback = [ts, cb, this] {
        cb();
        mu_.lock();
        pull_data_.erase(ts);
        mu_.unlock();
      };
    } else {
      msg.callback = [ts, this] {
        mu_.lock();
        pull_data_.erase(ts);
        mu_.unlock();
      };
    }
    msg.task.mutable_param()->set_push(false);
    CHECK_EQ(ts, Submit(&msg));
    return ts;
  }

  /// called by system ///

  void Slice(const Message& request, const std::vector<Range<Key>>& krs,
             std::vector<Message*>* msgs) {
    SliceKOFVMessage<K>(request, krs, msgs);
  }

  void ProcessResponse(Message* msg) {
    // only need to process pull response
    if (msg->task.param().push()) return;

    // received kv
    SArray<K> recv_key(msg->key);
    if (recv_key.empty()) return;
    CHECK_EQ(msg->value.size(), 1);
    SArray<V> recv_data(msg->value[0]);
    int k = recv_data.size() / recv_key.size();

    // local kv
    auto& kv = pull_data_[msg->task.time()];
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
  int split_val_ = 10000;
};
}  // namespace ps
