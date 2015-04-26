#pragma once
#include "system/customer.h"
#include "base/parallel_ordered_match.h"
namespace ps {

template <typename K, typename V>
class KVCache : public Customer {
 public:
  KVCache(int id) : Customer(id) { }
  virtual ~KVCache() { }

  /// called by users ///

  inline int Push(const SBlob<K>& keys,
                  const SBlob<V>& values,
                  const SyncOpts& opts) {
    Message msg(ParseOption(opts), kServerGroup);
    msg.set_key(SArray<K>(keys));
    msg.add_value(SArray<V>(values));
    if (opts.callback) msg.callback = opts.callback;
    msg.task.mutable_param()->set_push(true);
    return Submit(&msg);
  }

  inline int Pull(const SBlob<Key>& keys,
                  SBlob<V>* values,
                  const SyncOpts& opts) {
    Message msg(ParseOption(opts), kServerGroup);
    int ts = msg.task.time();
    mu_.lock();
    auto& kv = pull_data_[ts];
    mu_.unlock();
    kv.key = SArray<K>(keys);
    kv.value = SArray<V>(*values);
    // LL << ts << " " << pull_data_[ts].key << " " << kv.value;
    msg.set_key(kv.key);
    msg.callback = [ts, opts, this] {
      if (opts.callback) opts.callback();
      mu_.lock();
      pull_data_.erase(ts);
      mu_.unlock();
    };
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
  Task ParseOption(const SyncOpts& opts) {
    Task req; req.set_request(true);
    req.set_time(exec_.time());
    for (int l : opts.deps) req.add_wait_time(l);
    for (const auto& f : opts.filters) req.add_filter()->CopyFrom(f);
    return req;
  }

  struct KVPair {
    SArray<K> key;    // [key_0,  ..., key_n]
    SArray<V> value;  // [val_00, ..., val_0k, ..., val_n0, ..., val_nk]
  };
  std::unordered_map<int, KVPair> pull_data_;
  std::mutex mu_;
  int split_val_ = 10000;
};
}  // namespace ps
