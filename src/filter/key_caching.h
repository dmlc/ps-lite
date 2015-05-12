#pragma once
#include <city.h>
#include "filter/filter.h"
// #include "base/crc32c.h"
namespace ps {

class KeyCachingFilter : public IFilter {
 public:
  // thread safe
  void Encode(Message* msg) {
    // if (!msg->task.has_key_range()) return;
    auto conf = Find(Filter::KEY_CACHING, msg);
    if (!conf) return;
    if (!msg->has_key()) {
      conf->clear_signature();
      return;
    }
    const auto& key = msg->key;
    uint64 sig = FastHash(key);
    conf->set_signature(sig);
    auto cache_k = std::make_pair(
        msg->task.key_channel(), Range<Key>(msg->task.key_range()));
    Lock l(mu_);
    auto& cache = cache_[cache_k];
    bool hit_cache = cache.first == sig && cache.second.size() == key.size();
    if (hit_cache) {
      msg->clear_key();
    } else {
      cache.first = sig;
      cache.second = key;
    }
    if (conf->clear_cache_if_done() && IsDone(msg->task)) {
      cache_.erase(cache_k);
    }
  }

  void Decode(Message* msg) {
    // if (!msg->task.has_key_range()) return;
    auto conf = Find(Filter::KEY_CACHING, msg);
    if (!conf || !conf->has_signature()) return;
    auto sig = conf->signature();
    // do a double check
    if (msg->has_key()) CHECK_EQ(FastHash(msg->key), sig);
    auto cache_k = std::make_pair(
        msg->task.key_channel(), Range<Key>(msg->task.key_range()));
    Lock l(mu_);
    auto& cache = cache_[cache_k];
    if (msg->has_key()) {
      cache.first = sig;
      cache.second = msg->key;
    } else {
      // the cache is invalid... may ask the sender to resend this task
      CHECK_EQ(sig, cache.first) << msg->DebugString();
      msg->set_key(cache.second);
    }
    if (conf->clear_cache_if_done() && IsDone(msg->task)) {
      cache_.erase(cache_k);
    }
  }

 private:
  bool IsDone(const Task& task) {
    return (!task.request() ||
            (task.has_param()
             && task.param().push()));
  }

  inline uint64 Hash64(const char* buf, size_t len) {
    return CityHash64(buf, len);
  }

  inline uint64 FastHash(const SArray<char>& arr) {
    if (arr.size() < max_sig_len_) {
      return Hash64(arr.data(), arr.size());
    }
    return (Hash64(arr.data(), max_sig_len_/2) ^
            Hash64(arr.data()+arr.size()-max_sig_len_/2, max_sig_len_/2));
  }

  std::unordered_map<
    std::pair<int, Range<Key>>, std::pair<uint32_t, SArray<char>>> cache_;

  // calculate the signature using the first max_sig_len_*4 bytes to accelerate
  // the computation

  const size_t max_sig_len_ = 1024;
  std::mutex mu_;
};

} // namespace
