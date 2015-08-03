#pragma once
#include <city.h>
#include "filter/filter.h"
// #include "base/crc32c.h"
namespace ps {

/// \brief Caches the key lists on both sender and receiver to avoid duplicated
/// communication
class KeyCachingFilter : public IFilter {
 public:
  // thread safe
  void Encode(Message* msg) {
    // if (!msg->task.has_key_range()) return;
    auto conf = Find(Filter::KEY_CACHING, msg);
    if (!conf) return;

    const auto& key = msg->key;
    if (key.size() < min_len_) {
      conf->clear_signature();
      return;
    }

    uint64 sig = FastHash(key);
    conf->set_signature(sig);

    Lock l(mu_);
    bool clear = conf->clear_cache() && IsDone(msg->task);
    auto it = cache_.find(sig);
    if (it != cache_.end() && it->second.size() == key.size()) {
      // hit cache
      msg->clear_key();
      if (clear) cache_.erase(it);
    } else {
      // not hit
      if (!clear) cache_[sig] = key;
    }
  }

  void Decode(Message* msg) {
    // if (!msg->task.has_key_range()) return;
    auto conf = Find(Filter::KEY_CACHING, msg);
    if (!conf || !conf->has_signature()) return;
    auto sig = conf->signature();
    // do a double check
    if (msg->has_key()) CHECK_EQ(FastHash(msg->key), sig);

    Lock l(mu_);
    bool clear = conf->clear_cache() && IsDone(msg->task);
    if (msg->has_key()) {
      if (!clear) cache_[sig] = msg->key;
    } else {
      // a lit bittle danger
      auto it = cache_.find(sig);
      CHECK(it != cache_.end()) << "invalid key cache";
      msg->set_key(it->second);
      if (clear) cache_.erase(it);
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

  std::unordered_map<uint64, SArray<char>> cache_;

  const size_t min_len_ = 64;
  const size_t max_sig_len_ = 4096;
  std::mutex mu_;
};

} // namespace
