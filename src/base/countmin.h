#pragma once
#include "base/sketch.h"
#include <math.h>
#include "base/shared_array_inl.h"
namespace ps {

template <typename K, typename V>
class CountMin : public Sketch {
 public:
  // TODO prefetch to accelerate the memory access
  bool empty() { return n_ == 0; }
  void clear() { data_.clear(); n_ = 0; }
  void resize(int n, int k, V v_max) {
    n_ = std::max(n, 64);
    data_.resize(n_);
    data_.SetZero();
    k_ = std::min(30, std::max(1, k));
    v_max_ = v_max;
  }

  void insert(const K& key, const V& count) {
    uint32 h = hash(key);
    const uint32 delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (int j = 0; j < k_; ++j) {
      V v = data_[h % n_];
      // to avoid overflow
      data_[h % n_] = count > v_max_ - v ? v_max_ : v + count;
      h += delta;
    }
  }

  V query(const K& key) const {
    V res = v_max_;
    uint32 h = hash(key);
    const uint32 delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (int j = 0; j < k_; ++j) {
      res = std::min(res, data_[h % n_]);
      h += delta;
    }
    return res;
  }

 private:
  SArray<V> data_;
  int n_ = 0;
  int k_ = 1;
  V v_max_ = 0;
};

} // namespace ps
