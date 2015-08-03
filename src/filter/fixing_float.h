#pragma once
#include "filter/filter.h"
#include <time.h>
#include <limits>
namespace ps {

/// \brief Convert float/double into less-bit integer with quantizing and random rounding
class FixingFloatFilter : public IFilter {
 public:
  FixingFloatFilter() {
    CHECK_EQ(RAND_MAX, std::numeric_limits<int>::max());
    srand(time(NULL));
    pos_ = 0;
    rand_ = rand();
  }
  void Encode(Message* msg) {
    convert(msg, true);
  }

  void Decode(Message* msg) {
    convert(msg, false);
  }

 private:

  // Decode / Encode a message
  void convert(Message* msg, bool Encode) {
    auto filter_conf = CHECK_NOTNULL(Find(Filter::FIXING_FLOAT, msg));
    if (filter_conf->num_bytes() == 0) return;
    int n = msg->value.size();
    CHECK_EQ(n, msg->task.value_type_size());
    int k = 0;
    for (int i = 0; i < n; ++i) {
      if (msg->value[i].size() == 0) continue;
      auto type = msg->task.value_type(i);
      if (filter_conf->fixed_point_size() <= k) {
        filter_conf->add_fixed_point();
      }
      if (type == DataType::FLOAT) {
        msg->value[i] = convert<float>(
            msg->value[i], Encode, filter_conf->num_bytes(),
            filter_conf->mutable_fixed_point(k++));
      }
      if (type == DataType::DOUBLE) {
        msg->value[i] = convert<double>(
            msg->value[i], Encode, filter_conf->num_bytes(),
            filter_conf->mutable_fixed_point(k++));
      }
    }
  }

  // Decode / Encode an array
  template <typename V>
  SArray<char> convert(const SArray<char>& array, bool encode, int nbytes,
                       Filter::FixedFloatConfig* conf) {
    CHECK_GT(nbytes, 0);
    CHECK_LT(nbytes, 8);
    double ratio = static_cast<double>(1 << (nbytes*8)) - 2;

    if (encode) {
      double min_v = 1e20, max_v = -1e20;
      if (!conf->has_min_value() || !conf->has_max_value()) {
        SArray<V> vec(array);
        for (V& v : vec) {
          if (std::isnan(v)) v = 0;
          min_v = v < min_v ? v : min_v;
          max_v = v > max_v ? v : max_v;
        }
        // make it symmetric so it will not bias from 0 too much
        if (min_v * max_v < 0) {
          if (min_v > - max_v) min_v = - max_v;
          if (max_v < - min_v) max_v = - min_v;
        }
        // to avoid be divided by 0
        if (max_v == min_v) max_v = min_v + 1e-6;
        conf->set_min_value(min_v);
        conf->set_max_value(max_v);
      }
    }

    CHECK(conf->has_min_value());
    double min_v = static_cast<double>(conf->min_value());
    CHECK(conf->has_max_value());
    double max_v = static_cast<double>(conf->max_value());
    double bin = max_v - min_v;
    CHECK_GT(bin, 0);

    if (encode) {
      // float/double to nbytes*8 int
      SArray<V> orig(array);
      SArray<uint8> code(orig.size() * nbytes);
      uint8* code_ptr = code.data();
      // int seed = time(NULL);
      for (size_t i = 0; i < orig.size(); ++i) {
        double proj = orig[i] > max_v ? max_v : orig[i] < min_v ? min_v : orig[i];
        double tmp = (proj - min_v) / bin * ratio;
        uint64 r = static_cast<uint64>(floor(tmp)) + boolrand();
        for (int j = 0; j < nbytes; ++j) {
          *(code_ptr++) = static_cast<uint8>(r & 0xFF);
          r = r >> 8;
        }
      }
      return SArray<char>(code);
    } else {
      // nbytes*8 int to float/double
      uint8* code_ptr = SArray<uint8>(array).data();
      SArray<V> orig(array.size() / nbytes);
      for (size_t i = 0; i < orig.size(); ++i) {
        double r = 0;
        for (int j = 0; j < nbytes; ++j) {
          r += static_cast<uint64>(*(code_ptr++)) << 8 * j;
        }
        orig[i] = static_cast<V>(r / ratio * bin + min_v);
      }
      return SArray<char>(orig);
    }
  }

  // a fast random function
  bool boolrand() {
    if (pos_ == 30) {
      pos_ = 0;
      rand_ = rand();
    }
    return ((rand_ >> (pos_++)) & 1);
  }
  int pos_;
  int rand_;
};

} // namespace ps

  // static bool boolrand(int* seed) {
  //   *seed = (214013 * *seed + 2531011);
  //   return ((*seed >> 16) & 0x1) == 0;
  // }
