#pragma once
#include "filter/filter.h"
#include <limits>
namespace ps {

/// \brief Convert float/double into less-bit integer by projecting into
/// [int_min, int_max].
class TruncateFloatFilter : public IFilter {
 public:
  void Encode(Message* msg) {
    Convert(msg, true);
  }

  void Decode(Message* msg) {
    Convert(msg, false);
  }
 private:
  // Decode / Encode a message
  void Convert(Message* msg, bool encode) {
    auto filter_conf = CHECK_NOTNULL(Find(Filter::TRUNCATE_FLOAT, msg));
    int nbytes = filter_conf->num_bytes();
    if (nbytes == 0) return;

    int n = msg->value.size();
    CHECK_EQ(n, msg->task.value_type_size());
    for (int i = 0; i < n; ++i) {
      if (msg->value[i].size() == 0) continue;
      auto type = msg->task.value_type(i);

      if (type == DataType::FLOAT) {
        msg->value[i] = Convert<float>(msg->value[i], encode, nbytes);
      } else if (type == DataType::DOUBLE) {
        msg->value[i] = Convert<double>(msg->value[i], encode, nbytes);
      }
    }
  }

  template <typename Real>
  SArray<char> Convert(const SArray<char>& array, bool encode, int nbytes) {
    if (nbytes == 1) {
      return Convert<Real, uint8>(array, encode);
    } else if (nbytes == 2) {
      return Convert<Real, uint16>(array, encode);
    } else if (nbytes == 4) {
      return Convert<Real, uint32>(array, encode);
    } else if (nbytes == -1) {
      return Convert<Real, int8>(array, encode);
    } else if (nbytes == -2) {
      return Convert<Real, int16>(array, encode);
    } else if (nbytes == -4) {
      return Convert<Real, int32>(array, encode);
    } else {
      LOG(FATAL) << "unsupported num_bytes: " << nbytes;
      return SArray<char>();
    }
  }


  template <typename Real, typename Int>
  SArray<char> Convert(const SArray<char>& array, bool encode) {
    if (encode) {
      SArray<Real> in(array);
      SArray<Int> out(in.size());
      Real max_v = static_cast<Real>(std::numeric_limits<Int>::max());
      Real min_v = static_cast<Real>(std::numeric_limits<Int>::min());
      for (size_t i = 0; i < in.size(); ++i) {
        Real v = in[i];
        Real proj = v > max_v ? max_v : v < min_v ? min_v : v;
        out[i] = static_cast<Int>(proj);
      }
      return SArray<char>(out);
    } else {
      SArray<Int> in(array);
      SArray<Real> out(in.size());
      for (size_t i = 0; i < in.size(); ++i) {
        out[i] = static_cast<Real>(in[i]);
      }
      return SArray<char>(out);
    }
  }

};
} // namespace ps
