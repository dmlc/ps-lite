#pragma once
#include "filter/filter.h"
#ifdef __MACH__
#include <random>
#endif

namespace ps {

/**
 * @brief Add gaussian noises to float/double values
 */
class AddNoiseFilter : public IFilter {
 public:
  void Encode(Message* msg) {
    auto filter_conf = CHECK_NOTNULL(Find(Filter::NOISE, msg));
    int n = msg->value.size();
    CHECK_EQ(n, msg->task.value_type_size());
    for (int i = 0; i < n; ++i) {
      auto type = msg->task.value_type(i);
      if (type == DataType::FLOAT) {
        AddNoise<float>(msg->value[i], filter_conf);
      }
      if (type == DataType::DOUBLE) {
        AddNoise<double>(msg->value[i], filter_conf);
      }
    }
  }

 private:

  template <typename V>
  void AddNoise(const SArray<char>& array, Filter* cf) {
    std::default_random_engine generator;
    std::normal_distribution<V> distribution((V)cf->mean(), (V)cf->std());
    SArray<V> data(array);
    for (size_t i = 0; i < data.size(); ++i) {
      data[i] += distribution(generator);
    }
  }

};

}  // namespace ps
