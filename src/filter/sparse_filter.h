#pragma once
#include "filter/filter.h"
namespace ps {

class SparseFilter : public IFilter {
 public:
  SparseFilter() {
    // use 0xffff..ff as the mark when a value is filtered, it is nan for float
    // and double.
    memcpy(&double_v_, &kuint64max, sizeof(double));
    memcpy(&float_v_, &kuint32max, sizeof(float));
  }

  // mark an entry as filtered
  void mark(float* v) { *v = float_v_; }
  void mark(double* v) { *v = double_v_; }

  // test whether or not an entry is filtered
  bool marked(double v) { return v != v; }
  bool marked(float v) { return v != v; }
 private:
  float float_v_;
  double double_v_;
};

} // namespace ps
