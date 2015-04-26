#pragma once
#include "base/shared_array.h"
proto/evaluation.pb.h"

namespace ps {

// distributed auc
class AUC {
 public:
  void setGoodness(int64 goodness) { goodness_ = goodness; }

  // functions for the scheduler (or the root machine)
  // merge local results from a worker
  void merge(const AUCData& data) {
    CHECK_EQ(data.tp_key_size(), data.tp_count_size());
    for (size_t i = 0; i < data.tp_key_size(); ++i)
      tp_count_[data.tp_key(i)] += data.tp_count(i);

    CHECK_EQ(data.fp_key_size(), data.fp_count_size());
    for (size_t i = 0; i < data.fp_key_size(); ++i)
      fp_count_[data.fp_key(i)] += data.fp_count(i);

    // LL << tp_count_.size() << " " << fp_count_.size();
  }

  double accuracy(double threshold = 0) {
    double total = 0, correct = 0;
    double x = threshold * goodness_;
    for (auto& it : tp_count_) {
      if (it.first >= x) correct += it.second;
      total += it.second;
    }

    for (auto& it : fp_count_) {
      if (it.first < x) correct += it.second;
      total += it.second;
    }
    return (correct / total);
  }

  // evaluate the auc after merging all workers' results
  double evaluate() {
    if (tp_count_.empty() || fp_count_.empty()) return 0.5;
    double tp_sum = 0, fp_sum = 0, auc = 0;
    auto tp_it = tp_count_.begin();

    for (auto& fp_it : fp_count_) {
      auto fp_v = fp_it.second;
      for (; tp_it != tp_count_.end() && tp_it->second <= fp_v; ++ tp_it)
        tp_sum += tp_it->second;
      fp_sum += fp_v;
      auc += tp_sum * fp_v;
    }
    for (; tp_it != tp_count_.end(); ++tp_it) tp_sum += tp_it->second;

    // LL << tp_sum << " " << fp_sum;
    auc = auc / tp_sum / fp_sum;
    return (auc < .5 ? 1 - auc : auc);
  }

  // clear cached results of workers
  void clear() {
    tp_count_.clear();
    fp_count_.clear();
  }

  // worker: compute results using local data
  template<typename T>
  void compute(const SArray<T>& label, const SArray<T>& predict, AUCData* res) {
    CHECK_EQ(label.size(), predict.size());
    CHECK_GT(label.size(), 0);

    clear();
    for (size_t i = 0; i < predict.size(); ++i) {
      int64 k = (int64)(predict[i] * goodness_);
      if (label[i] > 0)
        ++ tp_count_[k];
      else
        ++ fp_count_[k];
    }


    // LL << goodness_ << " " << tp_count_.size() << " " << fp_count_.size();

    res->clear_tp_key();
    res->clear_tp_count();
    for (auto& it : tp_count_) {
      res->add_tp_key(it.first);
      res->add_tp_count(it.second);
    }

    res->clear_fp_key();
    res->clear_fp_count();
    for (auto& it : fp_count_) {
      res->add_fp_key(it.first);
      res->add_fp_count(it.second);
    }
  }
 private:
  int64 goodness_ = 1000;
  std::map<int64, uint64> fp_count_, tp_count_;

};

} // namespace ps
