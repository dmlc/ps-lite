#pragma once

#include <mutex>
#include <condition_variable>
#include "base/macros.h"
#include "glog/logging.h"

namespace ps {

class Barrier {
 public:
  explicit Barrier(int num_threads)
      : num_to_block_(num_threads), num_to_exit_(num_threads) {}

  // return true if this is the last thread
  bool Block() {
    std::unique_lock<std::mutex> l(mu_);
    num_to_block_--;
    CHECK_GE(num_to_block_, 0);

    if (num_to_block_ > 0) {
      while (num_to_block_ > 0) cv_.wait(l);
    } else {
      cv_.notify_all();
    }

    num_to_exit_--;
    CHECK_GE(num_to_exit_, 0);
    return (num_to_exit_ == 0);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(Barrier);
  std::mutex mu_;
  std::condition_variable cv_;
  int num_to_block_;
  int num_to_exit_;
};


}  // PS
