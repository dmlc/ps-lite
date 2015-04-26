#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include "base/common.h"

namespace ps {

template<typename T>
class ThreadsafeLimitedQueue {
 public:
  ThreadsafeLimitedQueue() { }
  ThreadsafeLimitedQueue(size_t capacity) { setMaxCapacity(capacity); }
  void setMaxCapacity(size_t capacity) { max_capacity_ = capacity; }

  void push(const T& value, size_t capacity, bool finished = false) {
    CHECK(!done_) << "must not call push again if *finished* is set true";
    if (capacity > max_capacity_) {
      LL << "push obj with size " << capacity
         << " into queue with capacity " << max_capacity_
         << ". you will be blocked here forever...";
    }
    // do not insert
    if (finished == false && capacity == 0) return;
    std::unique_lock<std::mutex> l(mu_);
    full_cond_.wait(l, [this, capacity]{
        return (capacity + cur_capacity_ <= max_capacity_); });
    queue_.push(std::move(std::make_pair(value, capacity)));
    cur_capacity_ += capacity;
    done_ = finished;
    empty_cond_.notify_all();
  }

  bool pop(T& value) {
    std::unique_lock<std::mutex> l(mu_);
    // already finished
    if (done_ && queue_.empty()) return false;

    empty_cond_.wait(l, [this]{ return !queue_.empty(); });
    std::pair<T, size_t> e = std::move(queue_.front());

    // an empty item, which is inserted only when finished
    if (e.second == 0) {
      CHECK(done_);
      return false;
    }

    // get a valid item
    value = std::move(e.first);
    cur_capacity_ -= e.second;
    queue_.pop();
    full_cond_.notify_all();
    return true;
  }

  size_t size() const {
    std::lock_guard<std::mutex> l(mu_);
    return queue_.size();
  }

  bool empty() const {
    return size() == 0;
  }

 private:
  mutable std::mutex mu_;
  bool done_ = false;
  size_t max_capacity_ = 0, cur_capacity_ = 0;
  std::queue<std::pair<T, size_t> > queue_;
  std::condition_variable empty_cond_, full_cond_;
};
} // namespace ps
