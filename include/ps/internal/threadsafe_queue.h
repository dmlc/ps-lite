/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_THREADSAFE_QUEUE_H_
#define PS_INTERNAL_THREADSAFE_QUEUE_H_
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include "ps/base.h"
#include "spsc_queue.h"

namespace ps {

/**
 * \brief thread-safe queue allowing push and waited pop
 */
template <typename T>
class ThreadsafeQueue {
 public:
  ThreadsafeQueue() : lockless_queue_(32768) {
    auto lockless_str = getenv("DMLC_LOCKLESS_QUEUE");
    lockless_ = lockless_str ? atoi(lockless_str) : false;
    auto polling_str = getenv("DMLC_POLLING_IN_NANOSECOND");
    int polling_duration_int = polling_str ? atoi(polling_str) : 1000;
    polling_duration_ = std::chrono::nanoseconds(polling_duration_int);
  }

  ~ThreadsafeQueue() {}

  /**
   * \brief push an value into the end. threadsafe.
   * \param new_value the value
   */
  void Push(T new_value) {
    if (lockless_) {
      PushLockless(std::move(new_value));
      return;
    }
    mu_.lock();
    queue_.push(std::move(new_value));
    mu_.unlock();
    cond_.notify_all();
  }

  /**
   * \brief wait until pop an element from the beginning, threadsafe
   * \param value the poped value
   */
  void WaitAndPop(T* value) {
    if (lockless_) {
      WaitAndPopLockless(value);
      return;
    }
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this] { return !queue_.empty(); });
    *value = std::move(queue_.front());
    queue_.pop();
  }

  /**
   * \brief peek queue size
   */
  int Size() {
    if (lockless_) {
      return SizeLockless();
    }
    std::unique_lock<std::mutex> lk(mu_);
    return queue_.size();
  }

 private:
  // lockless impl
  void PushLockless(T new_value) {
    write_mu_.lock();
    lockless_queue_.push(std::move(new_value));
    write_mu_.unlock();
  }

  void WaitAndPopLockless(T* value) {
    auto t = std::chrono::high_resolution_clock::now() + polling_duration_;
    for (;;) {
      read_mu_.lock();
      if (lockless_queue_.front()) {
        *value = *(lockless_queue_.front());
        lockless_queue_.pop();
        read_mu_.unlock();
        break;
      }
      read_mu_.unlock();
      if (std::chrono::high_resolution_clock::now() < t) {
        std::this_thread::yield();
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
    }
  }

  int SizeLockless() {
    std::unique_lock<std::mutex> lk_read(read_mu_);
    std::unique_lock<std::mutex> lk_write(write_mu_);
    return lockless_queue_.size();
  }

  bool lockless_;

  // cv implementation
  mutable std::mutex mu_;
  std::queue<T> queue_;
  std::condition_variable cond_;

  // lockless implementation
  mutable std::mutex read_mu_;
  mutable std::mutex write_mu_;
  rigtorp::SPSCQueue<T> lockless_queue_;
  std::chrono::nanoseconds polling_duration_;
};

}  // namespace ps

#endif  // PS_INTERNAL_THREADSAFE_QUEUE_H_
