/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_THREADSAFE_PQUEUE_H_
#define PS_INTERNAL_THREADSAFE_PQUEUE_H_
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <utility>
#include <vector>
#include "ps/base.h"
namespace ps {

/**
 * \brief thread-safe queue allowing push and waited pop
 */
class ThreadsafePQueue {
 public:
  ThreadsafePQueue() { }
  ~ThreadsafePQueue() { }

  /**
   * \brief push an value into the end. threadsafe.
   * \param new_value the value
   */
  void Push(Message new_value) {
    mu_.lock();
    queue_.push(std::move(new_value));
    mu_.unlock();
    cond_.notify_all();
  }

  /**
   * \brief wait until pop an element from the beginning, threadsafe
   * \param value the poped value
   */
  void WaitAndPop(Message* value) {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this]{return !queue_.empty();});
    *value = std::move(queue_.top());
    queue_.pop();
  }

 private:
  class Compare {
   public:
    bool operator()(const Message &l, const Message &r) {
      return l.meta.priority <= r.meta.priority;
    }
  };
  mutable std::mutex mu_;
  std::priority_queue<Message, std::vector<Message>, Compare> queue_;
  std::condition_variable cond_;
};

}  // namespace ps

#endif  // PS_INTERNAL_THREADSAFE_PQUEUE_H_
