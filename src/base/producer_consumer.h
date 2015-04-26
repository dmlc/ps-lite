#pragma once
#include "base/threadsafe_limited_queue.h"
#include "base/common.h"
namespace ps {

template<class V>
class ProducerConsumer {
 public:
  ProducerConsumer() { setCapacity(1000); }
  ProducerConsumer(int capacity_in_mb) { setCapacity(capacity_in_mb); }
  void setCapacity(int mb) { queue_.setMaxCapacity(mb*1000000); }

  // *func* returns false if finished, true otherwise
  void startProducer(const std::function<bool(V*, size_t*)>& func) {
    producer_thr_ = std::thread([this, func](){
        V entry;
        bool done = false;
        while (!done) {
          size_t size = 0;
          done = !func(&entry, &size);
          queue_.push(entry, size, done);
        }
      });
    producer_thr_.detach();
  }

  void startConsumer(const std::function<void(const V&)>& func) {
    consumer_thr_ = std::thread([this, func](){
        V entry;
        while (pop(&entry)) {
          func(entry);
        }
      });
    // consumer_thr_.detach();
  }
  void waitConsumer() { consumer_thr_.join(); }

  bool pop(V* data) {
    return queue_.pop(*data);
  }
  void push(const V& entry, size_t size = 1, bool finished = false) {
    queue_.push(entry, size, finished);
  }
  void setFinished() {
    V empty;
    queue_.push(empty, 0, true);
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(ProducerConsumer);
  ThreadsafeLimitedQueue<V> queue_;
  std::thread producer_thr_;
  std::thread consumer_thr_;
};
} // namespace ps
