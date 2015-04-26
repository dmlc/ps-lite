#pragma once

#include <list>
#include <string>
#include <vector>
#include <functional>

#include <thread>
#include <mutex>
#include <condition_variable>

#include "base/macros.h"
#include "base/barrier.h"

namespace ps {

class ThreadPool {
 public:
  explicit ThreadPool(int num_workers)
    : num_workers_(num_workers), final_barrier_(num_workers + 1) {}
  ~ThreadPool();

  typedef std::function<void()> Task;
  void add(const Task& task);

  void startWorkers();

  // for internal use
  Task getNextTask();
  void stopOnFinalBarrier() { final_barrier_.Block(); }
 private:
  DISALLOW_COPY_AND_ASSIGN(ThreadPool);


  const int num_workers_;
  std::list<Task> tasks_;
  std::mutex mu_;
  std::condition_variable cv_;

  // std::unique_ptr<Barrier> final_barrier_;
  Barrier final_barrier_;
  std::vector<std::thread> all_workers_;

  bool waiting_to_finish_ = false;
  bool started_ = false;
};

}  // PS
