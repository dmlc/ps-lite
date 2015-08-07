#pragma once

#include <list>
#include <string>
#include <vector>
#include <functional>

#include <thread>
#include <mutex>
#include <condition_variable>

#include "ps/base.h"

namespace ps {

class ThreadPool {
 public:
  explicit ThreadPool(int num_workers)
      : num_workers_(num_workers) {}

  /// \brief Guarantee all tasks have been finished if \ref StartWorkers has
  /// been called
  ~ThreadPool();

  /// \brief A task type
  typedef std::function<void()> Task;

  /**
   * \brief Start all worker threads. Tasks will not be executed before calling
   * this function
   */
  void StartWorkers();

  /**
   * \brief Add a task to this pool
   */
  void Add(const Task& task);


  /**
   * \brief Block the caller until all tasked added before have been
   * finished
   */
  void Wait();

 private:
  DISALLOW_COPY_AND_ASSIGN(ThreadPool);

  /// \brief Get next task, for internal use
  Task GetNextTask();

  /// \brief Finished one task, for internal use
  void FinishTask();

  void RunWorker();

  bool Done() { return tasks_.empty() && num_running_tasks_ == 0; }

  const int num_workers_;
  std::list<Task> tasks_;
  std::mutex mu_;
  std::condition_variable worker_cond_, fin_cond_;

  std::vector<std::thread> all_workers_;

  bool waiting_to_finish_ = false;
  bool started_ = false;
  int num_running_tasks_ = 0;
};

}  // PS
