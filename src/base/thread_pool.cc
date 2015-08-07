#include "base/thread_pool.h"

namespace ps {

ThreadPool::~ThreadPool() {
  if (!started_) return;

  mu_.lock();
  waiting_to_finish_ = true;
  mu_.unlock();

  worker_cond_.notify_all();
  Wait();

  for (int i = 0; i < num_workers_; ++i) {
    all_workers_[i].join();
  }
}

void ThreadPool::Add(const Task& task) {
  std::lock_guard<std::mutex> l(mu_);
  tasks_.push_back(std::move(task));
  if (started_) worker_cond_.notify_one();
}

void ThreadPool::Wait() {
  std::unique_lock<std::mutex> l(mu_);
  fin_cond_.wait(l, [this]{ return Done(); });
}

typename ThreadPool::Task ThreadPool::GetNextTask() {
  std::unique_lock<std::mutex> l(mu_);
  for (;;) {
    if (!tasks_.empty()) {
      auto task = std::move(tasks_.front());
      tasks_.pop_front();
      ++ num_running_tasks_;
      return task;
    }
    if (waiting_to_finish_) {
      break;
    } else {
      worker_cond_.wait(l);
    }
  }
  return Task();
}

void ThreadPool::FinishTask() {
  std::lock_guard<std::mutex> l(mu_);
  -- num_running_tasks_;
  CHECK_GE(num_running_tasks_, 0);
  if (Done()) fin_cond_.notify_all();
}

void ThreadPool::RunWorker() {
  auto task = GetNextTask();
  while (task) {
    task();
    FinishTask();
    task = GetNextTask();
  }
}

void ThreadPool::StartWorkers() {
  started_ = true;
  for (int i = 0; i < num_workers_; ++i) {
    all_workers_.push_back(std::move(std::thread(&ThreadPool::RunWorker, this)));
  }
}

}  // namespace ps
