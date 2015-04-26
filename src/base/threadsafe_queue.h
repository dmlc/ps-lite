#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
namespace ps {

// TODO the code style is inconsistent with others
template<typename T> class ThreadsafeQueue {
 public:
  ThreadsafeQueue() {}

  void push(T new_value) {
    std::lock_guard<std::mutex> lk(mut);
    data_queue.push(std::move(new_value));
    data_cond.notify_all();
  }

  void wait_and_pop(T& value) {
    std::unique_lock<std::mutex> lk(mut);
    data_cond.wait(lk, [this]{return !data_queue.empty();});
    value = std::move(data_queue.front());
    data_queue.pop();
  }

  bool try_pop(T& value) {
    std::lock_guard<std::mutex> lk(mut);
    if(data_queue.empty())
      return false;
    value=std::move(data_queue.front());
    data_queue.pop();
    return true;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lk(mut);
    return data_queue.size();
  }

  bool empty() const {
    std::lock_guard<std::mutex> lk(mut);
    return data_queue.empty();
  }

 private:
  mutable std::mutex mut;
  std::queue<T> data_queue;
  std::condition_variable data_cond;
};

} // namespace ps

  // std::shared_ptr<T> wait_and_pop() {
  //   std::unique_lock<std::mutex> lk(mut);
  //   data_cond.wait(lk, [this]{return !data_queue.empty();});
  //   std::shared_ptr<T> res(
  //       std::make_shared<T>(std::move(data_queue.front())));
  //   data_queue.pop();
  //   return res;
  // }


  // std::shared_ptr<T> try_pop() {
  //   std::lock_guard<std::mutex> lk(mut);
  //   if(data_queue.empty())
  //     return std::shared_ptr<T>();
  //   std::shared_ptr<T> res(
  //       std::make_shared<T>(std::move(data_queue.front())));
  //   data_queue.pop();
  //   return res;
  // }
