#pragma once
#include "base/common.h"
#include "system/message.h"
#include "base/threadsafe_queue.h"
#include "system/manager.h"
namespace ps {

class Postoffice {
 public:
  SINGLETON(Postoffice);
  ~Postoffice();

  /**
   * @brief Starts the system
   */
  void Run(int* argc, char***);
  /**
   * @brief Stops the system
   */
  void Stop() { manager_.Stop(); }

  /**
   * @brief Queue a message into the sending buffer, which will be sent by the
   * sending thread. It is thread safe.
   *
   * @param msg it will be DELETE by system after sent successfully. so do NOT
   * delete it before
   */
  void Queue(Message* msg) { sending_queue_.push(msg); }

  Manager& manager() { return manager_; }

 private:
  Postoffice() { }
  void Send();
  void Recv();

  std::unique_ptr<std::thread> recv_thread_;
  std::unique_ptr<std::thread> send_thread_;
  ThreadsafeQueue<Message*> sending_queue_;

  Manager manager_;
  DISALLOW_COPY_AND_ASSIGN(Postoffice);
};

} // namespace ps
