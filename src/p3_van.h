/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_P3_VAN_H_
#define PS_P3_VAN_H_
#include <memory>
namespace ps {

/**
 * \brief P3 based Van implementation
 */
class P3Van : public ZMQVan {
 public:
  P3Van() {}
  virtual ~P3Van() {}

 protected:
  void Start(int customer_id) override {
    start_mu_.lock();
    if (init_stage == 0) {
      // start sender
      sender_thread_ = std::unique_ptr<std::thread>(
            new std::thread(&P3Van::Sending, this));
      init_stage++;
    }
    start_mu_.unlock();
    ZMQVan::Start(customer_id);
  }

  void Stop() override {
    ZMQVan::Stop();
    sender_thread_->join();
  }

  int SendMsg(const Message& msg) override {
    send_queue_.Push(msg);
    return 0;
  }

  void Sending() {
    while (true) {
      Message msg;
      send_queue_.WaitAndPop(&msg);
      ZMQVan::SendMsg(msg);
      if (!msg.meta.control.empty() &&
          msg.meta.control.cmd == Control::TERMINATE) {
        break;
      }
    }
  }

 private:
  /** the thread for sending messages */
  std::unique_ptr<std::thread> sender_thread_;
  ThreadsafePQueue send_queue_;
  int init_stage = 0;
};
}  // namespace ps

#endif  // PS_P3_VAN_H_
