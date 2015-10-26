#pragma once
#include <unordered_map>
#include <mutex>
#include <string>
#include <thread>
#include <memory>
#include <atomic>
#include "ps/base.h"

#include "ps/internal/message.h"
#include "ps/internal/node.pb.h"

namespace ps {

/**
 * \brief Van sends messages to remote nodes
 */
class Van {
 public:
  /** \brief constructer, do nothing. use \ref Start for real things */
  Van() { }

  /**\brief deconstructer, do nothing. use \ref Stop for real staff */
  ~Van() { }

  /**
   * \brief start van
   *
   * must call it before calling Send
   * initalize all connections to other nodes
   * start the receiving and monitoring threads.
   *
   * the former keeps receiving messages. if it is a system control message,
   * give it to postoffice::manager, otherwise, give it to the accoding app
   *
   * for the latter, if this is a scheduler node, then monitors the liveness
   * other nodes. otherwise, monitor the liveness of the scheduler
   */
  void Start();

  /**
   * \brief send a message, thread-safe
   * \return the number of bytes sent. -1 if failed
   */
  int Send(const Message& msg) {
    CHECK(ready_) << "call Start() first";
    return Send_(msg);
  }

  /**
   * \brief return my node
   */
  const Node& my_node() const {
    CHECK(ready_) << "call Start() first";
    return my_node_;
  }


  /**
   * \brief stop van
   *
   * stop both receiving and monitoring threads
   */
  void Stop();

 private:
  /**
   * \return interal version without ready check
   */
  int Send_(const Message& msg);

  /**
   * return the node id given the received identity
   * \return -1 if not find
   */
  int GetNodeID(const char* buf, size_t size);

  /**
   * \brief connect to a node
   */
  void Connect(const Node& node);

  /**
   * \brief receive a packge
   * \return the number of bytes received. -1 if failed
   */
  int Recv(Message* msg);

  /**
   * thread function for receving
   */
  void Receiving();

  /**
   * thread function for monioring
   */
  void Monitoring();

  void *context_ = nullptr;
  void *receiver_ = nullptr;

  Node scheduler_;
  Node my_node_;
  bool is_scheduler_;

  /**
   * whether it is ready for sending
   */
  std::atomic<bool> ready_{false};

  /**
   * in exiting if true
   */
  std::atomic<bool> exit_{true};

  std::mutex mu_;

  size_t send_bytes_ = 0;
  size_t recv_bytes_ = 0;

int num_servers_ = 0;
int num_workers_ = 0;

  /**
   * \brief node_id to the socket for sending data to this node
   */
  std::unordered_map<int, void *> senders_;

  /**
   * the thread for monitoering node liveness
   */
  std::unique_ptr<std::thread> monitor_thread_;

  /**
   * the thread for receiving messages
   */

  std::vector<int> barrier_count_;

  std::unique_ptr<std::thread> receiver_thread_;
  DISALLOW_COPY_AND_ASSIGN(Van);
};
}  // namespace ps
