#pragma once
#include "base/common.h"
#include "proto/node.pb.h"
#include "system/message.h"
namespace ps {

/**
 * @brief Van sends (receives) packages to (from) a node The current
 * implementation uses ZeroMQ
 */
class Van {
 public:
  Van() { }
  ~Van();

  void Init();

  void Disconnect(const Node&  node);
  bool Connect(const Node&  node);

  bool Send(Message* msg, size_t* send_bytes);
  bool Recv(Message* msg, size_t* recv_bytes);

  static Node ParseNode(const string& node_str);

  Node& my_node() { return my_node_; }
  Node& scheduler() { return scheduler_; };

 private:
  // bind to my port
  void Bind();

  static void FreeData(void *data, void *hint) {
    if (hint == NULL) {
      delete [] (char*)data;
    } else {
      delete (SArray<char>*)hint;
    }
  }

  bool IsScheduler() { return my_node_.role() == Node::SCHEDULER; }

  // for scheduler: monitor the liveness of all other nodes
  // for other nodes: monitor the liveness of the scheduler
  void Monitor();

  void *context_ = nullptr;
  void *receiver_ = nullptr;
  Node my_node_;
  Node scheduler_;
  std::unordered_map<NodeID, void *> senders_;

  // for connection monitor
  std::unordered_map<int, NodeID> fd_to_nodeid_;
  std::mutex fd_to_nodeid_mu_;
  std::thread* monitor_thread_;

  DISALLOW_COPY_AND_ASSIGN(Van);
};

} // namespace ps
