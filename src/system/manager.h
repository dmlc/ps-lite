#pragma once
#include "base/common.h"
#include "proto/node.pb.h"
#include "proto/task.pb.h"
#include "system/van.h"
#include "system/env.h"
#include "system/node_assigner.h"
#include "system/network_usage.h"
namespace ps {

class App;
class Customer;

class Manager {
 public:
  Manager();
  ~Manager();

  void Init(int argc, char *argv[]);
  void Run();
  void Stop();
  bool Process(Message* msg);

  // manage nodes
  void AddNode(const Node& node);
  void RemoveNode(const NodeID& node_id);
  // detect that *node_id* is disconnected
  void NodeDisconnected(const NodeID node_id);
  // add a function handler which will be called in *nodeDisconnected*
  typedef std::function<void(const NodeID&)> NodeFailureHandler;
  void AddNodeFailureHandler(NodeFailureHandler handler) {
    node_failure_handlers_.push_back(handler);
  }

  // manage customer
  Customer* customer(int id);
  void AddCustomer(Customer* obj);
  void TransferCustomer(Customer* obj);
  void RemoveCustomer(int id);
  int NextCustomerID();

  int num_workers() { return num_workers_; }
  int num_servers() { return num_servers_; }

  // manage message. no fault tolerance yet...
  void AddRequest(Message* msg) { delete msg; }
  void AddResponse(Message* msg) { }

  // accessors
  Van& van() { return van_; }
  App* app() { return app_; }
  NetworkUsage& net_usage() { return net_usage_; }

 private:
  bool IsScheduler() { return van_.my_node().role() == Node::SCHEDULER; }
  Task NewControlTask(Control::Command cmd);
  void SendTask(const NodeID& recver, const Task& task);
  void SendTask(const Node& recver, const Task& task) {
    SendTask(recver.id(), task);
  }

  bool Timeout(int sec, const std::function<bool()>& pred);

  void ForceExit() {
    string kill = "kill -9 " + std::to_string(getpid());
    int ret = system(kill.c_str());
    if (ret != 0) LOG(INFO) << "failed to " << kill;
  }

  // the app
  App* app_ = nullptr;

  // nodes
  std::map<NodeID, Node> nodes_;
  std::mutex nodes_mu_;
  // number of nodes this node connected to
  int num_workers_ = 0;
  int num_servers_ = 0;

  // the following two are only available for the scheduler
  std::unordered_set<NodeID> active_nodes_;
  std::unordered_set<NodeID> alive_nodes_;
  size_t unknown_dead_nodes_ = 0;

  std::vector<NodeFailureHandler> node_failure_handlers_;
  bool is_my_node_inited_ = false;

  bool inited_ = false;
  // only available at the scheduler node
  NodeAssigner* node_assigner_ = nullptr;

  // customers
  // format: <id, <obj_ptr, is_deletable>>
  std::map<int, std::pair<Customer*, bool>> customers_;

  bool done_ = false;
  bool in_exit_ = false;
  int time_ = 0;

  Van van_;
  Env env_;

  NetworkUsage net_usage_;

  DISALLOW_COPY_AND_ASSIGN(Manager);
};

} // namespace ps
