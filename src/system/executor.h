#pragma once
#include "system/remote_node.h"
#include "system/message.h"
namespace ps {

const static NodeID kGroupPrefix  = "all_";
// all server nodes
const static NodeID kServerGroup  = kGroupPrefix + "servers";
// all worker nodes
const static NodeID kWorkerGroup  = kGroupPrefix + "workers";
// kServerGroup + kWorkerGroup
const static NodeID kCompGroup    = kGroupPrefix + "comp_nodes";
// the nodes maintaining a replica of the key segment I own
const static NodeID kReplicaGroup = kGroupPrefix + "replicas";
// the owner nodes of the key segments this node backup
const static NodeID kOwnerGroup   = kGroupPrefix + "owners";
// all live nodes, including scheduler, workers, servers, unused nodes...
const static NodeID kLiveGroup    = kGroupPrefix + "lives";

// Executor maintain all remote nodes for a customer. It has its own thread to process
// received tasks.
class Executor {
 public:
  Executor(Customer& obj);
  ~Executor();

  // -- communication and synchronization --
  // see comments in customer.h
  int Submit(Message* request);
  void Reply(Message* request, Message* response);

  void Accept(Message* msg);
  void WaitSentReq(int timestamp);
  void WaitRecvReq(int timestamp, const NodeID& sender);
  void FinishRecvReq(int timestamp, const NodeID& sender);
  int QueryRecvReq(int timestamp, const NodeID& sender);

  // the last received request
  inline std::shared_ptr<Message> last_request() { return last_request_; }
  // the last received response
  inline std::shared_ptr<Message> last_response() { return last_response_; }

  int IncrClock(int delta) { Lock l(node_mu_); time_ += delta; return time_; }
  int time() { Lock l(node_mu_); return time_; }
  // node management
  void AddNode(const Node& node);
  void RemoveNode(const Node& node);
  void ReplaceNode(const Node& old_node, const Node& new_node);
 private:
  // Runs the DAG engine
  void Run() {
    while (!done_) {
      if (PickActiveMsg()) ProcessActiveMsg();
    }
  }
  // Returns true if a message with dependency satisfied is picked. Otherwise
  // will be blocked.
  bool PickActiveMsg();
  void ProcessActiveMsg();

  // -- received messages --
  std::list<Message*> recv_msgs_;
  std::mutex msg_mu_;
  // the message is going to be processed or the last one be processed
  std::shared_ptr<Message> active_msg_, last_request_, last_response_;
  std::condition_variable dag_cond_;

  // -- remote nodes --
  std::mutex node_mu_;
  std::condition_variable recv_req_cond_;
  std::condition_variable sent_req_cond_;
  std::unordered_map<NodeID, RemoteNode> nodes_;

  inline RemoteNode* GetRNode(const NodeID& node_id) {
    auto it = nodes_.find(node_id);
    CHECK(it != nodes_.end()) << "node [" << node_id << "] doesn't exist";
    return &(it->second);
  }

  inline bool CheckFinished(RemoteNode* rnode, int timestamp, bool sent);
  inline int NumFinished(RemoteNode* rnode, int timestamp, bool sent);

  std::vector<NodeID> GroupIDs() {
   std::vector<NodeID> ids = {
     kServerGroup, kWorkerGroup, kCompGroup, kReplicaGroup, kOwnerGroup, kLiveGroup};
    return ids;
  }

  Customer& obj_;
  Postoffice& sys_;
  Node my_node_;
  int num_replicas_ = 0;  // number of replicas for a server node

  int time_ = 0;  // current timestamp
  struct ReqInfo {
    NodeID recver;
    Message::Callback callback;
  };
  // <timestamp, (receiver, callback)>
  std::unordered_map<int, ReqInfo> sent_reqs_;

  // the processing thread
  bool done_ = false;
  std::thread* thread_ = nullptr;
};

} // namespace ps

  // bool IsGroupID(const NodeID& node_id) {
  //   return node_id.compare(
  //       0, std::max(node_id.size(), kGroupPrefix.size()), kGroupPrefix) == 0;
  // }
