#pragma once
#include "base/common.h"
#include "proto/task.pb.h"
#include "system/van.h"
#include "system/postoffice.h"
#include "filter/filter.h"
namespace ps {

// The presentation of a remote node used by Executor. It's not thread
// safe, do not use them directly.

// Track a request by its timestamp.
class RequestTracker {
 public:
  RequestTracker() { }
  ~RequestTracker() { }

  // Returns true if timestamp "ts" is marked as finished.
  bool IsFinished(int ts) {
    return ts < 0 || (((int)data_.size() > ts) && data_[ts]);
  }

  // Mark timestamp "ts" as finished.
  void Finish(int ts) {
    CHECK_GE(ts, 0);
    CHECK_LT(ts, 100000000);
    if ((int)data_.size() <= ts) data_.resize(ts*2+5);
    data_[ts] = true;
  }
 private:
  std::vector<bool> data_;
};

// A remote node
struct RemoteNode {
 public:
  RemoteNode() { }
  ~RemoteNode() {
    for (auto f : filters) delete f.second;
  }

  void EncodeMessage(Message* msg);
  void DecodeMessage(Message* msg);

  Node node;         // the remote node
  bool alive = true; // aliveness

  // timestamp tracker
  RequestTracker sent_req_tracker;
  RequestTracker recv_req_tracker;

  // node group info. if "node" is a node group, then "group" contains all node
  // pointer in this group. otherwise, group contains "this"
  void AddGroupNode(RemoteNode* rnode);
  void RemoveGroupNode(RemoteNode* rnode);
  std::vector<RemoteNode*> group;

  // keys[i] is the key range of group[i]
  std::vector<Range<Key>> keys;

 private:

  IFilter* FindFilterOrCreate(const Filter& conf);
  // key: filter_type
  std::unordered_map<int, IFilter*> filters;

};


} // namespace ps
