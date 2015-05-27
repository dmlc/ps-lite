#pragma once
#include "base/common.h"
#include "base/range.h"
#include "proto/node.pb.h"
#include "proto/data.pb.h"
namespace ps {

// assign *node* with proper rank_id, key_range, etc..
class NodeAssigner {
 public:
  NodeAssigner(int num_servers, Range<Key> key_range) {
    num_servers_ = num_servers;
    key_range_ = key_range;
  }
  ~NodeAssigner() { }

  void Assign(Node* node) {
    Range<Key> kr = key_range_;
    int rank = 0;
    if (node->role() == Node::SERVER) {
      kr = key_range_.EvenDivide(num_servers_, server_rank_);
      rank = server_rank_ ++;
    } else if (node->role() == Node::WORKER) {
      rank = worker_rank_ ++;
    }
    node->set_rank(rank);
    kr.To(node->mutable_key());
  }

  void Remove(const Node& node) {
    // TODO...
  }
 protected:
  int num_servers_ = 0;
  int server_rank_ = 0;
  int worker_rank_ = 0;
  Range<Key> key_range_;
};

} // namespace ps
