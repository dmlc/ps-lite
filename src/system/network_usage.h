#pragma once
#include "proto/node.pb.h"
#include <unordered_set>
#include "base/resource_usage.h"
namespace ps {

/**
 * @brief Monitor network usage
 */
class NetworkUsage {
 public:
  NetworkUsage() { start_ = tic(); }
  ~NetworkUsage() {
    Display();
  }

  void AddMyNode(const Node& node) {
    my_id_ = node.id();
    my_host_ = node.hostname();
  }
  void AddNode(const Node& node) {
    auto host = node.hostname();
    CHECK(!host.empty());
    CHECK(!my_host_.empty());
    if (host == my_host_) {
      // LL << host << " " << my_host_;
      local_machine_.insert(node.id());
    }
  }

  inline void IncrSend(const NodeID& recver, size_t bytes) {
    if (local_machine_.find(recver) != local_machine_.end()) {
      sent_to_local_ += bytes;
    }
    sent_ += bytes;
  }
  inline void IncrRecv(const NodeID& sender, size_t bytes) {
    if (local_machine_.find(sender) != local_machine_.end()) {
      received_from_local_ += bytes;
    }
    received_ += bytes;
  }

 private:
  void Display() {
    double time = toc(start_);
    auto gb = [](size_t x) { return  x / 1e9; };
    LOG(INFO) << my_id_ << ": sent " << gb(sent_)
              << " (local " << gb(sent_to_local_) << ") Gbyte, avg. speed "
              << gb(sent_) / time * 1000 << " Mbyte/sec";

    LOG(INFO) << my_id_ << ": received " << gb(received_)
              << " (local " << gb(received_from_local_) << ") Gbyte, avg. speed "
              << gb(received_) / time * 1000 << " Mbyte/sec";
  }
  NodeID my_id_;
  std::string my_host_;
  std::unordered_set<NodeID> local_machine_;
  system_clock::time_point start_;
  size_t sent_to_local_ = 0;
  size_t sent_ = 0;
  size_t received_from_local_ = 0;
  size_t received_ = 0;

};

}  // namespace ps
