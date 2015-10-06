#pragma once
#include "ps/app.h"
namespace ps {

DECLARE_int32(num_workers);
DECLARE_int32(num_servers);

/// \brief Queries runtime info about the node
class NodeInfo {
 public:

  /// \brief Returns the rank of this node in its group, which is in {0, ...,
  /// \ref RankSize - 1} .
  ///
  /// Each node has an unique rank in its group (e.g. worker group or server
  /// group), which is a continuous integer starting from 0.
  static inline int MyRank() { return MyNode().rank(); }

  /// \brief Returns the group size
  static inline int RankSize() {
    return IsWorker() ? NumWorkers() : (IsServer() ? NumServers() : 1);
  }

  /// \brief Returns the number of worker nodes
  static inline int NumWorkers() { return FLAGS_num_workers; }

  /// \brief Returns the number of server nodes
  static inline int NumServers() { return FLAGS_num_servers; }

  /// \brief Returns true if this node is a worker node
  static bool IsWorker() { return MyNode().role() == Node::WORKER; }

  /// \brief Returns true if this node is a server node.
  static inline int IsServer() { return MyNode().role() == Node::SERVER; }

  /// \brief Returns true if this node is a scheduler node.
  static inline int IsScheduler() { return MyNode().role() == Node::SCHEDULER; }

  /// \brief Returns the key range this node maintains
  static inline Range<Key> KeyRange() { return Range<Key>(MyNode().key()); }

  /// \brief Returns my node id
  static inline std::string MyID() {
    return MyNode().id();
  }

  /// \brief Returns my node info
  static inline Node MyNode() {
    return Postoffice::instance().manager().van().my_node();
  }

  /// \brief Returns the scheduler ID
  static inline std::string SchedulerID() {
    return Postoffice::instance().manager().van().scheduler().id();
  }

  /// \brief The app this node runs
  static inline App* MyApp() {
    return Postoffice::instance().manager().app();
  }
};

}  // namespace ps
