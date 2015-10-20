#pragma once
#include <mutex>
#include "ps/range.h"
#include "ps/internal/customer.h"
#include "ps/internal/van.h"
namespace ps {

/**
 * \brief the center of the system
 */
class Postoffice {
 public:
  /**
   * \brief return the singleton object
   */
  static Postoffice* Get() {
    static Postoffice e; return &e;
  }

  /** \brief get the van */
  Van* van() { return van_; }


  /**
   * \brief start the system
   *
   * This function will block until every nodes are started.
   * \param argv0 the program name, used for logging
   */
  void Start(const char* argv0);

  /**
   * \brief terminate the system
   *
   * All nodes should call this function before existing. It will block until
   * every node is finalized.
   */
  void Finalize();

  /**
   * \brief add an customer to the system. threadsafe
   */
  void AddCustomer(Customer* customer);

  /**
   * \brief remove a customer by given it's id. threasafe
   */
  void RemoveCustomer(Customer* customer);

  /**
   * \brief get the customer by id, threadsafe
   * \param id the customer id
   * \param timeout timeout in sec
   * \return return nullptr if doesn't exist and timeout
   */
  Customer* GetCustomer(int id, int timeout = 0) const;

  /**
   * \brief get the id of a node (group), threadsafe
   *
   * if it is a  node group, return the list of node ids in this
   * group. otherwise, return {node_id}
   */
  const std::vector<int>& GetNodeIDs(int node_id) const {
    const auto it = node_ids_.find(node_id);
    CHECK(it != node_ids_.cend()) << "node " << node_id << " doesn't exist";
    return it->second;
  }

  /**
   * \brief return the key ranges of all server nodes
   */
  const std::vector<Range>& GetServerKeyRanges();

  /**
   * \brief the template of a callback
   */
  using Callback = std::function<void()>;

  /**
   * \brief Register a callback to the system which is called after Finalize()
   *
   * The following codes are equal
   * \code {cpp}
   * RegisterExitCallback(cb);
   * Finalize();
   * \endcode
   *
   * \code {cpp}
   * Finalize();
   * cb();
   * \endcode
   * \param cb the callback function
   */
  void RegisterExitCallback(const Callback& cb) {
    exit_callback_ = cb;
  }

  /**
   * \brief convert from a worker rank into a node id
   * \param rank the worker rank
   */
  static inline int WorkerRankToID(int rank) {
    return rank * 2 + 9;
  }

  /**
   * \brief convert from a server rank into a node id
   * \param rank the server rank
   */
  static inline int ServerRankToID(int rank) {
    return rank * 2 + 8;
  }

  /**
   * \brief convert from a node id into a server or worker rank
   * \param id the node id
   */
  static inline int IDtoRank(int id) {
    return std::max((id - 8) / 2, 0);
  }

  /** \brief Returns the number of worker nodes */
  int num_workers() const { return num_workers_; }

  /** \brief Returns the number of server nodes */
  int num_servers() const { return num_servers_; }

  /** \brief Returns the rank of this node in its group
   *
   * Each worker will have a unique rank within [0, NumWorkers()). So are
   * servers. This function is available only after \ref Start has been called.
   */
  int my_rank() const { return IDtoRank(van_->my_node().id()); }

  /** \brief Returns true if this node is a worker node */
  int is_worker() const { return is_worker_; }

  /** \brief Returns true if this node is a server node. */
  int is_server() const { return is_server_; }

  /** \brief Returns true if this node is a scheduler node. */
  int is_scheduler() const { return is_scheduler_; }

  /**
   * \brief barrier
   * \param node_id the barrier group id
   */
  void Barrier(int node_id);

  /**
   * \brief process a control message, called by van
   * \param the received message
   */
  void Manage(const Message& recv);

 private:
  Postoffice();
  ~Postoffice() { delete van_; }
  Van* van_;
  mutable std::mutex mu_;
  DISALLOW_COPY_AND_ASSIGN(Postoffice);

  std::unordered_map<int, Customer*> customers_;
  std::unordered_map<int, std::vector<int>> node_ids_;
  std::vector<Range> server_key_ranges_;

  bool is_worker_, is_server_, is_scheduler_;
  int num_servers_, num_workers_;

  bool barrier_done_;
  std::mutex barrier_mu_;
  std::condition_variable barrier_cond_;

  Callback exit_callback_;
};
}  // namespace ps
