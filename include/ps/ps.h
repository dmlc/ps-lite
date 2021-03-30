/*!
 *  Copyright (c) 2015 by Contributors
 * @file   ps.h
 * \brief  The parameter server interface
 */
#ifndef PS_PS_H_
#define PS_PS_H_
/** \brief basic setups in ps */
#include "ps/base.h"
/** \brief communicating with a pair of (int, string). */
#include "ps/simple_app.h"
/** \brief communcating with a list of key-value paris. */
#include "ps/kv_app.h"
namespace ps {
/** \brief Returns the number of worker nodes */
inline int NumWorkers() { return Postoffice::Get()->num_workers(); }
/** \brief Returns the number of server nodes */
inline int NumServers() { return Postoffice::Get()->num_servers(); }

/** \brief Returns true if this node is a server node. */
inline bool IsServer() { return Postoffice::Get()->is_server(); }
/** \brief Returns true if this node is a scheduler node. */
inline bool IsScheduler() { return Postoffice::Get()->is_scheduler(); }
/** \brief Returns the rank of this node in its group
 *
 * Each worker will have a unique rank within [0, NumWorkers()). So are
 * servers. This function is available only after \ref Start has been called.
 */
inline int MyRank() { return Postoffice::Get()->my_rank(); }
/**
 * \brief start the system
 *
 * This function will block until every nodes are started.
 * \param argv0 the program name, used for logging
 */

inline Node::Role GetRole(const std::string role_str) {
  Node::Role role = Node::SCHEDULER;
  if (role_str == "worker") {
    role = Node::WORKER;
  } else if (role_str == "server") {
    role = Node::SERVER;
  } else if (role_str == "scheduler") {
    role = Node::SCHEDULER;
  } else if (role_str == "joint") {
    role = Node::JOINT;
  } else {
    CHECK(false) << "Unexpected role: " << role_str;
  }
  return role;
}
/**
 * \brief start the system
 *
 * This function will NOT block.
 * \param customer_id the customer id
 * \param preferred_rank the preferred rank. -1 means no preference and the rank will be assigned by
          the scheduler. If the rank is non-negative, the preferred rank will be assigned accordingly.
 * \param argv0 the program name, used for logging
 */
inline void StartPS(int customer_id, Node::Role role, int rank, bool do_barrier, const char *argv0 = nullptr) {
  if (role == Node::WORKER) {
    Postoffice::GetWorker()->Start(customer_id, role, rank, do_barrier, argv0);
  } else if (role == Node::SERVER || role == Node::SCHEDULER) {
    Postoffice::GetServer()->Start(customer_id, role, rank, do_barrier, argv0);
  } else {
    // Joint PS: one worker, one server
    std::thread thread_s(StartPS, customer_id, Node::SERVER, rank, do_barrier, argv0);
    LOG(INFO) << "Postoffice server started.";

    std::thread thread_w(StartPS, customer_id, Node::WORKER, rank, do_barrier, argv0);
    LOG(INFO) << "Postoffice worker started.";

    thread_s.join();
    thread_w.join();
  }
}

/**
 * \brief terminate the system
 *
 * All nodes should call this function before existing.
 * \param do_barrier whether to block until every node is finalized, default true.
 */
inline void Finalize(int customer_id, Node::Role role, const bool do_barrier = true) {
  if (role == Node::WORKER) {
    Postoffice::GetWorker()->Finalize(customer_id, do_barrier);
  } else if (role == Node::SERVER || role == Node::SCHEDULER) {
    Postoffice::GetServer()->Finalize(customer_id, do_barrier);
  } else {
    // Joint PS: one worker, one server
    std::thread thread_s(&Postoffice::Finalize, Postoffice::GetServer(), customer_id, do_barrier);
    LOG(INFO) << "Finalize Postoffice server.";

    std::thread thread_w(&Postoffice::Finalize, Postoffice::GetWorker(), customer_id, do_barrier);
    LOG(INFO) << "Finalize Postoffice worker.";

    thread_s.join();
    thread_w.join();
  }
}
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
inline void RegisterExitCallback(const std::function<void()> &cb) {
  Postoffice::Get()->RegisterExitCallback(cb);
}

}  // namespace ps
#endif  // PS_PS_H_
