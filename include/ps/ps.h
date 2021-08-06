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
 * The rank is group-level instead of instance-level.
 */
inline int MyRank() {
  return Postoffice::Get()->my_rank() / Postoffice::Get()->group_size();
}
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
 * \brief start the system for one worker/server/scheduler instance
 *
 * \param instance_idx the offset of the instance in the instance group
 */
inline void _StartPS(int customer_id, Node::Role role, int rank,
                     bool do_barrier, const char *argv0, int instance_idx) {
  if (role == Node::WORKER) {
    Postoffice::GetWorker(instance_idx)
        ->Start(customer_id, role, rank, do_barrier, argv0);
  } else if (role == Node::SERVER || role == Node::SCHEDULER) {
    Postoffice::GetServer(instance_idx)
        ->Start(customer_id, role, rank, do_barrier, argv0);
  } else {
    // Joint PS: one worker, one server
    std::thread thread_s(_StartPS, customer_id, Node::SERVER, rank, do_barrier,
                         argv0, instance_idx);
    PS_VLOG(1) << "Postoffice server started.";

    std::thread thread_w(_StartPS, customer_id, Node::WORKER, rank, do_barrier,
                         argv0, instance_idx);
    PS_VLOG(1) << "Postoffice worker started.";

    thread_s.join();
    thread_w.join();
  }
}

/**
 * \brief start the system for a group of worker/server/scheduler instances,
 * based on instance-level ranks
 *
 * \param worker_ranks the **instance-level** ranks of the worker instances to
 * start \param server_ranks the **instance-level** ranks of the server
 * instances to start
 */
inline void _StartPSGroup(int customer_id, std::vector<int> worker_ranks,
                          std::vector<int> server_ranks, bool do_barrier,
                          const char *argv0 = nullptr) {
  std::vector<std::thread> threads;
  for (size_t i = 0; i < worker_ranks.size(); ++i) {
    threads.emplace_back(_StartPS, customer_id, Node::WORKER, worker_ranks[i],
                         do_barrier, argv0, i);
    PS_VLOG(1) << "Postoffice worker rank " << worker_ranks[i] << " started.";
  }
  for (size_t i = 0; i < server_ranks.size(); ++i) {
    threads.emplace_back(_StartPS, customer_id, Node::SERVER, server_ranks[i],
                         do_barrier, argv0, i);
    PS_VLOG(1) << "Postoffice server rank " << server_ranks[i] << " started.";
  }
  for (auto &t : threads) {
    t.join();
  }
}

/**
 * \brief start the system. This can be called only ONCE
 *
 * \param customer_id the customer id
 * \param role the node group / role: worker, server, scheduler, joint. joint
 role means both having worker and server
 * \param rank the rank. -1 means no preference and the rank will be assigned by
 the scheduler.
 * \param do_barrier do a barrier to make sure every rank calls StartPS
 * \param argv0 the program name, used for logging
 */
inline void StartPS(int customer_id, Node::Role role, int rank, bool do_barrier,
                    const char *argv0 = nullptr) {
  auto val = Environment::Get()->find("DMLC_GROUP_SIZE");
  int group_size = val ? atoi(val) : 1;

  Postoffice::Init(role);
  if (group_size == 1 || role == Node::SCHEDULER) {
    int instance_idx = 0;
    _StartPS(customer_id, role, rank, do_barrier, argv0, instance_idx);
  } else {
    CHECK(rank >= 0 && group_size > 0) << group_size;
    std::vector<int> worker_ranks;
    std::vector<int> server_ranks;
    // start PS workers and servers as a group
    if (role == Node::WORKER || role == Node::JOINT) {
      for (int i = 0; i < group_size; ++i) {
        int rank_i = rank * group_size + i;
        worker_ranks.push_back(rank_i);
      }
    }
    if (role == Node::SERVER || role == Node::JOINT) {
      for (int i = 0; i < group_size; ++i) {
        int rank_i = rank * group_size + i;
        server_ranks.push_back(rank_i);
      }
    }
    _StartPSGroup(customer_id, worker_ranks, server_ranks, do_barrier, argv0);
  }
}

inline void _Finalize(int customer_id, Node::Role role,
                      const bool do_barrier = true, int index = 0) {
  if (role == Node::WORKER) {
    Postoffice::GetWorker(index)->Finalize(customer_id, do_barrier);
  } else if (role == Node::SERVER || role == Node::SCHEDULER) {
    Postoffice::GetServer(index)->Finalize(customer_id, do_barrier);
  } else {
    // Joint PS: one worker, one server
    std::thread thread_s(&Postoffice::Finalize, Postoffice::GetServer(index),
                         customer_id, do_barrier);
    PS_VLOG(1) << "Finalize Postoffice server.";

    std::thread thread_w(&Postoffice::Finalize, Postoffice::GetWorker(index),
                         customer_id, do_barrier);
    PS_VLOG(1) << "Finalize Postoffice worker.";

    thread_s.join();
    thread_w.join();
  }
}

inline void _FinalizeGroup(int customer_id, Node::Role role, int group_size,
                           bool do_barrier) {
  std::vector<std::thread> threads;
  if (role == Node::JOINT || role == Node::WORKER) {
    for (int i = 0; i < group_size; ++i) {
      threads.emplace_back(&Postoffice::Finalize, Postoffice::GetWorker(i),
                           customer_id, do_barrier);
      PS_VLOG(1) << "Finalize worker instance " << i;
    }
  }
  if (role == Node::JOINT || role == Node::SERVER) {
    for (int i = 0; i < group_size; ++i) {
      threads.emplace_back(&Postoffice::Finalize, Postoffice::GetServer(i),
                           customer_id, do_barrier);
      PS_VLOG(1) << "Finalize server instance " << i;
    }
  }
  for (auto &t : threads) {
    t.join();
  }
}

/**
 * \brief terminate the system
 *
 * All nodes should call this function before existing.
 * \param do_barrier whether to block until every node is finalized, default
 * true.
 */
inline void Finalize(int customer_id, Node::Role role,
                     const bool do_barrier = true) {
  auto val = Environment::Get()->find("DMLC_GROUP_SIZE");
  int group_size = val ? atoi(val) : 1;
  if (group_size == 1 || role == Node::SCHEDULER) {
    int instance_idx = 0;
    _Finalize(customer_id, role, do_barrier, instance_idx);
  } else {
    _FinalizeGroup(customer_id, role, group_size, do_barrier);
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
