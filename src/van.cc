/**
 *  Copyright (c) 2015 by Contributors
 *  Modifications Copyright (C) Mellanox Technologies Ltd. 2020.
 */
// clang-format off
#include "ps/internal/van.h"
#include "ps/internal/postoffice.h"

#include <string.h>

#include <chrono>
#include <fstream>
#include <set>
#include <sstream>
#include <thread>

#include "./fabric_van.h"
#include "./meta.h"
#include "./multi_van.h"
#include "./network_utils.h"
#include "./rdma_van.h"
#include "./resender.h"
#include "./ucx_van.h"
#include "./zmq_van.h"
#include "ps/base.h"
#include "ps/internal/customer.h"
#include "ps/sarray.h"
// clang-format on

#define USE_PROFILING

namespace ps {

// interval in second between to heartbeast signals. 0 means no heartbeat.
// don't send heartbeast in default. because if the scheduler received a
// heartbeart signal from a node before connected to that node, then it could be
// problem.
static const int kDefaultHeartbeatInterval = 0;
#ifdef USE_PROFILING
std::fstream fout_;
#endif

bool is_van_profiling_;
Van::ErrHandle Van::err_handle_ = nullptr;

Van *Van::Create(const std::string &type, Postoffice *postoffice) {
#ifdef USE_PROFILING
  const char *val;
  val = Environment::Get()->find("ENABLE_PROFILING");
  is_van_profiling_ = val ? atoi(val) : false;
  if (is_van_profiling_) {
    LOG(INFO) << "Van: Enable profiling.";
    std::string sysvar = "";
    if (getenv("PROFILE_PATH") != nullptr)
      sysvar = std::string(getenv("PROFILE_PATH"));
    std::string CONST_WORKER_STR("worker");
    std::string CONST_SERVER_STR("server");

    std::chrono::microseconds ms =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    std::stringstream temp_stream;
    std::string ts_string;
    temp_stream << ms.count();
    temp_stream >> ts_string;

    if (CONST_WORKER_STR == postoffice->role_str()) {
      if (sysvar.length() == 0) {
        fout_.open("pslite_profile_van_worker_" + ts_string, std::fstream::out);
      } else {
        fout_.open(sysvar + "_van_worker", std::fstream::out);
      }
    } else if (CONST_SERVER_STR == postoffice->role_str()) {
      if (sysvar.length() == 0) {
        fout_.open("pslite_profile_van_server_" + ts_string, std::fstream::out);
      } else {
        fout_.open(sysvar + "_van_server", std::fstream::out);
      }
    }
  }
#endif
  if (type == "multivan") {
    return new MultiVan(postoffice);
  } else if (type == "zmq" || type == "0") {
    return new ZMQVan(postoffice);
#ifdef DMLC_USE_RDMA
  } else if (type == "ibverbs") {
    return new RDMAVan(postoffice);
  } else if (type == "1") {
    LOG(INFO) << "Creating RDMAVan.";
    LOG(WARNING) << "DMLC_ENABLE_RDMA=1 will be deprecated. "
                 << "Please use DMLC_ENABLE_RDMA=ibverbs instead.";
    return new RDMAVan(postoffice);
#endif
#ifdef DMLC_USE_FABRIC
  } else if (type == "fabric") {
    return new FabricVan(postoffice);
#endif
#ifdef DMLC_USE_UCX
  } else if (type == "ucx") {
    return new UCXVan(postoffice);
#endif
  } else {
    LOG(FATAL) << "unsupported van type: " << type;
    return nullptr;
  }
}

void Van::ProcessTerminateCommand() {
  PS_VLOG(1) << my_node().ShortDebugString() << " is stopped";
  ready_ = false;
}

void Van::ProcessAddNodeCommandAtScheduler(Message *msg, Meta *nodes,
                                           Meta *recovery_nodes) {
  recovery_nodes->control.cmd = Control::ADD_NODE;
  time_t t = time(NULL);
  // consider all worker and server instances
  size_t num_nodes =
      postoffice_->num_server_instances() + postoffice_->num_worker_instances();
  if (nodes->control.node.size() == num_nodes) {
    bool mixed_mode = getenv("BYTEPS_ENABLE_MIXED_MODE")
                          ? atoi(getenv("BYTEPS_ENABLE_MIXED_MODE"))
                          : false;
    bool sparse_mode = getenv("BYTEPS_ORDERED_HOSTS") ? true : false;
    CHECK_NE(mixed_mode && sparse_mode, true)
        << "BYTEPS_ENABLE_MIXED_MODE and BYTEPS_ORDERED_HOSTS should not "
           "coexist";

    if (mixed_mode) {
      std::unordered_map<std::string, size_t> ip_cnt;
      for (auto &node : nodes->control.node) {
        ip_cnt[node.hostname] += 1;
        CHECK_LE(ip_cnt[node.hostname], 2) << node.hostname;
      }

      // put non-colocate servers' IP to front
      std::sort(
          nodes->control.node.begin(), nodes->control.node.end(),
          [&ip_cnt](const Node &a, const Node &b) {
            if (ip_cnt[a.hostname] == ip_cnt[b.hostname]) {
              return (a.hostname.compare(b.hostname) | (a.port < b.port)) > 0;
            }
            return ip_cnt[a.hostname] < ip_cnt[b.hostname];
          });

      for (auto &node : nodes->control.node) {
        std::string node_host_ip =
            node.hostname + ":" + std::to_string(node.port);
        if (ip_cnt[node.hostname] == 1) {
          PS_VLOG(1) << "Non-Colocate Server: " << node_host_ip;
          CHECK_EQ(node.role, Node::SERVER);
        } else {
          PS_VLOG(1) << "Colocated "
                     << ((node.role == Node::SERVER) ? "Server" : "Worker")
                     << ": \t" << node_host_ip;
        }
      }
    } else if (sparse_mode) {
      PS_VLOG(1) << "Assign the rank as the same order in BYTEPS_ORDERED_HOSTS";
      CHECK(getenv("BYTEPS_ORDERED_HOSTS"))
          << "\n should set it as a list of IP:port (port is optional) "
             "concatenated by comma "
          << "\n an example: BYTEPS_ORDERED_HOSTS=10.0.0.1:1234,10.0.0.2:4321";
      std::string sparse_hosts = std::string(getenv("BYTEPS_ORDERED_HOSTS"));
      std::vector<std::string> hosts_list;
      size_t pos = 0;
      while ((pos = sparse_hosts.find(",")) != std::string::npos) {
        std::string host = sparse_hosts.substr(0, pos);
        hosts_list.push_back(host);
        sparse_hosts.erase(0, pos + 1);
      }
      hosts_list.push_back(sparse_hosts);

      std::unordered_map<std::string, size_t> ip_pos;
      for (size_t i = 0; i < hosts_list.size(); ++i) {
        std::string ip = hosts_list[i].substr(0, hosts_list[i].find(":"));
        CHECK_EQ(ip_pos.find(ip), ip_pos.end())
            << "\nDuplicate IP found in BYTEPS_ORDERED_HOSTS: " << ip;
        ip_pos[ip] = i;
      }

      // sort the ip in the same order as BYTEPS_ORDERED_HOSTS
      std::sort(nodes->control.node.begin(), nodes->control.node.end(),
                [&ip_pos](const Node &a, const Node &b) {
                  return ip_pos[a.hostname] < ip_pos[b.hostname];
                });
    } else {
      // sort the nodes according their ip and port
      // no need to sort for p2p communication case, but sort might be good for
      // later usage
      std::sort(nodes->control.node.begin(), nodes->control.node.end(),
                [](const Node &a, const Node &b) {
                  return (a.hostname.compare(b.hostname) | (a.port < b.port)) >
                         0;
                });
    }

    bool with_preferred_rank = false;
    for (auto &node : nodes->control.node) {
      if (node.aux_id != -1) {
        LOG(INFO) << "rank detected for node " << node.DebugString();
        with_preferred_rank = true;
      }
    }

    // make sure ranks starts from 0 to num_servers and num_worker.
    if (with_preferred_rank) {
      std::unordered_set<int> server_ranks;
      std::unordered_set<int> worker_ranks;
      for (auto &node : nodes->control.node) {
        if (node.role == Node::SERVER) {
          CHECK_EQ(server_ranks.find(node.aux_id), server_ranks.end())
              << "rank must be unique: " << node.DebugString();
          server_ranks.insert(node.aux_id);
        } else if (node.role == Node::WORKER) {
          CHECK_EQ(worker_ranks.find(node.aux_id), worker_ranks.end())
              << "rank must be unique: " << node.DebugString();
          worker_ranks.insert(node.aux_id);
        } else {
          LOG(FATAL) << "unrecognized node role " << node.DebugString();
        }
      }

      // check continousity
      int num_servers = Postoffice::Get()->num_server_instances();
      for (int i = 0; i < num_servers; ++i) {
        CHECK(server_ranks.find(i) != server_ranks.end()) << i;
      }
      CHECK(server_ranks.size() == (size_t)num_servers);

      int num_workers = Postoffice::Get()->num_worker_instances();
      for (int i = 0; i < num_workers; ++i) {
        CHECK(worker_ranks.find(i) != worker_ranks.end()) << i;
      }
      CHECK(worker_ranks.size() == (size_t)num_workers);
    }

    // assign node rank
    for (auto &node : nodes->control.node) {
      std::string node_host_ip =
          node.hostname + ":" + std::to_string(node.port);
      int id = -1;
      if (node.role == Node::SERVER) {
        id = Postoffice::ServerRankToID(with_preferred_rank ? node.aux_id
                                                            : num_servers_);
      } else {
        id = Postoffice::WorkerRankToID(with_preferred_rank ? node.aux_id
                                                            : num_workers_);
      }
      if (connected_nodes_.find(node_host_ip) == connected_nodes_.end()) {
        CHECK_EQ(node.id, Node::kEmpty);
        PS_VLOG(1) << "assign id=" << id << " to node " << node.DebugString();
        node.id = id;
        Connect(node);
        postoffice_->UpdateHeartbeat(node.id, t);
        connected_nodes_[node_host_ip] = id;
      } else {
        shared_node_mapping_[id] = connected_nodes_[node_host_ip];
        node.id = connected_nodes_[node_host_ip];
      }
      if (node.role == Node::SERVER) num_servers_++;
      if (node.role == Node::WORKER) num_workers_++;
    }
    nodes->control.node.push_back(my_node_);
    nodes->control.cmd = Control::ADD_NODE;
    Message back;
    back.meta = *nodes;
    for (int r : postoffice_->GetNodeIDs(kWorkerGroup + kServerGroup)) {
      int recver_id = r;
      if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
        back.meta.recver = recver_id;
        back.meta.timestamp = timestamp_++;
        Send(back);
      }
    }
    PS_VLOG(1) << "The scheduler is connected to " << num_workers_
               << " workers and " << num_servers_ << " servers";
    ready_ = true;
  } else if (!recovery_nodes->control.node.empty()) {
    auto dead_nodes = postoffice_->GetDeadNodes(heartbeat_timeout_);
    std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
    // send back the recovery node
    CHECK_EQ(recovery_nodes->control.node.size(), 1);
    Connect(recovery_nodes->control.node[0]);
    postoffice_->UpdateHeartbeat(recovery_nodes->control.node[0].id, t);
    Message back;
    for (int r : postoffice_->GetNodeIDs(kWorkerGroup + kServerGroup)) {
      if (r != recovery_nodes->control.node[0].id &&
          dead_set.find(r) != dead_set.end()) {
        // do not try to send anything to dead node
        continue;
      }
      // only send recovery_node to nodes already exist
      // but send all nodes to the recovery_node
      back.meta =
          (r == recovery_nodes->control.node[0].id) ? *nodes : *recovery_nodes;
      back.meta.recver = r;
      back.meta.timestamp = timestamp_++;
      Send(back);
    }
  } else {
    PS_VLOG(1) << "AddNode (" << nodes->control.node.size() << "/" << num_nodes
               << "): " << nodes->control.node.back().DebugString();
  }
}

void Van::UpdateLocalID(Message *msg, std::unordered_set<int> *deadnodes_set,
                        Meta *nodes, Meta *recovery_nodes) {
  auto &ctrl = msg->meta.control;
  size_t num_nodes =
      postoffice_->num_server_instances() + postoffice_->num_worker_instances();
  // assign an id
  if (msg->meta.sender == Meta::kEmpty) {
    CHECK(is_scheduler_);
    CHECK_EQ(ctrl.node.size(), 1);
    if (static_cast<int>(nodes->control.node.size()) < (int)num_nodes) {
      nodes->control.node.push_back(ctrl.node[0]);
    } else {
      // some node dies and restarts
      CHECK(ready_.load());
      for (size_t i = 0; i < nodes->control.node.size() - 1; ++i) {
        const auto &node = nodes->control.node[i];
        if (deadnodes_set->find(node.id) != deadnodes_set->end() &&
            node.role == ctrl.node[0].role) {
          auto &recovery_node = ctrl.node[0];
          // assign previous node id
          recovery_node.id = node.id;
          recovery_node.is_recovery = true;
          PS_VLOG(1) << "replace dead node " << node.DebugString()
                     << " by node " << recovery_node.DebugString();
          nodes->control.node[i] = recovery_node;
          recovery_nodes->control.node.push_back(recovery_node);
          break;
        }
      }
    }
  }

  // update my id
  for (size_t i = 0; i < ctrl.node.size(); ++i) {
    const auto &node = ctrl.node[i];
    if (my_node_.hostname == node.hostname && my_node_.port == node.port) {
      if (getenv("DMLC_RANK") == nullptr || my_node_.id == Meta::kEmpty) {
        SetNode(node);
      }
    }
  }
}

void Van::ProcessHearbeat(Message *msg) {
  auto &ctrl = msg->meta.control;
  time_t t = time(NULL);
  for (auto &node : ctrl.node) {
    postoffice_->UpdateHeartbeat(node.id, t);
    if (is_scheduler_) {
      Message heartbeat_ack;
      heartbeat_ack.meta.recver = node.id;
      heartbeat_ack.meta.control.cmd = Control::HEARTBEAT;
      heartbeat_ack.meta.control.node.push_back(my_node_);
      heartbeat_ack.meta.timestamp = timestamp_++;
      // send back heartbeat
      Send(heartbeat_ack);
    }
  }
}

void Van::ProcessInstanceBarrierCommand(Message *msg) {
  auto &ctrl = msg->meta.control;
  if (msg->meta.request) {
    if (barrier_count_.empty()) {
      barrier_count_.resize(8, 0);
    }
    int group = ctrl.barrier_group;
    ++barrier_count_[group];
    PS_VLOG(1) << "Instance barrier count for " << group << " : "
               << barrier_count_[group];
    if (barrier_count_[group] ==
        static_cast<int>(postoffice_->GetNodeIDs(group).size())) {
      barrier_count_[group] = 0;
      Message res;
      res.meta.request = false;
      res.meta.app_id = msg->meta.app_id;
      res.meta.customer_id = msg->meta.customer_id;
      res.meta.control.cmd = Control::INSTANCE_BARRIER;
      for (int r : postoffice_->GetNodeIDs(group)) {
        int recver_id = r;
        if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
          res.meta.recver = recver_id;
          res.meta.timestamp = timestamp_++;
          CHECK_GT(Send(res), 0);
        }
      }
    }
  } else {
    postoffice_->Manage(*msg);
  }
}

// process the (group) barrier command
void Van::ProcessBarrierCommand(Message *msg) {
  // For group-level barrier, we only respond to the requesters
  auto &ctrl = msg->meta.control;
  if (msg->meta.request) {
    int node_group = ctrl.barrier_group;
    group_barrier_requests_[node_group].push_back(msg->meta.sender);
    PS_VLOG(1) << "Barrier count for " << node_group << " : "
               << group_barrier_requests_[node_group].size();

    // Note: special handling for the scheduler thread,
    // since the scheduler always has just one instance,
    // while the worker/server may have multiple instances
    int group_size = postoffice_->group_size();
    int num_instances =
        static_cast<int>(postoffice_->GetNodeIDs(node_group).size());
    size_t num_expected_requests;
    if (node_group == kScheduler) {
      // scheduler only
      num_expected_requests = 1;
    } else if (node_group & kScheduler) {
      // scheduler, workers and/or servers
      num_expected_requests = (num_instances - 1) / group_size + 1;
    } else {
      num_expected_requests = num_instances / group_size;
    }
    if (group_barrier_requests_[node_group].size() == num_expected_requests) {
      Message res;
      res.meta.request = false;
      res.meta.app_id = msg->meta.app_id;
      res.meta.customer_id = msg->meta.customer_id;
      res.meta.control.cmd = Control::BARRIER;
      // response to the requesters
      for (int r : group_barrier_requests_[node_group]) {
        int recver_id = r;
        if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
          res.meta.recver = recver_id;
          res.meta.timestamp = timestamp_++;
          CHECK_GT(Send(res), 0);
        }
      }
      group_barrier_requests_[node_group].clear();
    }
  } else {
    postoffice_->Manage(*msg);
  }
}

void Van::ProcessDataMsg(Message *msg) {
  // data msg
  CHECK_NE(msg->meta.sender, Meta::kEmpty);
  CHECK_NE(msg->meta.recver, Meta::kEmpty);
  CHECK_NE(msg->meta.app_id, Meta::kEmpty);
  int app_id = msg->meta.app_id;
  int customer_id = postoffice_->is_worker() ? msg->meta.customer_id : app_id;
  auto *obj = postoffice_->GetCustomer(app_id, customer_id, 5);
  CHECK(obj) << "timeout (5 sec) to wait App " << app_id << " customer "
             << customer_id << " ready at " << my_node_.role;
  obj->Accept(*msg);

#ifdef USE_PROFILING
  if (is_van_profiling_ && msg->data.size()) {
    std::chrono::microseconds ms =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    // LOG(INFO) << (uint8_t)msg->data[0].data()[0] + 256 *
    // (uint8_t)msg->data[0].data()[1] << "\tvan_recv\t" << ms.count();
    if (postoffice_->is_worker()) {  // is worker
      if (msg->meta.push)
        fout_ << (uint8_t)msg->data[0].data()[0] +
                     256 * (uint8_t)msg->data[0].data()[1]
              << "\tworker_van_recv_push\t" << ms.count() << "\n";
      else
        fout_ << (uint8_t)msg->data[0].data()[0] +
                     256 * (uint8_t)msg->data[0].data()[1]
              << "\tworker_van_recv_pull\t" << ms.count() << "\n";
    } else {  // is server
      if (msg->meta.push)
        fout_ << (uint8_t)msg->data[0].data()[0] +
                     256 * (uint8_t)msg->data[0].data()[1]
              << "\tserver_van_recv_push\t" << ms.count() << "\n";
      else
        fout_ << (uint8_t)msg->data[0].data()[0] +
                     256 * (uint8_t)msg->data[0].data()[1]
              << "\tserver_van_recv_pull\t" << ms.count() << "\n";
    }
  }
#endif
}

void Van::ProcessAddNodeCommand(Message *msg, Meta *nodes,
                                Meta *recovery_nodes) {
  auto dead_nodes = postoffice_->GetDeadNodes(heartbeat_timeout_);
  std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
  auto &ctrl = msg->meta.control;

  UpdateLocalID(msg, &dead_set, nodes, recovery_nodes);

  if (is_scheduler_) {
    ProcessAddNodeCommandAtScheduler(msg, nodes, recovery_nodes);
  } else {
    for (const auto &node : ctrl.node) {
      std::string addr_str = node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(addr_str) == connected_nodes_.end()) {
        Connect(node);
        connected_nodes_[addr_str] = node.id;
      }
      if (!node.is_recovery && node.role == Node::SERVER) ++num_servers_;
      if (!node.is_recovery && node.role == Node::WORKER) ++num_workers_;
    }
    PS_VLOG(1) << my_node_.ShortDebugString() << " is connected to others";
    ready_ = true;
  }
}

void Van::Start(int customer_id, bool standalone) {
  // get scheduler info
  start_mu_.lock();
  if (init_stage == 0) {
    scheduler_.hostname = std::string(
        CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_URI")));
    scheduler_.num_ports = 1;
    scheduler_.port =
        atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_PORT")));
    scheduler_.ports[0] = scheduler_.port;
    scheduler_.dev_types[0] = CPU;
    scheduler_.dev_ids[0] = 0;
    scheduler_.role = Node::SCHEDULER;
    scheduler_.id = kScheduler;
    is_scheduler_ = postoffice_->is_scheduler();

    // get my node info
    if (is_scheduler_) {
      SetNode(scheduler_);
    } else {
      auto role = postoffice_->is_worker() ? Node::WORKER : Node::SERVER;
      // host
      const char *nhost = Environment::Get()->find("DMLC_NODE_HOST");
      std::string ip;
      if (nhost) ip = std::string(nhost);
      if (ip.empty()) {
        const char *itf = Environment::Get()->find("DMLC_INTERFACE");
        std::string interface;
        if (itf) interface = std::string(itf);
        if (interface.size()) {
          GetIP(interface, &ip);
        } else {
          GetAvailableInterfaceAndIP(&interface, &ip);
        }
        CHECK(!interface.empty()) << "failed to get the interface";
      }
      // num_ports
      const char *npstr = Environment::Get()->find("DMLC_NUM_PORTS");
      int num_ports = 1;
      if (npstr) {
        num_ports = atoi(npstr);
      }
      // ports
      std::array<int, 32> ports;
      int num_available_ports = GetAvailablePort(num_ports, &ports);

      const char *pstr = Environment::Get()->find("DMLC_PORT");
      if (pstr) ports[0] = atoi(pstr);
      CHECK(!ip.empty()) << "failed to get ip";
      CHECK_EQ(num_available_ports, num_ports)
          << "failed to get " << num_ports << " ports";
      Node node = my_node_;
      node.hostname = ip;
      node.role = role;
      node.num_ports = num_ports;
      node.ports = ports;
      node.port = ports[0];
      // cannot determine my id now, the scheduler will assign it later
      // set it explicitly to make re-register within a same process possible
      node.id = Node::kEmpty;
      node.customer_id = customer_id;
      SetNode(node);
    }

    // bind.
    my_node_.port = Bind(my_node_, is_scheduler_ ? 0 : 40);
    PS_VLOG(1) << "Bind to " << my_node_.DebugString();
    CHECK_NE(my_node_.port, -1) << "bind failed";

    // connect to the scheduler
    Connect(scheduler_);

    // for debug use
    if (Environment::Get()->find("PS_DROP_MSG")) {
      drop_rate_ = atoi(Environment::Get()->find("PS_DROP_MSG"));
    }
    // start receiver
    receiver_thread_ =
        std::unique_ptr<std::thread>(new std::thread(&Van::Receiving, this));
    init_stage++;
  }
  start_mu_.unlock();

  if (!is_scheduler_) {
    // let the scheduler know myself
    Message msg;
    Node customer_specific_node = my_node_;
    customer_specific_node.aux_id = postoffice_->preferred_rank();
    customer_specific_node.customer_id = customer_id;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::ADD_NODE;
    msg.meta.control.node.push_back(customer_specific_node);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }

  // wait until ready
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  start_mu_.lock();
  if (init_stage == 1) {
    // resender
    if (Environment::Get()->find("PS_RESEND") &&
        atoi(Environment::Get()->find("PS_RESEND")) != 0) {
      int timeout = 1000;
      if (Environment::Get()->find("PS_RESEND_TIMEOUT")) {
        timeout = atoi(Environment::Get()->find("PS_RESEND_TIMEOUT"));
      }
      resender_ = new Resender(timeout, 10, this);
    }

    if (!is_scheduler_) {
      // start heartbeat thread
      heartbeat_thread_ =
          std::unique_ptr<std::thread>(new std::thread(&Van::Heartbeat, this));
    }
    init_stage++;
  }
  start_mu_.unlock();
}

void Van::Stop() {
  // stop threads
  Message exit;
  exit.meta.control.cmd = Control::TERMINATE;
  exit.meta.recver = my_node_.id;
  // only customer 0 would call this method
  exit.meta.customer_id = 0;
  int ret = SendMsg(exit);
  CHECK_NE(ret, -1);
  receiver_thread_->join();
  init_stage = 0;
  if (!is_scheduler_) heartbeat_thread_->join();
  if (resender_) delete resender_;
  ready_ = false;
  connected_nodes_.clear();
  shared_node_mapping_.clear();
  send_bytes_ = 0;
  timestamp_ = 0;
  my_node_.id = Meta::kEmpty;
  barrier_count_.clear();

#ifdef USE_PROFILING
  if (is_van_profiling_) {
    fout_.clear();
    fout_.flush();
    fout_.close();
  }
#endif
}

int Van::Send(Message &msg) {
  int send_bytes = SendMsg(msg);
  CHECK_NE(send_bytes, -1) << this->GetType() << " sent -1 bytes";
  send_bytes_ += send_bytes;
  if (resender_) resender_->AddOutgoing(msg);
  PS_VLOG(2) << this->GetType() << " " << my_node_.id
             << "\tsent: " << msg.DebugString();
  return send_bytes;
}

void Van::Receiving() {
  Meta nodes;
  Meta recovery_nodes;  // store recovery nodes
  recovery_nodes.control.cmd = Control::ADD_NODE;

  while (true) {
    Message msg;
    int recv_bytes = RecvMsg(&msg);
    // For debug, drop received message
    if (ready_.load() && drop_rate_ > 0) {
      unsigned seed = time(NULL) + my_node_.id;
      if (rand_r(&seed) % 100 < drop_rate_) {
        LOG(WARNING) << "Drop message " << msg.DebugString();
        continue;
      }
    }

    CHECK_NE(recv_bytes, -1);
    recv_bytes_ += recv_bytes;
    PS_VLOG(2) << this->GetType() << " " << my_node_.id
               << "\treceived: " << msg.DebugString();
    // duplicated message
    if (resender_ && resender_->AddIncomming(msg)) continue;

    if (!msg.meta.control.empty()) {
      // control msg
      auto &ctrl = msg.meta.control;
      if (ctrl.cmd == Control::TERMINATE) {
        ProcessTerminateCommand();
        break;
      } else if (ctrl.cmd == Control::ADD_NODE) {
        ProcessAddNodeCommand(&msg, &nodes, &recovery_nodes);
      } else if (ctrl.cmd == Control::BARRIER) {
        ProcessBarrierCommand(&msg);
      } else if (ctrl.cmd == Control::INSTANCE_BARRIER) {
        ProcessInstanceBarrierCommand(&msg);
      } else if (ctrl.cmd == Control::HEARTBEAT) {
        ProcessHearbeat(&msg);
      } else {
        LOG(WARNING) << "Drop unknown typed message " << msg.DebugString();
      }
    } else {
      ProcessDataMsg(&msg);
    }
  }
}

int Van::GetPackMetaLen(const Meta &meta) {
  auto data_type_size = meta.data_type.size() * sizeof(int);
  return sizeof(RawMeta) + meta.body.size() + data_type_size +
         meta.control.node.size() * sizeof(RawNode);
}

void Van::PackMeta(const Meta &meta, char **meta_buf, int *buf_size) {
  *buf_size = GetPackMetaLen(meta);
  // allocate buffer only when needed
  if (*meta_buf == nullptr) {
    *meta_buf = new char[*buf_size + 1];
  }

  RawMeta *raw = (RawMeta *)*meta_buf;
  bzero(raw, sizeof(RawMeta));
  char *raw_body = *meta_buf + sizeof(RawMeta);
  auto data_type_len = meta.data_type.size();
  int *raw_data_type = (int *)(raw_body + meta.body.size());
  RawNode *raw_node = (RawNode *)(raw_data_type + data_type_len);

  // convert into raw buffer
  raw->head = meta.head;
  raw->app_id = meta.app_id;
  raw->timestamp = meta.timestamp;
  if (meta.body.size()) {
    memcpy(raw_body, meta.body.c_str(), meta.body.size());
    raw->body_size = meta.body.size();
  }
  raw->push = meta.push;
  raw->request = meta.request;
  raw->simple_app = meta.simple_app;
  raw->customer_id = meta.customer_id;
  int data_type_count = 0;
  for (auto d : meta.data_type) {
    raw_data_type[data_type_count] = d;
    data_type_count++;
  }
  raw->data_type_size = meta.data_type.size();
  raw->src_dev_type = meta.src_dev_type;
  raw->src_dev_id = meta.src_dev_id;
  raw->dst_dev_type = meta.dst_dev_type;
  raw->dst_dev_id = meta.dst_dev_id;
  auto ctrl = &(raw->control);
  if (!meta.control.empty()) {
    ctrl->cmd = meta.control.cmd;
    if (meta.control.cmd == Control::BARRIER ||
        meta.control.cmd == Control::INSTANCE_BARRIER) {
      ctrl->barrier_group = meta.control.barrier_group;
    } else if (meta.control.cmd == Control::ACK) {
      ctrl->msg_sig = meta.control.msg_sig;
    }
    ctrl->node_size = meta.control.node.size();
    int node_count = 0;
    for (const auto &n : meta.control.node) {
      raw_node[node_count].id = n.id;
      raw_node[node_count].role = n.role;
      raw_node[node_count].port = n.port;
      raw_node[node_count].num_ports = n.num_ports;
      bzero(raw_node[node_count].ports, 32 * sizeof(int));
      memcpy(raw_node[node_count].ports, n.ports.data(), 32 * sizeof(int));
      bzero(raw_node[node_count].dev_types, 32 * sizeof(int));
      memcpy(raw_node[node_count].dev_types, n.dev_types.data(),
             32 * sizeof(int));
      bzero(raw_node[node_count].dev_ids, 32 * sizeof(int));
      memcpy(raw_node[node_count].dev_ids, n.dev_ids.data(), 32 * sizeof(int));
      bzero(raw_node[node_count].hostname,
            sizeof(raw_node[node_count].hostname));
      memcpy(raw_node[node_count].hostname, n.hostname.c_str(),
             n.hostname.size());
      bzero(raw_node[node_count].endpoint_name,
            sizeof(raw_node[node_count].endpoint_name));
      memcpy(raw_node[node_count].endpoint_name, n.endpoint_name,
             sizeof(raw_node[node_count].endpoint_name));
      raw_node[node_count].endpoint_name_len = n.endpoint_name_len;
      raw_node[node_count].is_recovery = n.is_recovery;
      raw_node[node_count].customer_id = n.customer_id;
      raw_node[node_count].aux_id = n.aux_id;
      node_count++;
    }
  } else {
    ctrl->cmd = Control::EMPTY;
  }
  raw->data_size = meta.data_size;
  raw->key = meta.key;
  raw->addr = meta.addr;
  raw->val_len = meta.val_len;
  raw->option = meta.option;
  raw->sid = meta.sid;
}

void Van::UnpackMeta(const char *meta_buf, int buf_size, Meta *meta) {
  RawMeta *raw = (RawMeta *)meta_buf;
  const char *raw_body = meta_buf + sizeof(RawMeta);
  const int *raw_data_type = (const int *)(raw_body + raw->body_size);
  auto data_type_len = raw->data_type_size;
  const RawNode *raw_node = (RawNode *)(raw_data_type + data_type_len);

  // to meta
  meta->head = raw->head;
  meta->app_id = raw->app_id;
  meta->timestamp = raw->timestamp;
  meta->request = raw->request;
  meta->push = raw->push;
  meta->simple_app = raw->simple_app;
  meta->body = std::string(raw_body, raw->body_size);
  meta->customer_id = raw->customer_id;
  meta->data_type.resize(raw->data_type_size);
  for (int i = 0; i < raw->data_type_size; ++i) {
    meta->data_type[i] = static_cast<DataType>(raw_data_type[i]);
  }
  meta->src_dev_type = static_cast<DeviceType>(raw->src_dev_type);
  meta->src_dev_id = raw->src_dev_id;
  meta->dst_dev_type = static_cast<DeviceType>(raw->dst_dev_type);
  meta->dst_dev_id = raw->dst_dev_id;
  auto ctrl = &(raw->control);
  meta->control.cmd = static_cast<Control::Command>(ctrl->cmd);
  meta->control.barrier_group = ctrl->barrier_group;
  meta->control.msg_sig = ctrl->msg_sig;
  for (int i = 0; i < ctrl->node_size; ++i) {
    const auto &p = raw_node[i];
    Node n;
    n.role = static_cast<Node::Role>(p.role);
    n.port = p.port;
    n.num_ports = p.num_ports;
    n.hostname = p.hostname;
    n.id = p.id;
    n.is_recovery = p.is_recovery;
    n.customer_id = p.customer_id;
    n.aux_id = p.aux_id;
    n.endpoint_name_len = p.endpoint_name_len;
    bzero(n.endpoint_name, sizeof(n.endpoint_name));
    memcpy(n.endpoint_name, p.endpoint_name, sizeof(n.endpoint_name));
    bzero(n.ports.data(), 32 * sizeof(int));
    memcpy(n.ports.data(), p.ports, 32 * sizeof(int));
    bzero(n.dev_types.data(), 32 * sizeof(int));
    memcpy(n.dev_types.data(), p.dev_types, 32 * sizeof(int));
    bzero(n.dev_ids.data(), 32 * sizeof(int));
    memcpy(n.dev_ids.data(), p.dev_ids, 32 * sizeof(int));
    meta->control.node.push_back(n);
  }

  meta->data_size = raw->data_size;
  meta->key = raw->key;
  meta->addr = raw->addr;
  meta->val_len = raw->val_len;
  meta->option = raw->option;
  meta->sid = raw->sid;
}

void Van::Heartbeat() {
  const char *val = Environment::Get()->find("PS_HEARTBEAT_INTERVAL");
  const int interval = val ? atoi(val) : kDefaultHeartbeatInterval;
  while (interval > 0 && ready_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    Message msg;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::HEARTBEAT;
    msg.meta.control.node.push_back(my_node_);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }
}

bool Van::IsValidPushpull(const Message &msg) {
  if (!msg.meta.control.empty()) return false;
  if (msg.meta.simple_app) return false;
  return true;
}

}  // namespace ps
