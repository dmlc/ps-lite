
// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =============================================================================

#ifndef PS_MULTI_VAN_H_
#define PS_MULTI_VAN_H_

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <zmq.h>

#include <algorithm>
#include <map>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"
#include "van_common.h"

namespace ps {

const int ID_OFFSET = 1000000;
const int MAX_NUM_IDS = 1000;

class MultiVanBufferContext {
 public:
  int msg_len;
  int src_idx;
  Message msg;
};

// A van that uses multiple 0mq van under the hood. Implemented for testing
// purpose. It supports multiple CPU devices and multiple ports.
class MultiVan : public Van {
 public:
  MultiVan(Postoffice* postoffice) : Van(postoffice), postoffice_(postoffice) {}

  Postoffice* postoffice_;

  ~MultiVan() { PS_VLOG(1) << "~MultiVan"; }

  virtual std::string GetType() const { return std::string("multivan"); }

 protected:
  void Start(int customer_id, bool standalone) override {
    start_mu_.lock();
    should_stop_ = false;

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    LOG(INFO) << "This is a " << role;
    start_mu_.unlock();

    // only 1 port needed for the scheduler
    is_scheduler_ = "scheduler" == role;
    num_ports_ = 1;
    const char* npstr = Environment::Get()->find("DMLC_NUM_PORTS");
    if (npstr && !is_scheduler_) num_ports_ = atoi(npstr);

    // van creation
    for (int i = 0; i < num_ports_; ++i) {
      auto van = Van::Create("zmq", postoffice_);
      van->Start(customer_id, true);
      vans_.push_back(van);
    }
    Van::Start(customer_id, false);
  }

  void Stop() override {
    PS_VLOG(1) << "Stopping " << my_node_.ShortDebugString();
    Van::Stop();

    // send a TERMINATE message to myself
    should_stop_ = true;
    for (int i = 0; i < num_ports_; ++i) {
      Message msg;
      msg.meta.control.cmd = Control::TERMINATE;
      msg.meta.recver = my_nodes_[i].id;
      vans_[i]->SendMsg(msg);
    }

    PS_VLOG(1) << "Stopping polling_threads_";
    for (auto& thread : polling_threads_) {
      thread->join();
      thread.reset();
    }

    // stop zmq vans
    for (auto van : vans_) {
      van->Stop();
    }
    PS_VLOG(1) << "polling_threads_ stopped";
  }

  int Bind(Node& node, int max_retry) override {
    std::lock_guard<std::mutex> lk(endpoints_mu_);
    CHECK_EQ(node.num_ports, num_ports_);
    my_nodes_.resize(num_ports_);
    for (int i = 0; i < num_ports_; ++i) {
      int port = node.ports[i];
      Node one_node = node;
      one_node.id = EncodeManagedID(node.id, i);
      one_node.num_ports = 1;
      one_node.ports[0] = port;
      one_node.port = node.port;
      one_node.dev_types[i] = CPU;
      one_node.dev_ids[i] = i;
      int bound_port = vans_[i]->Bind(one_node, max_retry);

      // update node with port and device info
      node.dev_types[i] = CPU;
      node.dev_ids[i] = i;
      node.ports[i] = bound_port;
      my_nodes_[i] = one_node;
      polling_threads_.emplace_back(
          new std::thread(&MultiVan::PollingThread, this, i));
    }
    return node.ports[0];
  }

  void Connect(const Node& node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      PS_VLOG(3) << "Multivan skipped connection to " << node.DebugString()
                 << ". My node is " << my_node_.DebugString();
      return;
    }
    for (int i = 0; i < num_ports_; ++i) {
      for (int j = 0; j < node.num_ports; ++j) {
        // connect each van pair
        Node one_node = node;
        // the underlying van uses id: 0001, 1001, 2001, etc
        one_node.id = EncodeManagedID(node.id, j);
        one_node.num_ports = 1;
        one_node.port = node.ports[j];
        one_node.ports[0] = one_node.port;
        PS_VLOG(3) << "Connect: " << one_node.DebugString() << " from "
                   << my_nodes_[i].DebugString();
        vans_[i]->Connect(one_node);
      }
    }
  }

  int SendMsg(Message& msg) override {
    int remote_id = msg.meta.recver;
    CHECK_NE(remote_id, Node::kEmpty);
    // XXX assume device IDs are: [0, 1 ... num_ports - 1]
    bool pushpull = IsValidPushpull(msg);
    int src_idx = 0;
    int dst_idx = 0;
    if (pushpull && msg.data.size() == 3) {
      auto& data = msg.data[1];
      CHECK(data.src_device_type_ == CPU);
      CHECK(data.dst_device_type_ == CPU);
      src_idx = data.src_device_id_;
      dst_idx = data.dst_device_id_;
      CHECK_EQ(my_nodes_[src_idx].dev_types[0], data.src_device_type_);
      CHECK_EQ(my_nodes_[src_idx].dev_ids[0], data.src_device_id_);
      // TODO: check msg.meta.src_dev_ids, types, etc.
    }
    Message van_msg = msg;
    auto van = vans_[src_idx];
    van_msg.meta.sender = EncodeManagedID(msg.meta.sender, src_idx);
    van_msg.meta.recver = EncodeManagedID(msg.meta.recver, dst_idx);
    PS_VLOG(3) << "SendMsg: " << van_msg.DebugString();
    return van->SendMsg(van_msg);
  }

  void RegisterRecvBuffer(Message& msg) {
    Message van_msg = msg;
    CHECK_EQ(msg.data.size(), 3);
    int src_idx = msg.data[1].src_device_id_;
    int dst_idx = msg.data[1].dst_device_id_;
    van_msg.meta.sender = EncodeManagedID(msg.meta.sender, src_idx);
    auto van = vans_[dst_idx];
    van->RegisterRecvBuffer(van_msg);
  }

  int RecvMsg(Message* msg) override {
    msg->data.clear();
    MultiVanBufferContext ctx;
    recv_buffers_.WaitAndPop(&ctx);
    PS_VLOG(3) << "RecvMsg: " << ctx.msg.DebugString();
    ctx.msg.meta.sender = DecodeMangedID(ctx.msg.meta.sender);
    ctx.msg.meta.recver = DecodeMangedID(ctx.msg.meta.recver);
    *msg = ctx.msg;
    return ctx.msg_len;
  }

  inline void SetNode(const Node& node) {
    my_node_ = node;
    my_nodes_.resize(num_ports_);
    for (int i = 0; i < num_ports_; ++i) {
      Node one_node = node;
      one_node.id = EncodeManagedID(node.id, i);
      PS_VLOG(3) << "Child " << i << " id=" << one_node.id;
      one_node.num_ports = 1;
      one_node.ports[0] = node.ports[i];
      one_node.port = node.ports[i];
      one_node.dev_types[0] = node.dev_types[i];
      one_node.dev_ids[0] = node.dev_ids[i];
      vans_[i]->SetNode(one_node);
      my_nodes_[i] = one_node;
    }
  }

 private:
  int EncodeManagedID(int id, int index) {
    if (id == Node::kEmpty) {
      return Node::kEmpty;
    }
    CHECK(id < MAX_NUM_IDS);
    return ID_OFFSET + id + index * MAX_NUM_IDS;
  }

  int DecodeMangedID(int managed_id) {
    if (managed_id == Node::kEmpty) {
      return managed_id;
    }
    managed_id -= ID_OFFSET;
    int id = managed_id % MAX_NUM_IDS;
    return id;
  }

  void PollingThread(int index) {
    while (!should_stop_) {
      Message msg;
      int recv_bytes = vans_[index]->RecvMsg(&msg);
      MultiVanBufferContext ctx;
      ctx.msg_len = recv_bytes;
      ctx.src_idx = index;
      ctx.msg = msg;
      recv_buffers_.Push(ctx);
    }
    PS_VLOG(3) << "PollingThread exited";
  }

  // stop signals
  std::atomic<bool> should_stop_;
  std::mutex endpoints_mu_;

  // event thread
  std::vector<std::unique_ptr<std::thread>> polling_threads_;
  // Recv buffer queue
  ThreadsafeQueue<MultiVanBufferContext> recv_buffers_;

  std::vector<Van*> vans_;

  bool is_scheduler_;
  int num_ports_;
  std::vector<Node> my_nodes_;

};  // FabricVan
};  // namespace ps

#endif  // PS_FABRIC_VAN_H_
