
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

#ifndef PS_FABRIC_VAN_H_
#define PS_FABRIC_VAN_H_

#ifdef DMLC_USE_FABRIC

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_tagged.h>
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

#include "fabric_transport.h"
#include "fabric_utils.h"
#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"
#include "van_common.h"

namespace ps {
class FabricTransport;
struct FabricMemoryAllocator;
struct FabricEndpoint;

std::string hostport_str(const std::string &host, const int port) {
  return host + ":" + std::to_string(port);
}

static_assert(std::is_pod<RendezvousMsg>::value,
              "RendezvousMsg must be a POD type.");

class FabricVan : public Van {
 public:
  FabricVan() {
    struct fi_info *hints = nullptr;
    int fi_version, ret;

    // set hints for capacity and modes, create fabric, domain and cq
    hints = fi_allocinfo();
    CHECK(hints != nullptr) << "Failed to allocate hints";

    // hints to filter providers
    hints->ep_attr->type = FI_EP_RDM;
    hints->caps = FI_TAGGED | FI_MSG | FI_DIRECTED_RECV;
    hints->mode = FI_CONTEXT;
    hints->domain_attr->av_type = FI_AV_TABLE;
    hints->domain_attr->control_progress = FI_PROGRESS_MANUAL;
    hints->domain_attr->data_progress = FI_PROGRESS_MANUAL;
    hints->tx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->msg_order = FI_ORDER_SAS;

    // request for EFA as the provider
    hints->fabric_attr->prov_name = strdup("efa");
    fi_version = FI_VERSION(1, 8);

    // fi_getinfo
    ret = fi_getinfo(fi_version, nullptr, 0, 0, hints, &info_);
    CHECK_NE(ret, -FI_ENODATA) << "Could not find any optimal provider";
    check_err(ret, "fi_getinfo failed");
    struct fi_info *providers = info_;
    while (providers) {
      LOG(INFO) << "Found a fabric provider "
                << providers->fabric_attr->prov_name;
      providers = providers->next;
    }
  }

  ~FabricVan() { PS_VLOG(3) << "~FabricVan"; }

  virtual std::string GetType() const { return std::string("fabric"); }

 protected:
  void Start(int customer_id, bool standalone) override {
    start_mu_.lock();
    should_stop_ = false;
    should_stop_polling_ = false;

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    LOG(INFO) << "This is a " << role;

    start_mu_.unlock();

    zmq_ = Van::Create("zmq");
    zmq_->Start(customer_id, true);

    Van::Start(customer_id, false);
  }

  void Stop() override {
    PS_VLOG(1) << "Stopping " << my_node_.ShortDebugString();
    // stop zmq van
    {
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      std::string hostport = hostport_str(my_node_.hostname, my_node_.port);
      if (zmq_connections_.find(hostport) != zmq_connections_.end()) {
        int id = zmq_connections_[hostport];
        Message exit;
        exit.meta.control.cmd = Control::TERMINATE;
        exit.meta.recver = id;
        // only customer 0 would call this method
        exit.meta.customer_id = 0;
        int ret = zmq_->SendMsg(exit);
        CHECK_NE(ret, -1);
      }
    }

    should_stop_ = true;
    Van::Stop();

    PS_VLOG(1) << "Stopping event_polling_thread_";
    event_polling_thread_->join();
    event_polling_thread_.reset();

    PS_VLOG(1) << "Clearing endpoints.";
    {
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      for (auto &t : workers_) {
        t.second->join();
      }
      // endpoint must be cleared after thread->join
      worker_endpoints_.clear();
    }
    // free fabric info
    fi_freeinfo(info_);
  }

  int Bind(Node &node, int max_retry) override {
    CHECK_EQ(my_node_.num_ports, 1)
        << "fabric van does not support multiple ports";
    std::lock_guard<std::mutex> lk(endpoints_mu_);
    int my_port = zmq_->Bind(node, max_retry);
    PS_VLOG(3) << "Done zmq->Bind. My port is " << my_port;
    event_polling_thread_.reset(
        new std::thread(&FabricVan::EventPollingThread, this));
    return my_port;
  }

  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    PS_VLOG(3) << "Connect: " << node.DebugString() << " from "
               << my_node_.DebugString();
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      PS_VLOG(3) << "Fabric skipped connection to " << node.DebugString()
                 << ". My node is " << my_node_.DebugString();
      return;
    }
    if (node.id != Node::kEmpty) {
      FabricEndpoint *endpoint =
          GetOrCreateEndpoint(node.hostname, node.port, node.role);

      // set node id
      endpoint->SetNodeID(node.id);

      int connection_id = endpoint->connection_id;
      while (endpoint->status != FabricEndpoint::CONNECTED) {
        std::unique_lock<std::mutex> lk(endpoint->status_mu);
        endpoint->status = FabricEndpoint::CONNECTING;
        // addr_request message
        Message req;
        req.meta.recver = connection_id;
        req.meta.control.cmd = Control::ADDR_REQUEST;
        Node req_info;
        req_info.role = my_node_.role;
        req_info.hostname = my_node_.hostname;
        req_info.port = my_node_.port;
        req_info.id = my_node_.id;
        // auxilary id for tracking connection requests
        req_info.aux_id = node.id;
        endpoint->fabric_ctx->addr.CopyTo(req_info.endpoint_name,
                                          &req_info.endpoint_name_len);
        req.meta.control.node.push_back(req_info);
        zmq_->Send(req);

        endpoint->status_cv.wait(lk, [endpoint] {
          return endpoint->status != FabricEndpoint::CONNECTING;
        });

        if (endpoint->status == FabricEndpoint::CONNECTED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
  }

  int SendMsg(Message &msg) override {
    int remote_id = msg.meta.recver;
    PS_VLOG(3) << "SendMsg: " << msg.DebugString() << " to node " << remote_id;
    CHECK_NE(remote_id, Node::kEmpty);

    endpoints_mu_.lock();
    CHECK_NE(endpoints_.find(remote_id), endpoints_.end())
        << remote_id << " v.s. " << EndpointsDebugStr();
    FabricEndpoint *endpoint = endpoints_[remote_id];
    endpoints_mu_.unlock();

    int meta_len = GetPackMetaLen(msg.meta);
    size_t data_len = msg.meta.data_size;
    size_t total_len = meta_len + data_len;
    CHECK(meta_len);

    // pack meta info
    if (IsValidPushpull(msg)) {
      AddMeta(msg);
    }
    // push the request to corresponding send queue
    endpoint->send_queue.Push(msg);
    return total_len;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    // recv the meta first
    FabricBufferContext *buffer_ctx;
    recv_buffers_.WaitAndPop(&buffer_ctx);
    FabricEndpoint *endpoint = buffer_ctx->endpoint;

    msg->meta.recver = my_node_.id;
    msg->meta.sender = endpoint->node_id;

    // the second argument is actually deprecated,
    // we keep it as is in order to be compatible
    UnpackMeta(buffer_ctx->meta_buffer, buffer_ctx->meta_len, &msg->meta);
    int meta_len = GetPackMetaLen(msg->meta);

    int total_len = 0;
    total_len += meta_len;

    auto trans = CHECK_NOTNULL(endpoint->GetTransport());

    // PrintRecvLog(msg, buffer_ctx, meta_len);

    if (!IsValidPushpull(*msg)) {
      if (should_stop_.load()) {
        PS_VLOG(3) << "set should_stop_polling_ = true";
        should_stop_polling_ = true;
      }
      return total_len;
    }

    // valid data message
    if (msg->meta.push && msg->meta.request) {
      // push request
      CHECK_NE(msg->meta.sender, Node::kEmpty);
      total_len += trans->RecvPushRequest(msg, buffer_ctx, meta_len);
      // StoreWorkerTensorAddress(msg);
    } else if (!msg->meta.push && msg->meta.request) {
      // pull request
      total_len += trans->RecvPullRequest(msg, buffer_ctx, meta_len);
    } else if (msg->meta.push && !msg->meta.request) {
      // push response
      CHECK_NE(msg->meta.sender, Node::kEmpty);
      total_len += trans->RecvPushResponse(msg, buffer_ctx, meta_len);
    } else if (!msg->meta.push && !msg->meta.request) {
      // pull response
      total_len += trans->RecvPullResponse(msg, buffer_ctx, meta_len);
    } else {
      CHECK(0) << "unknown msg type";
    }

    if (should_stop_.load()) {
      VLOG(3) << "set should_stop_polling_ = true";
      should_stop_polling_ = true;
    }

    return total_len;
  }

  inline void SetNode(const Node &node) {
    my_node_ = node;
    zmq_->SetNode(node);
  }

 private:
  std::string EndpointsDebugStr() const {
    std::stringstream ss;
    ss << "{";
    for (auto it = endpoints_.begin(); it != endpoints_.end(); it++) {
      ss << std::to_string(it->first) << ",";
    }
    ss << "}";
    return ss.str();
  }

  void PrintRecvLog(Message *msg, FabricBufferContext *buffer_ctx,
                    int meta_len) {
    std::lock_guard<std::mutex> lock(log_mu_);

    if (!IsValidPushpull(*msg)) {
      PS_VLOG(4) << "Recv Control Message" << std::flush;
    } else if (msg->meta.push && msg->meta.request) {
      // push request
      PS_VLOG(4) << "Recv Push Request: key=" << msg->meta.key
                 << "\t timestamp=" << msg->meta.timestamp
                 << "\t sender=" << msg->meta.sender
                 << "\t tensor_len=" << buffer_ctx->data_len[1] << std::flush;
    } else if (!msg->meta.push && msg->meta.request) {
      // pull request
      PS_VLOG(4) << "Recv Pull Request: key=" << msg->meta.key
                 << "\t timestamp=" << msg->meta.timestamp
                 << "\t sender=" << msg->meta.sender << std::flush;
    } else if (msg->meta.push && !msg->meta.request) {
      // push response
      PS_VLOG(4) << "Recv Push Response: key=" << msg->meta.key
                 << "\t timestamp=" << msg->meta.timestamp
                 << "\t sender=" << msg->meta.sender << std::flush;
    } else if (!msg->meta.push && !msg->meta.request) {
      // pull response
      PS_VLOG(4) << "Recv Pull Response: key=" << msg->meta.key
                 << "\t timestamp=" << msg->meta.timestamp
                 << "\t sender=" << msg->meta.sender
                 << "\t tensor_len=" << msg->meta.val_len;
    }
  }

  FabricMessageBuffer *PrepareNewMsgBuf(Message &msg,
                                        FabricEndpoint *endpoint) {
    FabricMessageBuffer *msg_buf = new FabricMessageBuffer();
    auto meta_len = GetPackMetaLen(msg.meta);
    msg_buf->inline_len = meta_len;
    size_t meta_size = 0;
    msg_buf->inline_buf = endpoint->mem_allocator->Alloc(meta_len, &meta_size);
    msg_buf->data = msg.data;
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);

    // prepare send context
    FabricWRContext *ctx = new FabricWRContext();
    ctx->type = kSendWithData;
    msg_buf->reserved_context = ctx;
    ctx->private_data = msg_buf;
    void *data_buff = nullptr;
    size_t data_size = 0;
    if (msg.data.size() != 0) {
      // pull response / push request : size = 3
      // push response / pull request : size = 2
      CHECK_GE(msg.data.size(), 2)
          << "Unexpected number of data: " << msg.data.size();
      data_buff = static_cast<char *>(msg.data[1].data());
      data_size = msg.data[1].size();
    }
    PrepareWRContext(ctx, msg_buf->inline_buf, meta_size, data_buff, data_size);
    return msg_buf;
  }

  void AddMeta(Message &msg) {
    if (msg.meta.request) {
      msg.meta.key = DecodeKey(msg.data[0]);
      // push request
      if (msg.meta.push) {
        CHECK_EQ(msg.data.size(), 3) << msg.data.size();
        msg.meta.val_len = msg.data[1].size();
      }
    }
  }

  void ReleaseWRContext(FabricWRContext *context, FabricEndpoint *endpoint) {
    switch (context->type) {
      case kSendRendStart:
        endpoint->free_start_ctx.Push(context);
        break;
      case kSendRendReply:
        endpoint->free_reply_ctx.Push(context);
        break;
      case kSendWithData:
        endpoint->StoreRemoteContext(
            reinterpret_cast<FabricMessageBuffer *>(context->private_data),
            context);
        break;
      case kReceiveWithData:
        endpoint->PostRecv(context);
        break;
      case kReceiveRend:
        endpoint->PostRecv(context);
        break;
      default:
        CHECK(0);
    }
  }

  void HandleCQError(struct fid_cq *cq) {
    struct fi_cq_err_entry err_entry;
    int ret = fi_cq_readerr(cq, &err_entry, 1);
    if (ret == FI_EADDRNOTAVAIL) {
      LOG(WARNING) << "fi_cq_readerr: FI_EADDRNOTAVAIL";
    } else if (ret < 0) {
      LOG(FATAL) << "fi_cq_readerr failed. Return Code: " << ret << ". ERROR: "
                 << fi_cq_strerror(cq, err_entry.prov_errno, err_entry.err_data,
                                   nullptr, err_entry.err_data_size);
    } else {
      check_err(-err_entry.err, "fi_cq_read failed. retrieved error: ");
    }
  }

  std::string EntryDebugStr(const struct fi_cq_tagged_entry &cq_entry) const {
    uint64_t flags = cq_entry.flags;
    bool is_send = flags & FI_SEND;
    bool is_recv = flags & FI_RECV;
    uint64_t tag = cq_entry.tag;
    std::stringstream ss;
    ss << "op_context = " << cq_entry.op_context << ", flags = " << flags;
    if (is_send) ss << " sent ";
    if (is_recv) ss << " received ";
    bool start_msg = tag == kRendezvousStartMask;
    bool reply_msg = tag == kRendezvousReplyMask;
    bool data_msg = not start_msg and not reply_msg;
    if (data_msg) ss << "a control message. tag = " << tag;
    if (start_msg) ss << "a rendevous start message";
    if (reply_msg) ss << "a rendevous reply message";
    return ss.str();
  }

  void HandleCompletionEvent(const struct fi_cq_tagged_entry &cq_entry,
                             FabricEndpoint *endpoint) {
    uint64_t flags = cq_entry.flags;
    bool is_tagged = flags & FI_TAGGED;
    bool is_send = flags & FI_SEND;
    bool is_recv = flags & FI_RECV;
    CHECK(is_tagged) << "Completion events must be tagged";
    uint64_t tag = cq_entry.tag;

    // op_context is the local context
    PS_VLOG(5) << EntryDebugStr(cq_entry);
    FabricWRContext *context =
        static_cast<FabricWRContext *>(cq_entry.op_context);
    CHECK_NE(context, nullptr)
        << "FabricWRContext should not be null: " << context;

    CHECK_EQ(context->buffers[0].iov_base, cq_entry.buf)
        << "buffer address does not match";
    bool start_msg = (tag == kRendezvousStartMask);
    bool reply_msg = (tag == kRendezvousReplyMask);
    bool data_msg = !start_msg && !reply_msg;
    if (is_send) {
      ReleaseWRContext(context, endpoint);
    } else if (is_recv) {
      // receive
      if (start_msg) {
        // kRendezvousStart
        RendezvousMsg *req =
            static_cast<RendezvousMsg *>(context->buffers[0].iov_base);
        auto trans = CHECK_NOTNULL(endpoint->GetTransport());
        trans->SendRendezvousReply(req, endpoint->addr_pool);
        ReleaseWRContext(context, endpoint);
      } else if (data_msg) {
        // the tag is the address of the buffer context posted for recving
        FabricBufferContext *buf_ctx = endpoint->addr_pool.GetAddress(tag);
        recv_buffers_.Push(buf_ctx);
        ReleaseWRContext(context, buf_ctx->endpoint);
      } else {
        RendezvousMsg *req =
            static_cast<RendezvousMsg *>(context->buffers[0].iov_base);

        // kRendezvousReply
        uint64_t origin_addr = req->origin_addr;
        FabricMessageBuffer *msg_buf =
            reinterpret_cast<FabricMessageBuffer *>(origin_addr);
        CHECK(msg_buf != nullptr);

        FabricWRContext *send_context = msg_buf->reserved_context;
        // now we know which tag to use to reach the peer
        send_context->tag = req->tag;

        endpoint->StoreRemoteContext(msg_buf, send_context);

        // PrintSendLog(*msg, msg_buf, addr_tuple);

        auto trans = CHECK_NOTNULL(endpoint->GetTransport());
        trans->Send(send_context);

        ReleaseWRContext(context, endpoint);
      }
    } else {
      LOG(FATAL) << "unknown completion entry" << flags;
    }
  }

  FabricEndpoint *WorkerCreateEndpoint(const std::string hostname,
                                       const int port, const Node::Role role,
                                       fi_info *info) {
    FabricEndpoint *endpoint = nullptr;
    {
      const std::string hostport = hostport_str(hostname, port);
      // initialize context
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      endpoint = worker_endpoints_[hostport].get();
      CHECK(endpoint != nullptr);

      // init fabric_ctx
      PS_VLOG(3) << "Initializing a fabric endpoint";
      endpoint->Create(hostport, info);

      // init zmq connection
      int connection_id = 10000 + zmq_connections_.size();
      Node conn_node;
      conn_node.role = role;
      conn_node.id = connection_id;
      conn_node.hostname = hostname;
      conn_node.port = port;
      zmq_->Connect(conn_node);
      zmq_connections_.emplace(hostport, connection_id);
      endpoint->connection_id = connection_id;
    }
    // unblock other threads waiting for this endpoint
    {
      std::lock_guard<std::mutex> lk(endpoint->status_mu);
      endpoint->status = FabricEndpoint::IDLE;
    }
    endpoint->status_cv.notify_all();
    return endpoint;
  }

  void WorkerInitPeerAddr(FabricEndpoint *endpoint) {
    // wait for peer addr
    PS_VLOG(4) << "wait peer addr info to initialize the endpoint";
    {
      std::unique_lock<std::mutex> lk(endpoint->addr_mu);
      endpoint->addr_cv.wait(lk,
                             [endpoint] { return endpoint->peer_addr_ready; });
    }
    endpoint->Init();
  }

  void WorkerSendMsg(FabricEndpoint *endpoint) {
    Message msg;
    endpoint->send_queue.WaitAndPop(&msg);
    int meta_len = GetPackMetaLen(msg.meta);
    auto trans = CHECK_NOTNULL(endpoint->GetTransport());
    // start rendezvous if no remote info
    if (!IsValidPushpull(msg)) {
      FabricMessageBuffer *msg_buf = PrepareNewMsgBuf(msg, endpoint);
      endpoint->StoreMsgBuf(msg_buf, msg);
      trans->SendRendezvousBegin(msg, msg_buf);
      return;
    } else {
      auto is_push = msg.meta.push;
      auto key = msg.meta.key;
      // store address if this is a pull request for zero-copy pull
      if (!is_push && msg.meta.request) endpoint->StorePullAddr(msg);
      if (!endpoint->HasRemoteInfo(key, is_push)) {
        FabricMessageBuffer *msg_buf = PrepareNewMsgBuf(msg, endpoint);
        endpoint->StoreMsgBuf(msg_buf, msg);
        trans->SendRendezvousBegin(msg, msg_buf);
        PS_VLOG(6) << "SendRendezvousBegin " << msg.DebugString();
        return;
      }
    }
    auto context_tuple =
        endpoint->GetRemoteContext(msg.meta.key, msg.meta.push);
    FabricWRContext *ctx = std::get<0>(context_tuple);  // WR context
    FabricMessageBuffer *msg_buf =
        std::get<1>(context_tuple);  // local message buffer

    // prepare new meta and data
    CHECK_EQ(msg_buf->inline_len, (size_t)meta_len);
    CHECK(msg_buf->inline_buf);
    msg_buf->data = msg.data;  // may not need this

    // the meta data might change. e.g. timestamp
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);

    // PrintSendLog(msg, msg_buf, addr_tuple);

    // knew remote address, directly send
    trans->Send(ctx);
  }

  void WorkerPollCQ(FabricEndpoint *endpoint,
                    struct fi_cq_tagged_entry *cq_entries) {
    int ret = fi_cq_read(endpoint->fabric_ctx->cq, cq_entries,
                         kMaxConcurrentWorkRequest);
    if (ret == -FI_EAGAIN) {
      return;
    } else if (ret == -FI_EAVAIL) {
      HandleCQError(endpoint->fabric_ctx->cq);
    } else if (ret < 0) {
      check_err(ret, "fi_cq_read failed");
    } else {
      CHECK_NE(ret, 0) << "at least one completion event is expected";
      PS_VLOG(3) << ret << " completion events ... ";
      for (int i = 0; i < ret; ++i) {
        const auto &cq_entry = cq_entries[i];
        HandleCompletionEvent(cq_entry, endpoint);
      }
    }
  }

  void WorkerThread(const std::string hostname, const int port,
                    const Node::Role role) {
    FabricEndpoint *endpoint =
        WorkerCreateEndpoint(hostname, port, role, info_);
    WorkerInitPeerAddr(endpoint);
    // Pre-allocated work completions array used for polling
    struct fi_cq_tagged_entry cq_entries[kMaxConcurrentWorkRequest];
    // non-blocking loop
    while (!should_stop_polling_.load()) {
      if (endpoint->send_queue.Size()) {
        WorkerSendMsg(endpoint);
      }
      // read from completion queue
      WorkerPollCQ(endpoint, cq_entries);
    }
    PS_VLOG(1) << "WorkerThread exited " << hostname << ":" << port;
  }

  void EventPollingThread() {
    while (true) {
      Message msg;
      int recv_bytes = zmq_->RecvMsg(&msg);
      CHECK_NE(recv_bytes, -1) << "unexpected message size " << recv_bytes;
      PS_VLOG(3) << "zmq recv: " << msg.DebugString();
      CHECK(!msg.meta.control.empty()) << "msg.meta.control is empty";
      auto &ctrl = msg.meta.control;
      if (ctrl.cmd == Control::ADDR_REQUEST) {
        OnConnectRequest(msg);
      } else if (ctrl.cmd == Control::ADDR_RESOLVED) {
        OnConnected(msg);
      } else if (ctrl.cmd == Control::TERMINATE) {
        break;
      } else {
        LOG(FATAL) << "Drop unknown typed message " << msg.DebugString();
      }
    }
    PS_VLOG(1) << "EventPollingThread exited";
  }

  FabricEndpoint *GetOrCreateEndpoint(const std::string &host, const int port,
                                      Node::Role role) {
    const std::string hostport = hostport_str(host, port);
    FabricEndpoint *endpoint;
    // create worker thread
    {
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      if (workers_.find(hostport) == workers_.end()) {
        CHECK(worker_endpoints_.find(hostport) == worker_endpoints_.end());
        worker_endpoints_[hostport].reset(new FabricEndpoint());
        auto thread =
            new std::thread(&FabricVan::WorkerThread, this, host, port, role);
        workers_[hostport].reset(thread);
      }
      endpoint = worker_endpoints_[hostport].get();
    }
    CHECK(endpoint != nullptr);
    // wait for endpoint context to be initialized
    {
      std::unique_lock<std::mutex> lk(endpoint->status_mu);
      endpoint->status_cv.wait(
          lk, [endpoint] { return endpoint->status != FabricEndpoint::EMPTY; });
    }
    return endpoint;
  };

  void OnConnectRequest(const Message &msg) {
    // start worker thread if necessary
    const auto &req_info = msg.meta.control.node[0];
    PS_VLOG(3) << "EQ: connection request " << req_info.DebugString();
    FabricEndpoint *endpoint =
        GetOrCreateEndpoint(req_info.hostname, req_info.port, req_info.role);
    // retrieve connection id - the connection_id is NOT the node id
    int connection_id = endpoint->connection_id;
    // set peer address
    endpoint->SetPeerAddr(req_info.endpoint_name, req_info.endpoint_name_len);
    // set endpoint node id
    if (req_info.id != Node::kEmpty) {
      endpoint->SetNodeID(req_info.id);
      PS_VLOG(3) << "Set endpoint ID = " << req_info.id;
    }

    // send back my connection info
    Node resp_info;
    endpoint->fabric_ctx->addr.CopyTo(resp_info.endpoint_name,
                                      &resp_info.endpoint_name_len);
    resp_info.hostname = my_node_.hostname;
    resp_info.port = my_node_.port;
    resp_info.aux_id = req_info.aux_id;
    // reply ADDR_RESOLVED message
    Message reply;
    reply.meta.recver = connection_id;
    reply.meta.control.cmd = Control::ADDR_RESOLVED;
    reply.meta.control.node.push_back(resp_info);
    zmq_->Send(reply);
  }

  void OnConnected(const Message &msg) {
    const auto &addr_info = msg.meta.control.node[0];
    const int node_id = addr_info.aux_id;
    const std::string hostport =
        hostport_str(addr_info.hostname, addr_info.port);
    PS_VLOG(3) << "EQ: connected reply" << addr_info.DebugString();
    // init node id -> endpoint lookup
    FabricEndpoint *endpoint = nullptr;
    {
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      endpoint = worker_endpoints_[hostport].get();
      endpoints_[node_id] = endpoint;
    }
    CHECK(endpoint != nullptr);
    endpoint->SetPeerAddr(addr_info.endpoint_name, addr_info.endpoint_name_len);
    {
      std::lock_guard<std::mutex> lk(endpoint->status_mu);
      endpoint->status = FabricEndpoint::CONNECTED;
    }
    endpoint->status_cv.notify_all();
  }

  // stop signals
  std::atomic<bool> should_stop_;
  // stop signal for the non-blocking polling thread
  std::atomic<bool> should_stop_polling_;

  std::mutex endpoints_mu_;
  // node id -> endpoint for fast lookup
  std::unordered_map<int, FabricEndpoint *> endpoints_;
  // host:port -> endpoint for connections
  std::unordered_map<std::string, std::unique_ptr<FabricEndpoint>>
      worker_endpoints_;
  // worker threads
  std::unordered_map<std::string, std::unique_ptr<std::thread>> workers_;

  // event thread
  std::unique_ptr<std::thread> event_polling_thread_;
  // Recv buffer queue
  ThreadsafeQueue<FabricBufferContext *> recv_buffers_;

  // whether my role is server or not
  bool is_server;

  // note that ZMQ use the node id to identify the senders.
  // to setup the connection for libfabric, we don't know the node id ahead of
  // time therefore, we need to use the add sender / receiver hostport to the
  // message meta such that when we unpack the message, we can still know where
  // the message was sent
  std::unordered_map<std::string, int> zmq_connections_;
  Van *zmq_;

  bool is_worker_;
  // fabric provider info
  struct fi_info *info_;

  std::mutex log_mu_;

  // TODO(haibin): make it configurable
  int kMaxConcurrentWorkRequest = 4224;  // 128 + 2048 * 2

};  // FabricVan
};  // namespace ps

#endif  // DMLC_USE_FABRIC
#endif  // PS_FABRIC_VAN_H_
