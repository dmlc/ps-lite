// Copyright 2019 Bytedance Inc. or its affiliates. All Rights Reserved.
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

#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_

#ifdef DMLC_USE_RDMA

#include "rdma_transport.h"
#include "rdma_utils.h"

namespace ps {

class RDMAVan : public Van {
 public:
  RDMAVan(Postoffice *postoffice) : Van(postoffice), postoffice_(postoffice) {
    CHECK_EQ(ibv_fork_init(), 0) << strerror(errno);
  }
  ~RDMAVan() {}

  virtual std::string GetType() const { return std::string("rdma"); }

  Postoffice *postoffice_;

 protected:
  void Start(int customer_id, bool standalone) override {
    start_mu_.lock();
    should_stop_ = false;

    auto val = Environment::Get()->find("BYTEPS_ENABLE_IPC");
    disable_ipc_ = val ? !atoi(val) : true;
    if (disable_ipc_) {
      LOG(INFO) << "Shared memory IPC has been disabled";
    } else {
      std::string role = Environment::Get()->find("DMLC_ROLE");
      if (role == "joint") {
        LOG(INFO) << "You are using IPC in joint mode, make sure no P2P "
                     "operations are involved";
      }
    }
    if (event_channel_ == nullptr) {
      event_channel_ = rdma_create_event_channel();
      CHECK(event_channel_) << "Create RDMA event channel failed";

      cm_event_polling_thread_.reset(
          new std::thread(&RDMAVan::PollEvents, this));
    }

    // enable logging
    val = Environment::Get()->find("BYTEPS_PRINT_RDMA_LOG");
    enable_log_ = val ? atoi(val) : false;
    if (enable_log_) LOG(INFO) << "Enable RDMA logging.";

    val = Environment::Get()->find("BYTEPS_RDMA_MAX_CONCURR_WR");
    if (val) {
      // should make sure: kMaxConcurrentWorkRequest >= kStartDepth +
      // kReplyDepth + kRxDepth
      kMaxConcurrentWorkRequest = atoi(val);

      auto start_depth_env =
          Environment::Get()->find("BYTEPS_RDMA_START_DEPTH");
      auto rx_depth_env = Environment::Get()->find("BYTEPS_RDMA_RX_DEPTH");

      auto start_depth = start_depth_env ? atoi(start_depth_env) : 128;
      auto rx_depth = rx_depth_env ? atoi(rx_depth_env) : 2048;
      auto reply_depth = rx_depth;

      CHECK_GE(kMaxConcurrentWorkRequest, start_depth + reply_depth + rx_depth)
          << "Should make sure: kMaxConcurrentWorkRequest >= kStartDepth + "
             "kReplyDepth + kRxDepth";
    }

    start_mu_.unlock();
    if (!standalone) Van::Start(customer_id, false);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();

    should_stop_ = true;
    CHECK(should_stop_);

    PS_VLOG(1) << "Stopping cq_polling_thread_.";
    cq_polling_thread_->join();
    cq_polling_thread_.reset();

    PS_VLOG(1) << "Stopping cm_event_polling_thread_.";
    cm_event_polling_thread_->join();
    cm_event_polling_thread_.reset();

    PS_VLOG(1) << "Clearing memory allocator.";
    mem_allocator_.reset();

    PS_VLOG(1) << "Clearing endpoints.";
    incoming_.clear();
    {
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      endpoints_.clear();
    }

    PS_VLOG(1) << "Destroying cq and pd.";
    CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
    CHECK(!ibv_destroy_comp_channel(comp_event_channel_))
        << "Failed to destroy channel";

    for (auto &it : mem_mr_) ibv_dereg_mr(it.second);

    // TODO: ibv_dealloc_pd sometimes complains resource busy, need to fix this
    // CHECK(!ibv_dealloc_pd(pd_)) << "Failed to deallocate PD: " <<
    // strerror(errno);

    PS_VLOG(1) << "Destroying listener.";
    rdma_destroy_id(listener_);
    rdma_destroy_event_channel(event_channel_);
  }

  int Bind(Node &node, int max_retry) override {
    CHECK_EQ(my_node_.num_ports, 1)
        << "RDMA van does not support multiple ports";
    CHECK(rdma_create_id(event_channel_, &listener_, nullptr, RDMA_PS_TCP) == 0)
        << "Create RDMA connection identifier failed";

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    auto val = Environment::Get()->find("DMLC_NODE_HOST");
    if (val) {
      PS_VLOG(1) << "bind to DMLC_NODE_HOST: " << std::string(val);
      addr.sin_addr.s_addr = inet_addr(val);
    }

    addr.sin_family = AF_INET;
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL) + port);
    for (int i = 0; i < max_retry + 1; ++i) {
      addr.sin_port = htons(port);
      if (rdma_bind_addr(listener_,
                         reinterpret_cast<struct sockaddr *>(&addr)) == 0) {
        break;
      }
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    CHECK(rdma_listen(listener_, kRdmaListenBacklog) == 0)
        << "Listen RDMA connection failed: " << strerror(errno);
    return port;
  }

  void Connect(const Node &node) override {
    PS_VLOG(1) << "Connecting to Node " << node.id
               << ", My_Node=" << my_node_.id;
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }

    if (node.id != Node::kEmpty) {
      endpoints_mu_.lock();
      auto it = endpoints_.find(node.id);

      // if there is an endpoint with pending connection
      if (it != endpoints_.end()) {
        endpoints_.erase(it);
      }

      Endpoint *endpoint;
      endpoints_[node.id] = std::make_unique<Endpoint>();
      endpoint = endpoints_[node.id].get();
      endpoints_mu_.unlock();

      endpoint->SetNodeID(node.id);

      struct addrinfo *remote_addr;
      CHECK_EQ(
          getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(),
                      nullptr, &remote_addr),
          0);

      while (endpoint->status != Endpoint::CONNECTED) {
        std::unique_lock<std::mutex> lk(endpoint->connect_mu);
        endpoint->status = Endpoint::CONNECTING;

        if (endpoint->cm_id != nullptr) {
          rdma_destroy_qp(endpoint->cm_id);
          CHECK_EQ(rdma_destroy_id(endpoint->cm_id), 0) << strerror(errno);
          endpoint->cm_id = nullptr;
        }

        CHECK_EQ(rdma_create_id(event_channel_, &endpoint->cm_id, nullptr,
                                RDMA_PS_TCP),
                 0)
            << "Create RDMA connection identifier failed";
        endpoint->cm_id->context = endpoint;

        auto val = Environment::Get()->find("DMLC_NODE_HOST");
        if (val) {
          struct addrinfo *addr;
          auto rc = getaddrinfo(val, "", NULL, &addr);
          CHECK_EQ(rc, 0) << "getaddrinfo failed: " << gai_strerror(rc);

          CHECK_EQ(rdma_resolve_addr(endpoint->cm_id, addr->ai_addr,
                                     remote_addr->ai_addr, kTimeoutms),
                   0)
              << "Resolve RDMA address failed with errno: " << strerror(errno);
        } else {
          CHECK_EQ(rdma_resolve_addr(endpoint->cm_id, nullptr,
                                     remote_addr->ai_addr, kTimeoutms),
                   0)
              << "Resolve RDMA address failed with errno: " << strerror(errno);
        }

        endpoint->cv.wait(lk, [endpoint] {
          return endpoint->status != Endpoint::CONNECTING;
        });

        if (endpoint->status == Endpoint::CONNECTED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }

      bool is_local_node =
          disable_ipc_ ? false
                       : (node.hostname == my_node_.hostname ? true : false);
      {
        std::lock_guard<std::mutex> lk(local_mu_);
        is_local_[node.id] = is_local_node;
      }

      LOG(INFO) << "Connect to Node " << node.id
                << " with Transport=" << (is_local_node ? "IPC" : "RDMA");

      std::shared_ptr<Transport> t =
          is_local_node ? std::make_shared<IPCTransport>(
                              endpoint, mem_allocator_.get(), postoffice_)
                        : std::make_shared<RDMATransport>(
                              endpoint, mem_allocator_.get(), postoffice_);
      endpoint->SetTransport(t);

      freeaddrinfo(remote_addr);
    }
  }

  int SendMsg(Message &msg) override {
    int remote_id = msg.meta.recver;
    CHECK_NE(remote_id, Meta::kEmpty);

    endpoints_mu_.lock();
    CHECK_NE(endpoints_.find(remote_id), endpoints_.end());
    Endpoint *endpoint = endpoints_[remote_id].get();
    endpoints_mu_.unlock();

    int meta_len = GetPackMetaLen(msg.meta);
    size_t data_len = msg.meta.data_size;
    size_t total_len = meta_len + data_len;
    CHECK(meta_len);

    RegisterMemory(msg);

    // pack meta info
    if (IsValidPushpull(msg)) {
      AddMeta(msg);
    }

    auto trans = CHECK_NOTNULL(endpoint->GetTransport());

    // start rendezvous if no remote info
    if (!IsValidPushpull(msg)) {
      MessageBuffer *msg_buf = PrepareNewMsgBuf(msg);
      StoreMsgBuf(msg_buf, msg);
      trans->SendRendezvousBegin(msg, msg_buf);
      return total_len;

    } else {
      auto is_push = msg.meta.push;
      auto key = msg.meta.key;
      if (!HasRemoteInfo(msg, key, is_push, remote_id)) {
        MessageBuffer *msg_buf = PrepareNewMsgBuf(msg);
        StoreMsgBuf(msg_buf, msg);
        PrepareData(msg, msg_buf);
        trans->SendRendezvousBegin(msg, msg_buf);
        return total_len;
      }
    }

    auto addr_tuple =
        GetRemoteAndLocalInfo(msg.meta.key, msg.meta.push, remote_id);
    MessageBuffer *msg_buf = std::get<3>(addr_tuple);  // local message buffer

    // prepare new meta and data
    CHECK_EQ(msg_buf->inline_len, (size_t)meta_len);
    CHECK(msg_buf->inline_buf);
    msg_buf->data = msg.data;  // may not need this
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);

    PrintSendLog(msg, msg_buf, addr_tuple);

    // already know remote address, directly use RDMA-write
    if (msg.meta.push && msg.meta.request) {
      // worker, push request
      trans->SendPushRequest(msg, msg_buf, addr_tuple);
    } else if (msg.meta.push && !msg.meta.request) {
      // server, push response
      trans->SendPushResponse(msg, msg_buf, addr_tuple);
    } else if (!msg.meta.push && msg.meta.request) {
      // worker, pull request
      trans->SendPullRequest(msg, msg_buf, addr_tuple);
    } else if (!msg.meta.push && !msg.meta.request) {
      // server, pull response
      map_mu_.lock();
      auto temp_mr = mem_mr_.find(msg_buf->data[1].data());
      CHECK_NE(temp_mr, mem_mr_.end());
      map_mu_.unlock();
      trans->SendPullResponse(msg, msg_buf, addr_tuple, temp_mr->second->lkey);
    } else {
      CHECK(0) << "unexpected message type";
    }

    return total_len;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    std::tuple<Endpoint *, BufferContext *> notification;
    recv_buffers_.WaitAndPop(&notification);

    Endpoint *endpoint = std::get<Endpoint *>(notification);
    BufferContext *buffer_ctx = std::get<BufferContext *>(notification);

    msg->meta.recver = my_node_.id;
    msg->meta.sender = endpoint->node_id;

    // the second argument is actually deprecated,
    // we keep it as is in order to be compatible
    UnpackMeta(buffer_ctx->buffer, buffer_ctx->meta_len, &msg->meta);
    int meta_len = GetPackMetaLen(msg->meta);

    int total_len = 0;
    total_len += meta_len;

    auto trans = CHECK_NOTNULL(endpoint->GetTransport());

    PrintRecvLog(msg, buffer_ctx, meta_len);

    if (!IsValidPushpull(*msg)) {
      return total_len;
    }

    // valid data message
    if (msg->meta.push && msg->meta.request) {
      // push request
      total_len += trans->RecvPushRequest(msg, buffer_ctx, meta_len);
    } else if (!msg->meta.push && msg->meta.request) {
      // pull request
      total_len += trans->RecvPullRequest(msg, buffer_ctx, meta_len);
    } else if (msg->meta.push && !msg->meta.request) {
      // push response
      total_len += trans->RecvPushResponse(msg, buffer_ctx, meta_len);
    } else if (!msg->meta.push && !msg->meta.request) {
      // pull response
      total_len += trans->RecvPullResponse(msg, buffer_ctx, meta_len);
    } else {
      CHECK(0) << "unknown msg type";
    }

    return total_len;
  }

 private:
  void PrintSendLog(Message &msg, MessageBuffer *msg_buf,
                    RemoteTuple remote_tuple) {
    if (!enable_log_) return;
    std::lock_guard<std::mutex> lock(log_mu_);

    if (!IsValidPushpull(msg)) {
      LOG(INFO) << "Send Control Message" << std::flush;
    } else if (msg.meta.push && msg.meta.request) {
      // worker, push request
      LOG(INFO) << "Send Push Request: key=" << msg.meta.key
                << "\t timestamp=" << msg.meta.timestamp
                << "\t recver=" << msg.meta.recver
                << "\t tensor_len=" << msg_buf->mrs[0].second
                << "\t remote_idx=" << std::get<2>(remote_tuple)
                << "\t remote_addr=" << (void *)std::get<0>(remote_tuple)
                << std::flush;
    } else if (msg.meta.push && !msg.meta.request) {
      // server, push response
      LOG(INFO) << "Send Push Response: key=" << msg.meta.key
                << "\t timestamp=" << msg.meta.timestamp
                << "\t recver=" << msg.meta.recver
                << "\t remote_idx=" << std::get<2>(remote_tuple)
                << "\t remote_addr=" << (void *)std::get<0>(remote_tuple)
                << std::flush;
    } else if (!msg.meta.push && msg.meta.request) {
      // worker, pull request
      LOG(INFO) << "Send Pull Request: key=" << msg.meta.key
                << "\t timestamp=" << msg.meta.timestamp
                << "\t recver=" << msg.meta.recver
                << "\t remote_idx=" << std::get<2>(remote_tuple)
                << "\t remote_addr=" << (void *)std::get<0>(remote_tuple)
                << std::flush;
    } else if (!msg.meta.push && !msg.meta.request) {
      // server, pull response
      LOG(INFO) << "Send Pull Response: key=" << msg.meta.key
                << "\t timestamp=" << msg.meta.timestamp
                << "\t recver=" << msg.meta.recver
                << "\t tensor_len=" << msg.meta.val_len << "\t idx="
                << "none"
                << "\t remote_addr=" << (void *)msg.meta.addr << std::flush;
    }
  }

  void PrintRecvLog(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    if (!enable_log_) return;
    std::lock_guard<std::mutex> lock(log_mu_);

    if (!IsValidPushpull(*msg)) {
      LOG(INFO) << "Recv Control Message" << std::flush;
    } else if (msg->meta.push && msg->meta.request) {
      // push request
      LOG(INFO) << "Recv Push Request: key=" << msg->meta.key
                << "\t timestamp=" << msg->meta.timestamp
                << "\t sender=" << msg->meta.sender
                << "\t tensor_len=" << buffer_ctx->data_len[1] << std::flush;
    } else if (!msg->meta.push && msg->meta.request) {
      // pull request
      LOG(INFO) << "Recv Pull Request: key=" << msg->meta.key
                << "\t timestamp=" << msg->meta.timestamp
                << "\t sender=" << msg->meta.sender << std::flush;
    } else if (msg->meta.push && !msg->meta.request) {
      // push response
      LOG(INFO) << "Recv Push Response: key=" << msg->meta.key
                << "\t timestamp=" << msg->meta.timestamp
                << "\t sender=" << msg->meta.sender << std::flush;
    } else if (!msg->meta.push && !msg->meta.request) {
      // pull response
      LOG(INFO) << "Recv Pull Response: key=" << msg->meta.key
                << "\t timestamp=" << msg->meta.timestamp
                << "\t sender=" << msg->meta.sender
                << "\t tensor_len=" << msg->meta.val_len;
    }
  }

  bool HasRemoteInfo(Message &msg, uint64_t key, bool is_push, int recver) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    if (is_push && (push_addr_.find(key) != push_addr_.end()) &&
        (push_addr_[key].find(recver) != push_addr_[key].end())) {
      return true;
    }
    if (!is_push && (pull_addr_.find(key) != pull_addr_.end()) &&
        (pull_addr_[key].find(recver) != pull_addr_[key].end())) {
      return true;
    }

    return false;
  }

  void StoreMsgBuf(MessageBuffer *msg_buf, Message &msg) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    CHECK_EQ(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
    msgbuf_cache_[msg_buf] = msg;
  }

  Message *GetFirstMsg(MessageBuffer *msg_buf) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
    return &msgbuf_cache_[msg_buf];
  }

  void ReleaseFirstMsg(MessageBuffer *msg_buf) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
    msgbuf_cache_.erase(msg_buf);
  }

  void StoreRemoteAndLocalInfo(MessageBuffer *msg_buf, uint64_t remote_addr,
                               uint32_t rkey, uint32_t idx) {
    std::lock_guard<std::mutex> lk(addr_mu_);

    CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());

    auto &msg = msgbuf_cache_[msg_buf];

    auto key = msg.meta.key;
    auto is_push = msg.meta.push;
    auto recver = msg.meta.recver;

    auto t = std::make_tuple(remote_addr, rkey, idx, msg_buf);
    if (is_push) {
      push_addr_[key][recver] = t;
    } else {
      pull_addr_[key][recver] = t;
    }
  }

  RemoteTuple GetRemoteAndLocalInfo(uint64_t key, bool is_push, int recver) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    return (is_push ? push_addr_[key][recver] : pull_addr_[key][recver]);
  }

  MessageBuffer *PrepareNewMsgBuf(Message &msg) {
    MessageBuffer *msg_buf = new MessageBuffer();
    auto meta_len = GetPackMetaLen(msg.meta);
    msg_buf->inline_len = meta_len;
    msg_buf->inline_buf = mem_allocator_->Alloc(meta_len);
    msg_buf->data = msg.data;
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);
    return msg_buf;
  }

  void RegisterMemory(Message &msg) {
    size_t sa_cnt = 0;
    for (auto &sa : msg.data) {
      if (sa.size() == 0) continue;
      std::lock_guard<std::mutex> lock(map_mu_);
      if ((mem_mr_.find(sa.data()) == mem_mr_.end()) &&
          (sa_cnt == 1)) {  // only vals register memory
        struct ibv_mr *temp_mr;
        CHECK(temp_mr =
                  ibv_reg_mr(mem_allocator_->GetPD(), sa.data(), sa.size(),
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
            << "Failed to register the memory region: " << strerror(errno)
            << ", sa.size()=" << sa.size();
        mem_mr_[sa.data()] = temp_mr;
      }
      ++sa_cnt;
    }
    // register for tensor address of pull request
    if (IsValidPushpull(msg) && !msg.meta.push && msg.meta.request) {
      CHECK_GT(msg.meta.val_len, 0) << msg.meta.val_len;
      auto addr = reinterpret_cast<char *>(msg.meta.addr);
      std::lock_guard<std::mutex> lock(map_mu_);
      if (mem_mr_.find(addr) == mem_mr_.end()) {
        struct ibv_mr *temp_mr;
        CHECK(temp_mr =
                  ibv_reg_mr(mem_allocator_->GetPD(), addr, msg.meta.val_len,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
            << "Failed to register the memory region: " << strerror(errno);
        mem_mr_[addr] = temp_mr;
      }
    }
  }

  void PrepareData(Message &msg, MessageBuffer *msg_buf) {
    if (!(msg.meta.push && msg.meta.request)) return;  // only push request
    auto &sa = msg_buf->data[1];
    if (sa.size() == 0) return;
    std::lock_guard<std::mutex> lock(map_mu_);
    auto it = mem_mr_.find(sa.data());
    CHECK_NE(it, mem_mr_.end());
    MRPtr ptr(it->second, [](struct ibv_mr *mr) {});
    CHECK(ptr.get()) << strerror(errno);
    msg_buf->mrs.push_back(std::make_pair(std::move(ptr), sa.size()));
  }

  void AddMeta(Message &msg) {
    if (msg.meta.request) {
      msg.meta.key = DecodeKey(msg.data[0]);
    }
    if (!msg.meta.push && msg.meta.request) {
      // pull request
      std::lock_guard<std::mutex> lock(map_mu_);
      auto val_addr = reinterpret_cast<char *>(msg.meta.addr);
      msg.meta.option = mem_mr_[val_addr]->rkey;
    }
  }

  void InitContext(struct ibv_context *context) {
    context_ = context;
    CHECK(context_) << "ibv_context* empty";

    pd_ = ibv_alloc_pd(context_);
    CHECK(pd_) << "Failed to allocate protection domain";

    mem_allocator_.reset(new MemoryAllocator(pd_));

    comp_event_channel_ = ibv_create_comp_channel(context_);

    // TODO(clan): Replace the rough estimate here
    cq_ = ibv_create_cq(context_, kMaxConcurrentWorkRequest * 2, NULL,
                        comp_event_channel_, 0);

    CHECK(cq_) << "Failed to create completion queue";
    CHECK(!ibv_req_notify_cq(cq_, 0)) << "Failed to request CQ notification";
  }

  void ReleaseWorkRequestContext(WRContext *context, Endpoint *endpoint) {
    switch (context->type) {
      case kRendezvousStartContext:
        endpoint->free_start_ctx.Push(context);
        break;
      case kRendezvousReplyContext:
        endpoint->free_reply_ctx.Push(context);
        break;
      case kReceiveContext:
        endpoint->PostRecv(context);
        break;
      default:
        CHECK(0);
    }
  }

  void PollCQ() {
    // Pre-allocated work completions array used for polling
    struct ibv_wc wc[kMaxConcurrentWorkRequest];
    while (!should_stop_.load()) {
      int ne = ibv_poll_cq(cq_, kMaxConcurrentWorkRequest, wc);
      CHECK_GE(ne, 0);
      for (int i = 0; i < ne; ++i) {
        CHECK(wc[i].status == IBV_WC_SUCCESS)
            << "Failed status \n"
            << ibv_wc_status_str(wc[i].status) << " " << wc[i].status << " "
            << static_cast<uint64_t>(wc[i].wr_id) << " " << wc[i].vendor_err
            << " postoffice ptr: " << (void *)postoffice_;

        WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);
        Endpoint *endpoint =
            reinterpret_cast<Endpoint *>(context->private_data);

        // IBV_WC_RDMA_WRITE use msg_buf as the wr_id
        // so there won't be context and endpoint for this op

        switch (wc[i].opcode) {
          case IBV_WC_SEND: {
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          case IBV_WC_RDMA_WRITE: {
            // do nothing
          } break;
          case IBV_WC_RECV_RDMA_WITH_IMM: {
            uint32_t addr_idx = wc[i].imm_data;
            BufferContext *buf_ctx = addr_pool_.GetAddress(addr_idx);
            recv_buffers_.Push(std::make_tuple(endpoint, buf_ctx));
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          case IBV_WC_RECV: {
            CHECK(wc[i].wc_flags & IBV_WC_WITH_IMM);
            uint32_t imm = wc[i].imm_data;
            struct ibv_mr *mr = context->buffer;

            if (imm == kRendezvousStart) {
              RendezvousStart *req =
                  reinterpret_cast<RendezvousStart *>(mr->addr);
              auto trans = CHECK_NOTNULL(endpoint->GetTransport());
              trans->SendRendezvousReply(req, addr_pool_);

            } else if (imm == kRendezvousReply) {
              RendezvousReply *resp =
                  reinterpret_cast<RendezvousReply *>(mr->addr);
              uint64_t remote_addr = resp->addr;
              uint64_t origin_addr = resp->origin_addr;
              uint32_t rkey = resp->rkey;
              uint32_t idx = resp->idx;

              MessageBuffer *msg_buf =
                  reinterpret_cast<MessageBuffer *>(origin_addr);

              // Before RDMA write, store the remote info so that
              // subsequent write does not need repeated rendezvous
              StoreRemoteAndLocalInfo(msg_buf, remote_addr, rkey, idx);

              Message *msg = GetFirstMsg(msg_buf);

              auto addr_tuple = GetRemoteAndLocalInfo(
                  msg->meta.key, msg->meta.push, msg->meta.recver);

              PrintSendLog(*msg, msg_buf, addr_tuple);

              auto trans = CHECK_NOTNULL(endpoint->GetTransport());
              if (!IsValidPushpull(*msg)) {
                // control message
                trans->RDMAWriteWithImm(msg_buf, remote_addr, rkey, idx);
              } else if (msg->meta.push && msg->meta.request) {
                // worker, push request
                trans->SendPushRequest(*msg, msg_buf, addr_tuple);
              } else if (msg->meta.push && !msg->meta.request) {
                // server, push response
                trans->SendPushResponse(*msg, msg_buf, addr_tuple);
              } else if (!msg->meta.push && msg->meta.request) {
                // worker, pull request
                trans->SendPullRequest(*msg, msg_buf, addr_tuple);
              } else if (!msg->meta.push && !msg->meta.request) {
                // server, pull response
                map_mu_.lock();
                auto temp_mr = mem_mr_.find(msg_buf->data[1].data());
                CHECK_NE(temp_mr, mem_mr_.end());
                map_mu_.unlock();
                trans->SendPullResponse(*msg, msg_buf, addr_tuple,
                                        temp_mr->second->lkey);
              }

              // release the msg_buf from msgbuf_cache_
              ReleaseFirstMsg(msg_buf);

            } else {
              CHECK(0);
            }
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          default:
            CHECK(0) << "Unexpected opcode: " << wc[i].opcode;
        }
      }
    }
  }

  void PollEvents() {
    int flags = fcntl(event_channel_->fd, F_GETFL);
    int rc = fcntl(event_channel_->fd, F_SETFL, flags | O_NONBLOCK);
    CHECK_GE(rc, 0);
    int error_flags = POLLERR | POLLHUP | POLLNVAL;

    while (!should_stop_.load()) {
      struct pollfd pfd = {
          .fd = event_channel_->fd, .events = POLLIN, .revents = 0};
      int ret = poll(&pfd, 1, 10);

      CHECK_GE(ret, 0) << strerror(errno);
      CHECK_EQ(pfd.revents & error_flags, 0);

      if (!(pfd.revents & POLLIN)) {
        continue;
      }

      struct rdma_cm_event *event;
      CHECK_EQ(rdma_get_cm_event(event_channel_, &event), 0);
      // TODO(clan): Reorder the list according to the event frequency
      switch (event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
          OnConnectRequest(event);
          break;
        case RDMA_CM_EVENT_ADDR_RESOLVED:
          OnAddrResolved(event);
          break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
          OnRouteResolved(event);
          break;
        case RDMA_CM_EVENT_ESTABLISHED:
          OnConnected(event);
          break;
        case RDMA_CM_EVENT_DISCONNECTED:
          OnDisconnected(event);
          break;
        case RDMA_CM_EVENT_REJECTED:
          OnRejected(event);
          break;
        default:
          CHECK(0) << "OnEvent: unknown event " << event->event << " ("
                   << rdma_event_str(event->event) << ")";
      }
      rdma_ack_cm_event(event);
    }
  }

  void OnRejected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);

    endpoints_mu_.lock();
    auto it = endpoints_.find(endpoint->node_id);
    CHECK(it != endpoints_.end()) << "Connection not ready.";
    endpoints_mu_.unlock();

    CHECK_EQ(endpoint->status, Endpoint::CONNECTING);
    CHECK_EQ(endpoint->cm_id, id);

    PS_VLOG(1) << my_node_.id << " to " << endpoint->node_id
               << " connection rejected, retrying...";
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::REJECTED;
    }
    endpoint->cv.notify_all();
  }

  void OnConnectRequest(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK_NOTNULL(id);

    CHECK_LE(sizeof(RequestContext), event->param.conn.private_data_len)
        << "RequestContext size mismatch. Actual: "
        << (size_t)event->param.conn.private_data_len
        << ", Expected: " << sizeof(RequestContext);
    CHECK_NOTNULL(event->param.conn.private_data);

    const RequestContext *remote_ctx = reinterpret_cast<const RequestContext *>(
        event->param.conn.private_data);

    const auto r = incoming_.emplace(std::make_unique<Endpoint>());
    Endpoint *endpoint = r.first->get();
    endpoint->SetNodeID(remote_ctx->node);
    endpoint->cm_id = id;
    id->context = endpoint;

    if (context_ == nullptr) {
      InitContext(id->verbs);
    }

    endpoint->Init(cq_, pd_);

    bool is_local_node =
        disable_ipc_
            ? false
            : (std::string(remote_ctx->hostname) == my_node_.hostname ? true
                                                                      : false);
    {
      std::lock_guard<std::mutex> lk(local_mu_);
      is_local_[remote_ctx->node] = is_local_node;
    }
    LOG(INFO) << my_node_.id << " OnConnect to " << remote_ctx->node
              << " with Transport=" << (is_local_node ? "IPC" : "RDMA");

    std::shared_ptr<Transport> t =
        is_local_node ? std::make_shared<IPCTransport>(
                            endpoint, mem_allocator_.get(), postoffice_)
                      : std::make_shared<RDMATransport>(
                            endpoint, mem_allocator_.get(), postoffice_);
    endpoint->SetTransport(t);

    RequestContext ctx;
    ctx.node = static_cast<uint32_t>(my_node_.id);
    ctx.port = static_cast<uint16_t>(my_node_.port);
    snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    cm_params.retry_count = 7;
    cm_params.rnr_retry_count = 7;
    cm_params.private_data = &ctx;
    cm_params.private_data_len = sizeof(RequestContext);

    CHECK_EQ(rdma_accept(id, &cm_params), 0)
        << "Accept RDMA connection failed: " << strerror(errno);
  }

  // Resolve a route after address is resolved
  void OnAddrResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK_EQ(rdma_resolve_route(id, kTimeoutms), 0)
        << "Resolve RDMA route failed";
  }

  // Make a connection after route is resolved
  void OnRouteResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);

    if (context_ == nullptr) {
      InitContext(id->verbs);
    }

    endpoint->Init(cq_, pd_);

    RequestContext ctx;
    ctx.node = static_cast<uint32_t>(my_node_.id);
    ctx.port = static_cast<uint16_t>(my_node_.port);
    snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    cm_params.retry_count = 7;
    cm_params.rnr_retry_count = 7;
    cm_params.private_data = &ctx;
    cm_params.private_data_len = sizeof(RequestContext);

    CHECK_EQ(rdma_connect(id, &cm_params), 0)
        << "RDMA connect failed" << strerror(errno);
  }

  void OnConnected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK(id) << "rdma_cm_id not found.";
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    CHECK(endpoint) << "Endpoint not found.";

    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&RDMAVan::PollCQ, this));
    }

    CHECK_EQ(endpoint->cm_id, id);
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::CONNECTED;
    }
    endpoint->cv.notify_all();
    if (endpoint->node_id != my_node_.id) {
      PS_VLOG(1) << my_node_.id << " OnConnected to " << endpoint->node_id;
    }
  }

  void OnDisconnected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::IDLE;
    }
    endpoint->cv.notify_all();
    LOG(INFO) << my_node_.id << " OnDisconnected from " << endpoint->node_id;
  }

  AddressPool<BufferContext> addr_pool_;
  std::unique_ptr<MemoryAllocator> mem_allocator_;

  std::unique_ptr<RDMATransport> rdma_trans_;
  std::unique_ptr<IPCTransport> ipc_trans_;

  struct rdma_cm_id *listener_ = nullptr;
  std::atomic<bool> should_stop_;

  std::mutex endpoints_mu_;
  std::unordered_map<int, std::unique_ptr<Endpoint>> endpoints_;
  std::unordered_set<std::unique_ptr<Endpoint>> incoming_;

  struct rdma_event_channel *event_channel_ = nullptr;
  struct ibv_context *context_ = nullptr;

  // ibverbs protection domain
  struct ibv_pd *pd_ = nullptr;
  // Completion event channel, to wait for work completions
  struct ibv_comp_channel *comp_event_channel_ = nullptr;
  // Completion queue, to poll on work completions
  struct ibv_cq *cq_ = nullptr;
  // cq thread
  std::unique_ptr<std::thread> cq_polling_thread_;
  // event thread
  std::unique_ptr<std::thread> cm_event_polling_thread_;
  // Recv buffer queue
  ThreadsafeQueue<std::tuple<Endpoint *, BufferContext *>> recv_buffers_;

  // local IPC related
  bool disable_ipc_ = false;
  std::mutex local_mu_;
  std::unordered_map<int, bool> is_local_;

  std::mutex addr_mu_;
  // <key, recver>, (<remote_addr, rkey, idx, local_addr>)
  std::unordered_map<uint64_t, RemoteAndLocalAddress> push_addr_;
  std::unordered_map<uint64_t, RemoteAndLocalAddress> pull_addr_;
  std::unordered_map<MessageBuffer *, Message> msgbuf_cache_;  // msg_buf, msg

  std::mutex map_mu_;
  std::unordered_map<char *, struct ibv_mr *>
      mem_mr_;  // (memory address, ibv_mr)

  // logging
  bool enable_log_;
  std::mutex log_mu_;

  int kMaxConcurrentWorkRequest = 4224;  // 128 + 2048 * 2

};  // class RDMAVan

};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_
