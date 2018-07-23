/**
 *  Copyright (c) 2018 by Chang Lan
 *  Copyright (c) 2017 by Junxue Zhang, Jingrong Chen
 */
#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_

#ifdef DMLC_USE_RDMA

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#include <rdma/rdma_cma.h>

#include <algorithm>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"

namespace ps {

static const int kRxDepth = 128;
static const int kTxDepth = 128;
static const int kSGEntry = 4;
static const int kTimeoutms = 1000;
static const int kRdmaListenBacklog = 128;
static const int kMaxConcurrentWorkRequest = kRxDepth + kTxDepth;
static const int kMaxHostnameLength = 16;
static const int kMaxDataFields = 4;
static const size_t kAlignment = 8;

template <typename T>
static inline T align_floor(T v, T align) {
  return v - (v % align);
}

template <typename T>
static inline T align_ceil(T v, T align) {
  return align_floor(v + align - 1, align);
}

class SimpleMempool {
 public:
  explicit SimpleMempool(struct ibv_pd *pd, size_t size = 0x80000000) {
    char *p = reinterpret_cast<char *>(aligned_alloc(kAlignment, size));
    CHECK(p);
    CHECK(mr = ibv_reg_mr(pd, p, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
    free_list.emplace(size, p);
  }

  ~SimpleMempool() {
    free(mr->addr);
    CHECK_EQ(ibv_dereg_mr(mr), 0);
  }

  char *Alloc(size_t size) {
    std::lock_guard<std::mutex> lk(mu_);

    if (size == 0) {
      return nullptr;
    }

    size_t proper_size = align_ceil(size, kAlignment);

    auto it = free_list.lower_bound(proper_size);
    CHECK_NE(free_list.end(), it);

    char *ret = it->second;

    CHECK_GE(it->first, proper_size);

    size_t space_left = it->first - proper_size;

    used_list.emplace(ret, proper_size);
    free_list.erase(it);

    if (space_left) {
      free_list.emplace(space_left, ret + proper_size);
    }

    return ret;
  }

  void Free(char *addr) {
    std::lock_guard<std::mutex> lk(mu_);

    if (!addr) {
      return;
    }

    auto it = used_list.find(addr);
    CHECK_NE(used_list.end(), it);

    size_t size = it->second;
    used_list.erase(it);
    free_list.emplace(size, addr);
  }

  uint32_t LocalKey() const { return mr->lkey; }
  uint32_t RemoteKey() const { return mr->rkey; }

 private:
  std::mutex mu_;
  std::multimap<size_t, char *> free_list;
  std::unordered_map<char *, size_t> used_list;
  struct ibv_mr *mr;
};

enum MessageTypes : uint32_t {
  kRendezvousStart,
  kRendezvousReply,
};

struct RendezvousStart {
  uint64_t meta_len;
  uint64_t data_num;
  uint64_t data_len[kMaxDataFields];
  uint64_t origin_addr;
};

struct RendezvousReply {
  uint64_t addr;
  uint64_t origin_addr;
  uint32_t rkey;
  uint32_t idx;
};

struct WRContext {
  struct ibv_mr *buffer;
  void *private_data;
};

struct BufferContext {
  char *buffer;
  size_t meta_len;
  size_t data_num;
  size_t data_len[kMaxDataFields];
};

struct LocalBufferContext {
  size_t meta_len;
  char *meta_buf;
  std::vector<SArray<char>> data;
};

struct MessageBuffer {
  size_t meta_len;
  size_t data_len;
  char *meta_buf;
  char *data_buf;
};

struct RequestContext {
  uint32_t node;
  uint16_t port;
  char hostname[kMaxHostnameLength];
};

static_assert(std::is_pod<RendezvousStart>::value, "RendezvousStart must be a POD type.");
static_assert(std::is_pod<RendezvousReply>::value, "RendezvousReply must be a POD type.");
static_assert(std::is_pod<RequestContext>::value, "RequestContext must be a POD type.");

static const size_t kMempoolChunkSize = std::max(sizeof(RendezvousStart), sizeof(RendezvousReply));

template <typename T>
class AddressPool {
 public:
  AddressPool() {
    std::lock_guard<std::mutex> lk(mu_);
    for (int i = 0; i < kMaxEntries; i++) {
      indices_.push(i);
      table_[i] = nullptr;
    }
  }

  T *GetAddressAndRelease(uint32_t index) {
    std::lock_guard<std::mutex> lk(mu_);
    T *ptr = table_[index];
    CHECK(ptr);
    indices_.push(index);
    table_[index] = nullptr;
    return ptr;
  }

  uint32_t StoreAddress(T *ptr) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(ptr);
    uint32_t idx = indices_.front();
    indices_.pop();
    CHECK_EQ(table_[idx], nullptr);
    table_[idx] = ptr;
    return idx;
  }

 private:
  static const int kMaxEntries = 512;

  std::mutex mu_;
  std::queue<uint32_t> indices_;
  T *table_[kMaxEntries];
};

struct RDMAEndpoint {
  std::atomic<bool> connected;
  bool active;
  int node_id;
  std::condition_variable cv;
  std::mutex connect_mu;
  struct rdma_cm_id *cm_id;
  WRContext rx_wr_ctx[kRxDepth];
  WRContext tx_wr_ctx[kTxDepth];
  ThreadsafeQueue<WRContext *> free_tx_wr;

  explicit RDMAEndpoint(struct rdma_cm_id *id)
      : connected(false), active(false), node_id(Node::kEmpty), cm_id(id), rx_wr_ctx() {}

  ~RDMAEndpoint() {
    for (int i = 0; i < kRxDepth; ++i) {
      if (!(rx_wr_ctx[i].buffer)) {
        continue;
      }
      free(rx_wr_ctx[i].buffer->addr);
      CHECK_EQ(ibv_dereg_mr(rx_wr_ctx[i].buffer), 0);
    }

    if (cm_id->qp) {
      rdma_destroy_qp(cm_id);
    }

    CHECK_EQ(rdma_destroy_id(cm_id), 0);
  }

  void Connect(const Node &remote) {
    std::unique_lock<std::mutex> lk(connect_mu);

    active = true;
    struct addrinfo *addr;
    CHECK(getaddrinfo(remote.hostname.c_str(), std::to_string(remote.port).c_str(), nullptr,
                      &addr) == 0)
        << "Set address and port for connection failed";
    CHECK(rdma_resolve_addr(cm_id, nullptr, addr->ai_addr, kTimeoutms) == 0)
        << "Resolve RDMA address failed with errno: " << errno;
    freeaddrinfo(addr);

    cv.wait(lk, [this] { return connected.load(); });
  }

  void SetNodeID(int id) { node_id = id; }

  void Init(struct ibv_cq *cq, struct ibv_pd *pd) {
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_init_attr));
    attr.send_cq = cq;
    attr.recv_cq = cq;
    attr.cap.max_send_wr = kTxDepth;
    attr.cap.max_recv_wr = kRxDepth;
    attr.cap.max_send_sge = kSGEntry;
    attr.cap.max_recv_sge = kSGEntry;
    attr.qp_type = IBV_QPT_RC;
    attr.sq_sig_all = 0;

    CHECK_EQ(rdma_create_qp(cm_id, pd, &attr), 0) << "Create RDMA queue pair failed";

    for (size_t i = 0; i < kTxDepth; ++i) {
      void *buf = aligned_alloc(kAlignment, kMempoolChunkSize);
      CHECK(buf);
      struct ibv_mr *mr = ibv_reg_mr(pd, buf, kMempoolChunkSize, 0);
      CHECK(mr);

      tx_wr_ctx[i].buffer = mr;
      tx_wr_ctx[i].private_data = this;

      free_tx_wr.Push(&tx_wr_ctx[i]);
    }

    for (size_t i = 0; i < kRxDepth; ++i) {
      void *buf = aligned_alloc(kAlignment, kMempoolChunkSize);
      CHECK(buf);
      struct ibv_mr *mr = ibv_reg_mr(pd, buf, kMempoolChunkSize, IBV_ACCESS_LOCAL_WRITE);
      CHECK(mr);

      rx_wr_ctx[i].buffer = mr;
      rx_wr_ctx[i].private_data = this;

      PostRecv(&rx_wr_ctx[i]);
    }
  }

  void PostRecv(WRContext *ctx) {
    struct ibv_recv_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));

    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(ctx->buffer->addr);
    sge.length = kMempoolChunkSize;
    sge.lkey = ctx->buffer->lkey;

    wr.wr_id = reinterpret_cast<uint64_t>(ctx);
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    CHECK_EQ(ibv_post_recv(cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_send failed.";
  }
};

static RDMAEndpoint *const kLocalTransfer = nullptr;

class RDMAVan : public Van {
 public:
  RDMAVan() {}
  ~RDMAVan() {}

 protected:
  void Start(int customer_id) override {
    start_mu_.lock();
    should_stop_ = false;

    if (event_channel_ == nullptr) {
      event_channel_ = rdma_create_event_channel();
      CHECK(event_channel_) << "Create RDMA event channel failed";

      cm_event_polling_thread_.reset(new std::thread(&RDMAVan::PollEvents, this));
    }

    start_mu_.unlock();
    Van::Start(customer_id);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();

    should_stop_ = true;

    CHECK(should_stop_);

    cq_polling_thread_->join();
    cq_polling_thread_.reset();

    for (auto &i : endpoints_) {
      i.second.reset();
    }

    mempool_.reset();

    cm_event_polling_thread_->join();
    cm_event_polling_thread_.reset();

    CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
    CHECK(!ibv_destroy_comp_channel(comp_event_channel_)) << "Failed to destroy channel";
    CHECK(!ibv_dealloc_pd(pd_)) << "Failed to deallocate PD";

    rdma_destroy_id(listener_);
    rdma_destroy_event_channel(event_channel_);
  }

  int Bind(const Node &node, int max_retry) override {
    CHECK(rdma_create_id(event_channel_, &listener_, nullptr, RDMA_PS_TCP) == 0)
        << "Create RDMA connection identifier failed";

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    int port = node.port;

    for (int i = 0; i < max_retry + 1; ++i) {
      addr.sin_port = htons(port);
      if (rdma_bind_addr(listener_, reinterpret_cast<struct sockaddr *>(&addr)) == 0) {
        break;
      }

      if (i == max_retry) {
        port = -1;
      } else {
        port += 1;
      }
    }

    CHECK(rdma_listen(listener_, kRdmaListenBacklog) == 0)
        << "Listen RDMA connection failed: " << strerror(errno);
    return port;
  }

  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }

    // Optimization
    if ((node.role == my_node_.role) && (node.id == my_node_.id)) {
      LOG(INFO) << "Node: " << node.id << " is myself. No need to connect.";
      return;
    }

    if (node.role != Node::SCHEDULER && my_node_.role != Node::SCHEDULER &&
        node.id != Node::kEmpty && my_node_.id != Node::kEmpty) {
      if (my_node_.id < node.id) {
        LOG(INFO) << my_node_.id << " < " << node.id << ", Skipping";
        while (true) {
          auto it = endpoints_.find(node.id);
          if (it != endpoints_.end() && it->second && it->second->connected) {
            break;
          }
        }
        return;
      }
    }

    std::string node_host_ip = node.hostname + ":" + std::to_string(node.port);

    if (node.id != Node::kEmpty) {
      // First check if there is any unresolved node ID.
      if (temp_endpoints_.find(node_host_ip) != temp_endpoints_.end()) {
        auto it = temp_endpoints_.find(node_host_ip);
        it->second->SetNodeID(node.id);
        endpoints_[node.id] = std::move(it->second);
        temp_endpoints_.erase(it);

      } else if (endpoints_.find(node.id) == endpoints_.end()) {
        struct rdma_cm_id *id = nullptr;
        CHECK(rdma_create_id(event_channel_, &id, nullptr, RDMA_PS_TCP) == 0)
            << "Create RDMA connection identifier failed";

        RDMAEndpoint *endpoint;

        endpoints_[node.id] = std::make_unique<RDMAEndpoint>(id);
        endpoint = endpoints_[node.id].get();

        id->context = endpoint;

        endpoint->SetNodeID(node.id);
        endpoint->Connect(node);
      }
    }
  }

  int SendMsg(const Message &msg) override {
    int remote_id = msg.meta.recver;
    CHECK_NE(remote_id, Meta::kEmpty);

    PBMeta meta;
    PackMetaPB(msg.meta, &meta);

    if (remote_id == my_node_.id) {
      LocalBufferContext *buf_ctx = new LocalBufferContext();
      buf_ctx->meta_len = meta.ByteSize();
      buf_ctx->meta_buf = mempool_->Alloc(buf_ctx->meta_len);
      meta.SerializeToArray(buf_ctx->meta_buf, buf_ctx->meta_len);
      buf_ctx->data = msg.data;
      recv_buffers_.Push(
          std::make_tuple(kLocalTransfer, reinterpret_cast<BufferContext *>(buf_ctx)));
      return buf_ctx->meta_len + msg.meta.data_size;
    }

    CHECK_NE(endpoints_.find(remote_id), endpoints_.end());
    RDMAEndpoint *endpoint = endpoints_[remote_id].get();

    MessageBuffer *msg_buf =
        reinterpret_cast<MessageBuffer *>(mempool_->Alloc(sizeof(MessageBuffer)));

    CHECK(meta.ByteSize());
    msg_buf->meta_len = meta.ByteSize();
    msg_buf->data_len = msg.meta.data_size;
    msg_buf->meta_buf = mempool_->Alloc(msg_buf->meta_len);
    msg_buf->data_buf = mempool_->Alloc(msg_buf->data_len);
    meta.SerializeToArray(msg_buf->meta_buf, msg_buf->meta_len);

    char *cur = reinterpret_cast<char *>(msg_buf->data_buf);
    for (size_t i = 0; i < msg.data.size(); ++i) {
      size_t size = msg.data[i].size();
      memcpy(cur, msg.data[i].data(), size);
      cur += size;
    }

    WRContext *context = nullptr;
    endpoint->free_tx_wr.WaitAndPop(&context);

    RendezvousStart *req = reinterpret_cast<RendezvousStart *>(context->buffer->addr);
    req->meta_len = meta.ByteSize();

    for (size_t i = 0; i < msg.data.size(); ++i) {
      req->data_len[i] = msg.data[i].size();
    }
    req->data_num = msg.data.size();
    req->origin_addr = reinterpret_cast<uint64_t>(msg_buf);

    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(req);
    sge.length = sizeof(RendezvousStart);
    sge.lkey = context->buffer->lkey;

    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = reinterpret_cast<uint64_t>(context);
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.next = nullptr;

    wr.imm_data = kRendezvousStart;

    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0) << strerror(errno);

    return meta.ByteSize() + msg.meta.data_size;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    std::tuple<RDMAEndpoint *, BufferContext *> notification;
    recv_buffers_.WaitAndPop(&notification);

    RDMAEndpoint *endpoint = std::get<RDMAEndpoint *>(notification);

    if (endpoint == kLocalTransfer) {
      LocalBufferContext *buffer_ctx =
          reinterpret_cast<LocalBufferContext *>(std::get<BufferContext *>(notification));
      msg->meta.recver = my_node_.id;
      msg->meta.sender = my_node_.id;
      UnpackMeta(buffer_ctx->meta_buf, buffer_ctx->meta_len, &msg->meta);
      msg->data = buffer_ctx->data;
      int total_len = buffer_ctx->meta_len + msg->meta.data_size;
      mempool_->Free(buffer_ctx->meta_buf);
      delete buffer_ctx;
      return total_len;
    }

    BufferContext *buffer_ctx = std::get<BufferContext *>(notification);

    int total_len = 0;

    msg->meta.recver = my_node_.id;
    msg->meta.sender = endpoint->node_id;

    const char *cur = reinterpret_cast<char *>(buffer_ctx->buffer);
    UnpackMeta(cur, buffer_ctx->meta_len, &msg->meta);
    total_len += buffer_ctx->meta_len;
    uint64_t data_num = buffer_ctx->data_num;
    cur += buffer_ctx->meta_len;

    for (size_t i = 0; i < data_num; i++) {
      uint32_t len = buffer_ctx->data_len[i];
      SArray<char> data;
      data.CopyFrom(cur, len);
      msg->data.push_back(data);
      cur += len;
      total_len += len;
    }

    mempool_->Free(buffer_ctx->buffer);
    mempool_->Free(reinterpret_cast<char *>(buffer_ctx));

    return total_len;
  }

 private:
  void InitContext(struct ibv_context *context) {
    context_ = context;
    CHECK(context_) << "ibv_context* empty";

    pd_ = ibv_alloc_pd(context_);
    CHECK(pd_) << "Failed to allocate protection domain";

    mempool_.reset(new SimpleMempool(pd_));

    comp_event_channel_ = ibv_create_comp_channel(context_);

    // TODO(clan): Replace the rough estimate here
    cq_ = ibv_create_cq(context_, kMaxConcurrentWorkRequest * 2, NULL, comp_event_channel_, 0);

    CHECK(cq_) << "Failed to create completion queue";
    CHECK(!ibv_req_notify_cq(cq_, 0)) << "Failed to request CQ notification";

    cq_polling_thread_.reset(new std::thread(&RDMAVan::PollCQ, this));
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
            << static_cast<int>(wc[i].wr_id) << " " << wc[i].vendor_err;

        WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);
        RDMAEndpoint *endpoint = reinterpret_cast<RDMAEndpoint *>(context->private_data);

        switch (wc[i].opcode) {
          case IBV_WC_SEND: {
            // LOG(INFO) << "opcode: IBV_WC_SEND";
            endpoint->free_tx_wr.Push(context);
          } break;
          case IBV_WC_RDMA_WRITE: {
            // LOG(INFO) << "opcode: IBV_WC_RDMA_WRITE";
            // Note: This is not a struct ibv_mr*
            MessageBuffer *msg_buf = *reinterpret_cast<MessageBuffer **>(context->buffer->addr);
            mempool_->Free(msg_buf->meta_buf);
            mempool_->Free(msg_buf->data_buf);
            mempool_->Free(reinterpret_cast<char *>(msg_buf));
            endpoint->free_tx_wr.Push(context);
          } break;
          case IBV_WC_RECV_RDMA_WITH_IMM: {
            // LOG(INFO) << "opcode: IBV_WC_RECV_RDMA_WITH_IMM";
            CHECK(endpoint);
            endpoint->PostRecv(context);

            uint32_t addr_idx = wc[i].imm_data;
            BufferContext *buf_ctx = addr_pool_.GetAddressAndRelease(addr_idx);
            recv_buffers_.Push(std::make_tuple(endpoint, buf_ctx));
          } break;
          case IBV_WC_RECV: {
            CHECK(wc[i].wc_flags & IBV_WC_WITH_IMM);
            uint32_t imm = wc[i].imm_data;
            struct ibv_mr *mr = context->buffer;

            if (imm == kRendezvousStart) {
              // LOG(INFO) << "opcode: IBV_WC_RECV kRendezvousStart";
              RendezvousStart *req = reinterpret_cast<RendezvousStart *>(mr->addr);

              BufferContext *buf_ctx =
                  reinterpret_cast<BufferContext *>(mempool_->Alloc(sizeof(BufferContext)));

              uint64_t len = req->meta_len;
              buf_ctx->meta_len = len;
              buf_ctx->data_num = req->data_num;
              for (size_t i = 0; i < req->data_num; ++i) {
                buf_ctx->data_len[i] = req->data_len[i];
                len += req->data_len[i];
              }

              char *buffer = mempool_->Alloc(len);
              CHECK(buffer) << "Alloc for " << len << " bytes, data_num: " << req->data_num;

              buf_ctx->buffer = buffer;

              uint64_t origin_addr = req->origin_addr;

              WRContext *context = nullptr;
              endpoint->free_tx_wr.WaitAndPop(&context);
              RendezvousReply *resp = reinterpret_cast<RendezvousReply *>(context->buffer->addr);

              resp->addr = reinterpret_cast<uint64_t>(buffer);
              resp->rkey = mempool_->RemoteKey();
              resp->origin_addr = origin_addr;
              resp->idx = addr_pool_.StoreAddress(buf_ctx);

              struct ibv_sge sge;
              sge.addr = reinterpret_cast<uint64_t>(resp);
              sge.length = sizeof(RendezvousReply);
              sge.lkey = context->buffer->lkey;

              struct ibv_send_wr wr, *bad_wr = nullptr;
              memset(&wr, 0, sizeof(wr));

              wr.wr_id = reinterpret_cast<uint64_t>(context);
              wr.opcode = IBV_WR_SEND_WITH_IMM;
              wr.next = nullptr;

              wr.imm_data = kRendezvousReply;

              wr.send_flags = IBV_SEND_SIGNALED;
              wr.sg_list = &sge;
              wr.num_sge = 1;

              CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
                  << "ibv_post_send failed.";

            } else if (imm == kRendezvousReply) {
              // LOG(INFO) << "opcode: IBV_WC_RECV kRendezvousReply";
              RendezvousReply *resp = reinterpret_cast<RendezvousReply *>(mr->addr);
              uint64_t remote_addr = resp->addr;
              uint64_t origin_addr = resp->origin_addr;
              uint32_t rkey = resp->rkey;
              uint32_t idx = resp->idx;

              MessageBuffer *msg_buf = reinterpret_cast<MessageBuffer *>(origin_addr);

              struct ibv_sge sge[2];

              sge[0].addr = reinterpret_cast<uint64_t>(msg_buf->meta_buf);
              sge[0].length = msg_buf->meta_len;
              sge[0].lkey = mempool_->LocalKey();

              sge[1].addr = reinterpret_cast<uint64_t>(msg_buf->data_buf);
              sge[1].length = msg_buf->data_len;
              sge[1].lkey = mempool_->LocalKey();

              WRContext *context = nullptr;
              endpoint->free_tx_wr.WaitAndPop(&context);
              MessageBuffer **tmp = reinterpret_cast<MessageBuffer **>(context->buffer->addr);
              *tmp = msg_buf;  // write the addr of msg_buf into the mr buffer

              struct ibv_send_wr wr, *bad_wr = nullptr;
              memset(&wr, 0, sizeof(wr));

              wr.wr_id = reinterpret_cast<uint64_t>(context);
              wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
              wr.next = nullptr;

              wr.imm_data = idx;

              wr.send_flags = IBV_SEND_SIGNALED;
              wr.sg_list = sge;
              wr.num_sge = 1 + (msg_buf->data_len > 0 ? 1 : 0);

              wr.wr.rdma.remote_addr = remote_addr;
              wr.wr.rdma.rkey = rkey;

              CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
                  << "ibv_post_send failed.";

            } else {
              CHECK(0);
            }
            endpoint->PostRecv(context);
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
      struct pollfd pfd = {.fd = event_channel_->fd, .events = POLLIN, .revents = 0};
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
        default:
          CHECK(0) << "OnEvent: unknown event " << event->event << " ("
                   << rdma_event_str(event->event) << ")";
      }
      rdma_ack_cm_event(event);
    }
  }

  void OnRejected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    RDMAEndpoint *endpoint = reinterpret_cast<RDMAEndpoint *>(id->context);

    auto it = endpoints_.find(endpoint->node_id);
    CHECK(it != endpoints_.end()) << "Connection not ready.";

    endpoint->connected = true;

    endpoint->cv.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_ERR;
    CHECK_EQ(ibv_modify_qp(endpoint->cm_id->qp, &attr, 0), 0);
  }

  void OnConnectRequest(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;

    const RequestContext *remote_ctx =
        reinterpret_cast<const RequestContext *>(event->param.conn.private_data);

    auto it = endpoints_.find(remote_ctx->node);
    CHECK(it == endpoints_.end())
        << "Unable to resolve conflict. Maybe two nodes connect each other simultaneously.";

    RDMAEndpoint *endpoint = nullptr;
    if (remote_ctx->node == Node::kEmpty) {
      std::string node_host_ip =
          std::string(remote_ctx->hostname) + ":" + std::to_string(remote_ctx->port);
      LOG(INFO) << "Unassigned Node ID. Host: " << node_host_ip;
      temp_endpoints_[node_host_ip] = std::make_unique<RDMAEndpoint>(id);
      endpoint = temp_endpoints_[node_host_ip].get();
    } else {
      endpoints_[remote_ctx->node] = std::make_unique<RDMAEndpoint>(id);
      endpoint = endpoints_[remote_ctx->node].get();
      endpoint->SetNodeID(remote_ctx->node);
    }

    CHECK(!endpoint->active) << "Should only happen on the passive side!";

    CHECK_LE(sizeof(RequestContext), event->param.conn.private_data_len)
        << "RequestContext size mismatch. Actual: " << (size_t)event->param.conn.private_data_len
        << ", Expected: " << sizeof(RequestContext);

    id->context = endpoint;

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

    CHECK_EQ(rdma_accept(id, &cm_params), 0) << "Accept RDMA connection failed";
  }

  // Resolve a route after address is resolved
  void OnAddrResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    RDMAEndpoint *endpoint = reinterpret_cast<RDMAEndpoint *>(id->context);
    CHECK(endpoint->active) << "Should only happen on the active side!";
    CHECK_EQ(rdma_resolve_route(id, kTimeoutms), 0) << "Resolve RDMA route failed";
  }

  // Make a connection after route is resolved
  void OnRouteResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    RDMAEndpoint *endpoint = reinterpret_cast<RDMAEndpoint *>(id->context);
    CHECK(endpoint->active) << "Should only happen on the active side!";

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

    CHECK_EQ(rdma_connect(id, &cm_params), 0) << "RDMA connect failed";
  }

  void OnConnected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK(id) << "rdma_cm_id not found.";

    RDMAEndpoint *endpoint = reinterpret_cast<RDMAEndpoint *>(id->context);

    CHECK(endpoint) << "RDMAEndpoint not found.";
    CHECK_EQ(endpoint->cm_id, id);

    if (!endpoint->active) {
      endpoint->connected = true;
      return;
    }

    const RequestContext *remote_ctx =
        reinterpret_cast<const RequestContext *>(event->param.conn.private_data);
    CHECK(remote_ctx) << "remote_ctx is NULL";

    int remote_node_id = remote_ctx->node;
    CHECK(remote_node_id == Node::kEmpty || remote_node_id == endpoint->node_id)
        << "remote_ctx->node mismatch.";

    if (remote_node_id == Node::kEmpty) {
      LOG(WARNING) << "Node " << endpoint->node_id << " does not know its own identity";
    }

    CHECK_LE(sizeof(RequestContext), event->param.conn.private_data_len)
        << "RequestContext size mismatch. Actual: " << (size_t)event->param.conn.private_data_len
        << ", Expected: " << sizeof(RequestContext);

    CHECK_NE(endpoints_.find(endpoint->node_id), endpoints_.end())
        << "Endpoint " << endpoint->node_id << " not found.";

    CHECK_EQ(endpoints_.find(endpoint->node_id)->second.get(), endpoint)
        << "Endpoint " << endpoint->node_id << " mismatch.";

    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->connected = true;
    }
    endpoint->cv.notify_all();
  }

  void OnDisconnected(struct rdma_cm_event *event) {
    LOG(INFO) << "OnDisconnected from Node " << my_node_.id;
  }

  AddressPool<BufferContext> addr_pool_;
  std::unique_ptr<SimpleMempool> mempool_;

  struct rdma_cm_id *listener_ = nullptr;
  std::atomic<bool> should_stop_;

  std::unordered_map<int, std::unique_ptr<RDMAEndpoint>> endpoints_;
  std::unordered_map<std::string, std::unique_ptr<RDMAEndpoint>> temp_endpoints_;

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
  ThreadsafeQueue<std::tuple<RDMAEndpoint *, BufferContext *>> recv_buffers_;
};  // namespace ps
};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_
