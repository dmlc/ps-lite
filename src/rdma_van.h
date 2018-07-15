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

#include <queue>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"

namespace ps {

const int kRxDepth = 128;
const int kTxDepth = 128;
const int kSGEntry = 4;
const int kTimeoutms = 1000;
const int kRdmaListenBacklog = 128;
const size_t kBufSize = 0x80000000;
const int kRingSlots = 256;
const int kMaxConcurrentWrites = kRxDepth + kTxDepth;
const int kMaxHostnameLength = 16;
const int kMaxDataFields = 4;

template <typename T>
static inline T align_floor(T v, T align) {
  return v - (v % align);
}

template <typename T>
static inline T align_ceil(T v, T align) {
  return align_floor(v + align - 1, align);
}

enum MessageTypes : uint32_t {
  kRequest,
  kResponse,
};

struct alignas(64) Request {
  uint64_t meta_len;
  uint64_t data_num;
  uint64_t data_len[kMaxDataFields];
  uint64_t origin_addr;
};

struct alignas(64) Response {
  uint64_t addr;
  uint64_t origin_addr;
  uint32_t rkey;
  uint32_t idx;
};

struct alignas(64) WRContext {
  void *buffer;
  size_t len;
  void *private_data;
};

struct alignas(64) BufferContext {
  struct ibv_mr *mr;
  size_t meta_len;
  size_t data_num;
  size_t data_len[kMaxDataFields];
};

struct alignas(64) LocalBufferContext {
  size_t meta_len;
  void *meta_buf;
  std::vector<SArray<char>> data;
};

struct alignas(64) MessageBuffer {
  size_t meta_len;
  void *meta_buf;
  std::vector<SArray<char>> data;
  std::vector<struct ibv_mr *> regions;
};

struct RequestContext {
  uint32_t node;
  uint16_t port;
  char hostname[kMaxHostnameLength];
};

static_assert(std::is_pod<Request>::value, "Request must be a POD type.");
static_assert(std::is_pod<Response>::value, "Response must be a POD type.");
static_assert(std::is_pod<RequestContext>::value, "RequestContext must be a POD type.");

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
  struct ibv_mr *rx_mr[kRxDepth];

  explicit RDMAEndpoint(struct rdma_cm_id *id)
      : connected(false), active(false), node_id(Node::kEmpty), cm_id(id), rx_mr() {}

  ~RDMAEndpoint() {
    for (int i = 0; i < kRxDepth; ++i) {
      if (!rx_mr[i]) {
        continue;
      }
      free(rx_mr[i]->addr);
      CHECK_EQ(ibv_dereg_mr(rx_mr[i]), 0);
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

    for (size_t i = 0; i < kRxDepth; ++i) {
      void *buf = malloc(sizeof(Request));
      CHECK(buf);
      rx_mr[i] = ibv_reg_mr(pd, buf, sizeof(Request), IBV_ACCESS_LOCAL_WRITE);
      CHECK(rx_mr[i]);
      PostRecv(rx_mr[i]);
    }
  }

  void PostRecv(struct ibv_mr *mr) {
    struct ibv_recv_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));

    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(mr->addr);
    sge.length = sizeof(Request);
    sge.lkey = mr->lkey;

    WRContext *context = reinterpret_cast<WRContext *>(malloc(sizeof(WRContext)));
    context->buffer = mr;
    context->len = sizeof(Request);
    context->private_data = this;

    wr.wr_id = reinterpret_cast<uint64_t>(context);
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
    // TODO(clan): examine race condition
    int remote_id = msg.meta.recver;
    CHECK_NE(remote_id, Meta::kEmpty);

    PBMeta meta;
    PackMetaPB(msg.meta, &meta);

    if (remote_id == my_node_.id) {
      LocalBufferContext *buf_ctx = new LocalBufferContext();
      buf_ctx->meta_len = meta.ByteSize();
      buf_ctx->meta_buf = malloc(buf_ctx->meta_len);
      meta.SerializeToArray(buf_ctx->meta_buf, buf_ctx->meta_len);
      buf_ctx->data = msg.data;
      recv_buffers_.Push(
          std::make_tuple(kLocalTransfer, reinterpret_cast<BufferContext *>(buf_ctx)));
      return buf_ctx->meta_len + msg.meta.data_size;
    }

    CHECK_NE(endpoints_.find(remote_id), endpoints_.end());
    RDMAEndpoint *endpoint = endpoints_[remote_id].get();

    MessageBuffer *msg_buf = new MessageBuffer();
    CHECK(meta.ByteSize());
    msg_buf->meta_len = meta.ByteSize();
    msg_buf->meta_buf = malloc(msg_buf->meta_len);
    msg_buf->data = msg.data;
    meta.SerializeToArray(msg_buf->meta_buf, msg_buf->meta_len);

    Request *req = reinterpret_cast<Request *>(malloc(sizeof(Request)));
    req->meta_len = meta.ByteSize();
    for (size_t i = 0; i < msg.data.size(); ++i) {
      req->data_len[i] = msg.data[i].size();
    }
    req->data_num = msg.data.size();
    req->origin_addr = reinterpret_cast<uint64_t>(msg_buf);

    struct ibv_mr *mr = ibv_reg_mr(pd_, req, sizeof(Request), 0);
    CHECK(mr);

    WRContext *context = reinterpret_cast<WRContext *>(malloc(sizeof(WRContext)));
    context->buffer = req;
    context->len = sizeof(Request);
    context->private_data = mr;

    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(req);
    sge.length = sizeof(Request);
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = reinterpret_cast<uint64_t>(context);
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.next = nullptr;

    wr.imm_data = kRequest;

    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0) << "ibv_post_send failed.";

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
      UnpackMeta(reinterpret_cast<char *>(buffer_ctx->meta_buf), buffer_ctx->meta_len, &msg->meta);
      msg->data = buffer_ctx->data;
      int total_len = buffer_ctx->meta_len + msg->meta.data_size;
      free(buffer_ctx->meta_buf);
      delete buffer_ctx;
      return total_len;
    }

    BufferContext *buffer_ctx = std::get<BufferContext *>(notification);

    int total_len = 0;

    msg->meta.recver = my_node_.id;
    msg->meta.sender = endpoint->node_id;

    const char *cur = reinterpret_cast<char *>(buffer_ctx->mr->addr);
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

    CHECK_EQ(ibv_dereg_mr(buffer_ctx->mr), 0);
    free(buffer_ctx);

    return total_len;
  }

 private:
  void InitContext(struct ibv_context *context) {
    context_ = context;
    CHECK(context_) << "ibv_context* empty";

    pd_ = ibv_alloc_pd(context_);
    CHECK(pd_) << "Failed to allocate protection domain";

    comp_event_channel_ = ibv_create_comp_channel(context_);
    cq_ = ibv_create_cq(context_, kMaxConcurrentWrites * 2, NULL, comp_event_channel_, 0);

    CHECK(cq_) << "Failed to create completion queue";
    CHECK(!ibv_req_notify_cq(cq_, 0)) << "Failed to request CQ notification";

    cq_polling_thread_.reset(new std::thread(&RDMAVan::PollCQ, this));
  }

  void PollCQ() {
    // Pre-allocated work completions array used for polling
    struct ibv_wc wc[kMaxConcurrentWrites * 2];
    while (!should_stop_.load()) {
      int ne = ibv_poll_cq(cq_, kMaxConcurrentWrites * 2, wc);
      CHECK_GE(ne, 0);
      for (int i = 0; i < ne; ++i) {
        CHECK(wc[i].status == IBV_WC_SUCCESS)
            << "Failed status \n"
            << ibv_wc_status_str(wc[i].status) << " " << wc[i].status << " "
            << static_cast<int>(wc[i].wr_id) << " " << wc[i].vendor_err;

        WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);

        switch (wc[i].opcode) {
          case IBV_WC_SEND: {
            // LOG(INFO) << "opcode: IBV_WC_SEND";
            free(context->buffer);
            struct ibv_mr *mr = reinterpret_cast<struct ibv_mr *>(context->private_data);
            CHECK_EQ(ibv_dereg_mr(mr), 0);
          } break;
          case IBV_WC_RDMA_WRITE: {
            // LOG(INFO) << "opcode: IBV_WC_RDMA_WRITE";
            MessageBuffer *msg_buf = reinterpret_cast<MessageBuffer *>(context->private_data);
            free(msg_buf->meta_buf);
            for (auto &mr : msg_buf->regions) {
              CHECK_EQ(ibv_dereg_mr(mr), 0);
            }
            delete msg_buf;
          } break;
          case IBV_WC_RECV_RDMA_WITH_IMM: {
            // LOG(INFO) << "opcode: IBV_WC_RECV_RDMA_WITH_IMM";
            uint32_t idx = wc[i].imm_data;
            BufferContext *buf_ctx = addr_pool_.GetAddressAndRelease(idx);
            RDMAEndpoint *endpoint = reinterpret_cast<RDMAEndpoint *>(context->private_data);
            CHECK(endpoint);
            struct ibv_mr *mr = reinterpret_cast<struct ibv_mr *>(context->buffer);
            endpoint->PostRecv(mr);
            recv_buffers_.Push(std::make_tuple(endpoint, buf_ctx));
          } break;
          case IBV_WC_RECV: {
            CHECK(wc[i].wc_flags & IBV_WC_WITH_IMM);
            uint32_t imm = wc[i].imm_data;

            RDMAEndpoint *endpoint = reinterpret_cast<RDMAEndpoint *>(context->private_data);
            struct ibv_mr *mr = reinterpret_cast<struct ibv_mr *>(context->buffer);

            if (imm == kRequest) {
              // LOG(INFO) << "opcode: IBV_WC_RECV kRequest";
              Request *req = reinterpret_cast<Request *>(mr->addr);

              BufferContext *buf_ctx =
                  reinterpret_cast<BufferContext *>(malloc(sizeof(BufferContext)));

              uint64_t len = req->meta_len;
              buf_ctx->meta_len = len;
              buf_ctx->data_num = req->data_num;
              for (size_t i = 0; i < req->data_num; ++i) {
                buf_ctx->data_len[i] = req->data_len[i];
                len += req->data_len[i];
              }

              void *buffer = malloc(len);
              CHECK(buffer) << "malloc for " << len << " bytes, data_num: " << req->data_num;
              struct ibv_mr *rx_mr =
                  ibv_reg_mr(pd_, buffer, len, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
              CHECK(rx_mr);
              buf_ctx->mr = rx_mr;
              uint64_t origin_addr = req->origin_addr;

              Response *resp = reinterpret_cast<Response *>(malloc(sizeof(Response)));
              resp->addr = reinterpret_cast<uint64_t>(buffer);
              resp->rkey = rx_mr->rkey;
              resp->origin_addr = origin_addr;
              resp->idx = addr_pool_.StoreAddress(buf_ctx);

              struct ibv_mr *mr = ibv_reg_mr(pd_, resp, sizeof(Response), 0);
              CHECK(mr);

              WRContext *context_tx = reinterpret_cast<WRContext *>(malloc(sizeof(WRContext)));
              context_tx->buffer = resp;
              context_tx->len = sizeof(Response);
              context_tx->private_data = mr;

              struct ibv_sge sge;
              sge.addr = reinterpret_cast<uint64_t>(resp);
              sge.length = sizeof(Response);
              sge.lkey = mr->lkey;

              struct ibv_send_wr wr, *bad_wr = nullptr;
              memset(&wr, 0, sizeof(wr));

              wr.wr_id = reinterpret_cast<uint64_t>(context_tx);
              wr.opcode = IBV_WR_SEND_WITH_IMM;
              wr.next = nullptr;

              wr.imm_data = kResponse;

              wr.send_flags = IBV_SEND_SIGNALED;
              wr.sg_list = &sge;
              wr.num_sge = 1;

              CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
                  << "ibv_post_send failed.";

            } else if (imm == kResponse) {
              // LOG(INFO) << "opcode: IBV_WC_RECV kResponse";
              Response *resp = reinterpret_cast<Response *>(mr->addr);
              uint64_t remote_addr = resp->addr;
              uint64_t origin_addr = resp->origin_addr;
              uint32_t rkey = resp->rkey;
              uint32_t idx = resp->idx;

              MessageBuffer *msg_buf = reinterpret_cast<MessageBuffer *>(origin_addr);

              WRContext *context_tx = reinterpret_cast<WRContext *>(malloc(sizeof(WRContext)));
              context_tx->buffer = nullptr;
              context_tx->len = 0;
              context_tx->private_data = msg_buf;

              struct ibv_sge sge[msg_buf->data.size() + 1];

              struct ibv_mr *meta_mr = ibv_reg_mr(pd_, msg_buf->meta_buf, msg_buf->meta_len, 0);
              CHECK(meta_mr) << strerror(errno);

              sge[0].addr = reinterpret_cast<uint64_t>(msg_buf->meta_buf);
              sge[0].length = msg_buf->meta_len;
              sge[0].lkey = meta_mr->lkey;

              msg_buf->regions.push_back(meta_mr);

              size_t num_sge = 1;
              for (size_t i = 0; i < msg_buf->data.size(); ++i) {
                void *p = msg_buf->data[i].ptr().get();
                size_t len = msg_buf->data[i].size();
                if (len > 0) {
                  struct ibv_mr *mr = ibv_reg_mr(pd_, p, len, 0);
                  sge[num_sge].addr = reinterpret_cast<uint64_t>(p);
                  sge[num_sge].length = len;
                  sge[num_sge].lkey = mr->lkey;
                  msg_buf->regions.push_back(mr);
                  ++num_sge;
                }
              }

              struct ibv_send_wr wr, *bad_wr = nullptr;
              memset(&wr, 0, sizeof(wr));

              wr.wr_id = reinterpret_cast<uint64_t>(context_tx);
              wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
              wr.next = nullptr;

              wr.imm_data = idx;

              wr.send_flags = IBV_SEND_SIGNALED;
              wr.sg_list = sge;
              wr.num_sge = num_sge;

              wr.wr.rdma.remote_addr = remote_addr;
              wr.wr.rdma.rkey = rkey;

              CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
                  << "ibv_post_send failed.";

            } else {
              CHECK(0);
            }
            endpoint->PostRecv(mr);
          } break;
          default:
            CHECK(0) << "Unexpected opcode: " << wc[i].opcode;
        }
        free(context);
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
