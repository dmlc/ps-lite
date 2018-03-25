/**
 *  Copyright (c) 2017 by Junxue ZHANG, Jingrong CHEN
 */
#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_
#ifdef MXNET_USE_RDMA

#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>

#include <rdma/rdma_cma.h>

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ps/internal/allocator.h"
#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"
#include "ps/srmem.h"

namespace ps {

#include <chrono>

//#define RDEBUG

#ifdef RDEBUG
#define debug(format, ...)                                                \
  do {                                                                    \
    auto now = std::chrono::high_resolution_clock::now();                 \
    fprintf(stdout, "\33[1;34m[%ld,%s,%d,%s,%d] " format "\33[0m\n",      \
            now.time_since_epoch().count(), __FILE__, __LINE__, __func__, \
            my_node_.id, ##__VA_ARGS__);                                  \
    fflush(stdout);                                                       \
  } while (0)
#else
#define debug(...)
#endif

#ifdef RDEBUG
void inspect(void *addr, int length) {
  char *ptr = (char *)addr;
  printf("In inspect, addr = %p, length = %d\n", addr, length);
  fflush(stdout);
  for (int i = 0; i < length; i++) {
    printf("%.2hhx ", ptr[i] & 0xff);
    if ((i & 0xf) == 0xf) {
      printf("\n");
      fflush(stdout);
    }
  }
  printf("\n");
  fflush(stdout);
}
#define inspect(...) inspect(__VA_ARGS__)
#else
#define inspect(...)
#endif

const int kRxDepth = 500;
const int kTxDepth = 2;
const int kSGEntry = 4;
const int kTimeoutms = 1000;

enum rdma_msg_type {
  MSG_REQ_REGION,
  MSG_RES_REGION,
  MSG_WRITE_DONE = 191,
};

/*
 * RDMA maximum message size <= 2GB,
 * thus we use 32bit int when it comes to length and offset
 */

struct rdma_write_header {
  int sender;
  int recver;
  int length[5];
};

struct rdma_msg {
  rdma_msg_type type;
  union {
    /* MSG_REQ_REGION */
    int length[5];
    struct {
      /* MSG_RES_REGION */
      uint32_t imm_data;
      struct {
        void *addr;
        uint32_t rkey;
      } mr;
    };
  } data;
};

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_mr *rdma_mr;
  int cnt;
};

struct connection {
  struct rdma_cm_id *id;
  struct ibv_qp *qp;
  struct ibv_cq *cq;

  // std::atomic<int> sr_slots, rr_slots;
  volatile int sr_slots, rr_slots;

  struct rdma_msg *send_msg;
  struct rdma_msg *recv_msg;
  struct ibv_mr *send_msg_mr;
  struct ibv_mr *recv_msg_mr;

  volatile int connected;
  int active_side;
  int max_inline_data;
};

class RDMAVan : public Van {
 public:
  RDMAVan() {}
  ~RDMAVan() {}

 protected:
  void Start(int customer_id) override {
    start_mu_.lock();
    if (event_channel_ == nullptr) {
      event_channel_ = rdma_create_event_channel();
      CHECK(event_channel_) << "create RDMA event channel failed";
      event_poller_should_stop_ = false;
      rdma_cm_event_poller_thread_ = new std::thread(&RDMAVan::OnEvent, this);
    }
    start_mu_.unlock();
    Van::Start(customer_id);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();

    rdma_destroy_id(listener_);

    cq_poller_should_stop_ = true;
    cq_poller_thread_->join();
    delete cq_poller_thread_;

    for (const auto &i : connections_)
      for (const auto &j : i.second) rdma_disconnect(j);

    while (num_connections_ > 0) {
    }
    /* TODO(cjr) flag here, there's a possibility the check fails. */
    CHECK_EQ(event_poller_should_stop_, true);
    rdma_cm_event_poller_thread_->join();
    delete rdma_cm_event_poller_thread_;

    rdma_destroy_event_channel(event_channel_);

    ibv_destroy_cq(context_->cq);
    ibv_dereg_mr(context_->rdma_mr);
    free(context_);
  }

  int Bind(const Node &node, int max_retry) override {
    CHECK(rdma_create_id(event_channel_, &listener_, nullptr, RDMA_PS_TCP) == 0)
        << "create RDMA connection identifier failed";

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    int port = node.port;

    for (int i = 0; i < max_retry + 1; ++i) {
      addr.sin_port = htons(port);
      if (rdma_bind_addr(listener_, (struct sockaddr *)&addr) == 0) break;

      if (i == max_retry)
        port = -1;
      else
        port += 1;
    }
    // TODO(cjr) change the backlog
    CHECK(rdma_listen(listener_, 10) == 0) << "listen RDMA connection failed";

    return port;
  }

  /* TODO(cjr) do some stuff to find and open device, make the code more robust
   */
  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    int node_id = node.id;

    if ((node.role == my_node_.role) && (node.id != my_node_.id)) return;

    struct rdma_cm_id *id;
    CHECK(rdma_create_id(event_channel_, &id, nullptr, RDMA_PS_TCP) == 0)
        << "create RDMA connection identifier failed";
    InitConnection(id, true);

    struct addrinfo *addr;
    CHECK(getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(),
                      nullptr, &addr) == 0)
        << "set address and port for connection failed";

    CHECK(rdma_resolve_addr(id, nullptr, addr->ai_addr, kTimeoutms) == 0)
        << "resolve RDMA address failed with errno: " << errno;

    freeaddrinfo(addr);

    struct connection *conn = (struct connection *)id->context;
    while (!IsConnected(conn)) {
    }

    connections_[node_id].push_back(id);
    LOG(INFO) << "Connected to node: " << node_id;
  }

  void make_sge(struct ibv_sge *sge, void *addr, uint32_t length,
                uint32_t lkey) {
    sge->addr = (uintptr_t)addr;
    sge->length = length;
    sge->lkey = lkey;
  }

  int SendMsg(const Message &msg) override {
    /* TODO(zjx) do we really need mutex lock */
    std::lock_guard<std::mutex> lock(s_send_mutex_);

    /* find the connection end point */
    int recver_id = msg.meta.recver;
    CHECK_NE(recver_id, Meta::kEmpty);

    auto it = connections_.find(recver_id);
    if (it == connections_.end()) {
      LOG(WARNING) << "there is no socket to node: " << recver_id;
      return -1;
    }

    struct rdma_cm_id *rdma_id = *it->second.rbegin();
    struct connection *conn = (struct connection *)rdma_id->context;

    PBMeta meta;
    PackMetaPB(msg.meta, &meta);
    uint32_t meta_size = meta.ByteSize();
    size_t send_bytes = meta_size + msg.meta.data_size;

    /* 1. Send region request */
    conn->send_msg->type = MSG_REQ_REGION;
    conn->send_msg->data.length[0] = meta_size;

    for (size_t i = 0; i < msg.data.size(); i++)
      conn->send_msg->data.length[i + 1] = msg.data[i].size();
    conn->send_msg->data.length[msg.data.size() + 1] = -1;

    debug("recver_id = %d, stage: client SEND MSG_REQ_REGION, conn = %p",
          recver_id, conn);
    CHECK_LE(sizeof(*conn->send_msg),
             static_cast<size_t>(conn->max_inline_data));

    PostSendRDMAMsg(conn, IBV_SEND_INLINE | IBV_SEND_SIGNALED);

    /* 2. Busy polling region response */
    struct ibv_wc wc;

    for (int ret, i = 0; i < 2; i++) {
      while ((ret = ibv_poll_cq(conn->cq, 1, &wc)) == 0) {
      }
      CHECK_GT(ret, 0) << "error happens in ibv_poll_cq";
      CHECK_EQ(wc.status, IBV_WC_SUCCESS)
          << "the worker completion status is not ibv_wc_success, but "
          << wc.status;
      CHECK(wc.opcode == IBV_WC_RECV || wc.opcode == IBV_WC_SEND)
          << "这又不可能了";
    }

    if (--conn->rr_slots <= 1) {
      PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
      conn->rr_slots = kRxDepth;
    }
    CHECK(conn->recv_msg->type == MSG_RES_REGION)
        << "receive message type != MSG_RES_REGION, " << conn->recv_msg->type;
    conn->sr_slots--;

    /* 3. Send the data using RDMA_WRITE_WITH_IMM */

    const size_t header_size = sizeof(struct rdma_write_header);
    SRMem<char> srmem(meta_size + header_size);
    meta.SerializeToArray(srmem.data() + header_size, meta_size);

    /* Fill the RDMA header */
    struct rdma_write_header *header;
    header = (struct rdma_write_header *)srmem.data();
    header->sender = my_node_.id;
    header->recver = recver_id;
    header->length[0] = meta_size;

    int total_length = srmem.size();
    struct ibv_sge sg_list[5];
    struct ibv_send_wr wr, *bad_wr = nullptr;

    memset(&wr, 0, sizeof(wr));

    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.next = nullptr;
    wr.wr.rdma.remote_addr = (uintptr_t)conn->recv_msg->data.mr.addr;
    wr.wr.rdma.rkey = conn->recv_msg->data.mr.rkey;
    wr.imm_data = htonl(conn->recv_msg->data.imm_data);
    // wr.send_flags = IBV_SEND_SIGNALED;

    make_sge(&sg_list[0], srmem.data(), srmem.size(), context_->rdma_mr->lkey);
    CHECK_EQ(srmem.size(), meta_size + header_size);

    std::vector<SRMem<char>> srmem_vec;

    int sge_idx = 1;
    for (size_t i = 0; i < msg.data.size(); i++) {
      /* TODO(cjr) check allocate and delete srmem, restructure the code, change
       * NICAllocator */
      // SRMem<char> srmem(msg.data[i]);
      srmem_vec.push_back(SRMem<char>(msg.data[i]));
      auto &srmem = *srmem_vec.rbegin();
      uint32_t lkey = context_->rdma_mr->lkey;

      if (NICAllocator::GetNICAllocator()->registered(srmem.data(), 0))
        lkey = NICAllocator::GetNICAllocator()->mr(srmem.data())->lkey;

      if (msg.data[i].size() > 0) {
        make_sge(&sg_list[sge_idx], srmem.data(), srmem.size(), lkey);
        sge_idx++;
        inspect(msg.data[i].data(), msg.data[i].size());
      }

      CHECK_EQ(srmem.size(), msg.data[i].size()) << "srmem出了点什么问题";

      header->length[i + 1] = srmem.size();
      total_length += srmem.size();
    }
    header->length[msg.data.size() + 1] = -1;

    wr.sg_list = sg_list;
    wr.num_sge = sge_idx;

    if (total_length <= conn->max_inline_data) wr.send_flags |= IBV_SEND_INLINE;

    debug(
        "recver_id = %d, stage: client WRITE_WITH_IMM, msg_num = %ld, "
        "total_length = %d, imm_data "
        "= %u, sr_slots = %d, conn = %p",
        recver_id, msg.data.size(), total_length, ntohl(wr.imm_data),
        conn->sr_slots, conn);

    while (conn->sr_slots >= kTxDepth - 1) {
    }
    conn->sr_slots++;
    CHECK(ibv_post_send(conn->qp, &wr, &bad_wr) == 0)
        << "RDMA post send failed with errno: " << errno;

    CHECK(conn->active_side);
    do {
      int ret;
      while ((ret = ibv_poll_cq(conn->cq, 1, &wc)) == 0) {
      }
      CHECK_GT(ret, 0);
      CHECK_EQ(wc.status, IBV_WC_SUCCESS) << "poll cq failed: " << wc.status;
      CHECK(wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM)
          << "不可能啊 opcode = " << wc.opcode;
      CHECK(wc.wc_flags & IBV_WC_WITH_IMM);
      if (wc.wc_flags & IBV_WC_WITH_IMM)
        debug(
            "recver_id = %d, stage: client receive WRITE_DONE message, "
            "imm_data = %d",
            recver_id, ntohl(wc.imm_data));
    } while (wc.opcode != IBV_WC_RECV_RDMA_WITH_IMM);

    if (--conn->rr_slots <= 1) {
      PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
      conn->rr_slots = kRxDepth;
    }

    // debug("conn->sr_slots= %d", conn->sr_slots);
    conn->sr_slots--;

    // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    return send_bytes;
  }

  void PollCQ() {
    struct connection *conn;
    struct ibv_wc wc;

    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.next = nullptr;
    wr.sg_list = nullptr;
    wr.num_sge = 0;
    wr.wr.rdma.remote_addr = 0;
    wr.wr.rdma.rkey = 0;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(MSG_WRITE_DONE);

    while (!cq_poller_should_stop_) {
      int ret = ibv_poll_cq(context_->cq, 1, &wc);
      if (ret == 0) continue;

      CHECK_GT(ret, 0) << "error happens in ibv_poll_cq";
      CHECK_EQ(wc.status, IBV_WC_SUCCESS)
          << "the worker completion status is not ibv_wc_success, but "
          << wc.status;

      conn = (struct connection *)wc.wr_id;
      CHECK(!conn->active_side);

      if (wc.opcode == IBV_WC_SEND) {
        CHECK(0);
        conn->sr_slots--;
        continue;
      }

      if (wc.opcode == IBV_WC_RDMA_WRITE) {
        conn->sr_slots--;
        debug("stage: server WRITE_WITH_IMM done, conn = %p, sr_slots = %d\n",
              conn, conn->sr_slots);
        continue;
      }

      if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
        // it indicates that a Send Region operation has done
        conn->sr_slots--;
        CHECK_NE(wc.wc_flags & IBV_WC_WITH_IMM, 0)
            << "In PollCQ WITH_IMM, some error happen";

        uint32_t imm_data = ntohl(wc.imm_data);
        debug("stage: server RECV_WIRTE_WITH_IMM, imm_data = %u, addr = %p",
              imm_data, recv_addr_[imm_data]);
        write_done_queue_.Push(recv_addr_[imm_data]);

        if (--conn->rr_slots <= 1) {
          PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
          conn->rr_slots = kRxDepth;
        }

        // WriteToPeer();
        wr.wr_id = (uintptr_t)conn;
        while (conn->sr_slots >= kTxDepth - 1) {
        }
        conn->sr_slots++;
        CHECK(ibv_post_send(conn->qp, &wr, &bad_wr) == 0)
            << "In PollCQ, RDMA post send failed with errno: " << errno;

        continue;
      }

      if (wc.opcode == IBV_WC_RECV) {
        if (--conn->rr_slots <= 1) {
          PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
          conn->rr_slots = kRxDepth;
        }
      }

      CHECK_EQ(wc.opcode, IBV_WC_RECV) << "不可能的吧";
      if (conn->recv_msg->type == MSG_REQ_REGION) {
        /* 1. Response MSG_REQ_REGION */
        conn->send_msg->type = MSG_RES_REGION;
        auto &data = conn->recv_msg->data;

        int total_length = sizeof(struct rdma_write_header);
        for (int i = 0, length; (length = data.length[i]) != -1; i++) {
          total_length += length;
        }

        conn->send_msg->data.mr.addr =
            NICAllocator::GetNICAllocator()->Allocate(total_length);
        conn->send_msg->data.mr.rkey = context_->rdma_mr->rkey;
        conn->send_msg->data.imm_data =
            static_cast<uint32_t>(recv_addr_.size());
        recv_addr_.push_back(conn->send_msg->data.mr.addr);

        debug(
            "stage: server SEND MSG_RES_REGION, total_length = %d, imm_data = "
            "%u, addr = %p",
            total_length, conn->send_msg->data.imm_data,
            conn->send_msg->data.mr.addr);

        CHECK_LE(sizeof(struct rdma_msg),
                 static_cast<size_t>(conn->max_inline_data));
        PostSendRDMAMsg(conn, IBV_SEND_INLINE);

      } else {
        LOG(ERROR) << "Unexpected msg: " << conn->recv_msg->type;
        exit(-1);
      }
    }
  }

  template <typename V>
  struct SRMemDeleter {
    SRMemDeleter(V *head, int count) : ref_count(count) {}
    void operator()(V *data) {
      if (--ref_count == 0) {
        NICAllocator::GetNICAllocator()->Deallocate(head);
      }
    }
    void *head;
    int ref_count = 0;
  };

  int RecvMsg(Message *msg) override {
    size_t recv_bytes = 0;
    struct rdma_write_header *header;

    msg->data.clear();

    void *addr;
    write_done_queue_.WaitAndPop(&addr);

    /* handle message meta */

    header = reinterpret_cast<struct rdma_write_header *>(addr);
    msg->meta.sender = header->sender;
    msg->meta.recver = my_node_.id;

    addr = static_cast<char *>(addr) + sizeof(*header);
    UnpackMeta(static_cast<char *>(addr), header->length[0], &msg->meta);

    recv_bytes += header->length[0];

    SRMemDeleter<char> deleter(reinterpret_cast<char *>(header), 0);
    // Zero-copy receiving
    for (int i = 1; header->length[i] != -1; i++) {
      addr = static_cast<char *>(addr) + header->length[i - 1];
      // SRMem<char> srmem(static_cast<char *>(addr), header->length[i], true);
      // SArray<char> sarray(srmem);

      /* TODO(cjr) sarray(0, 0); */
      // CHECK_NE(header->length[i], 0) << "In RecvMsg's loop, len = 0";
      if (header->length[i] == 0) {
        SArray<char> sarray(static_cast<char *>(addr), 0);
        msg->data.push_back(sarray);
      } else {
        SRMem<char> srmem(static_cast<char *>(addr), header->length[i],
                          deleter);
        SArray<char> sarray(srmem);
        inspect(sarray.data(), sarray.size());
        msg->data.push_back(sarray);
      }

      recv_bytes += header->length[i];
    }

    return recv_bytes;
  }

 private:
  bool IsConnected(struct connection *conn) { return conn->connected == 1; }

  void InitConnection(struct rdma_cm_id *id, bool active_side) {
    struct connection *conn =
        (struct connection *)malloc(sizeof(struct connection));
    // debug("struct conn constructed, conn = %p, active_side = %d", conn,
    // int(active_side));
    id->context = conn;
    conn->id = id;
    conn->connected = 0;
    conn->active_side = active_side;
  }

  /* TODO(cjr) add more event support to be more robust */
  void OnEvent() {
    struct rdma_cm_event *event;

    while (!event_poller_should_stop_ &&
           rdma_get_cm_event(event_channel_, &event) == 0) {
      struct rdma_cm_event event_copy;
      memcpy(&event_copy, event, sizeof(*event));
      rdma_ack_cm_event(event);

      if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST)
        OnConnectRequest(event_copy.id);
      else if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED)
        OnAddrResolved(event_copy.id);
      else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED)
        OnRouteResolved(event_copy.id);
      else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED)
        OnConnected(event_copy.id);
      else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED)
        OnDisconnected(event_copy.id);
      else
        CHECK(0) << "OnEvent: unknown event " << event_copy.event;
    }
  }

  void OnConnectRequest(struct rdma_cm_id *id) {
    struct rdma_conn_param cm_params;
    InitConnection(id, false);

    BuildConnection(id, false);
    BuildConnParam(&cm_params);
    CHECK(rdma_accept(id, &cm_params) == 0) << "accept RDMA connection failed";
  }

  void OnAddrResolved(struct rdma_cm_id *id) {
    BuildConnection(id, true);
    CHECK(rdma_resolve_route(id, kTimeoutms) == 0)
        << "resolve RDMA route failed";
  }

  void OnRouteResolved(struct rdma_cm_id *id) {
    struct rdma_conn_param cm_params;
    BuildConnParam(&cm_params);
    CHECK(rdma_connect(id, &cm_params) == 0) << "RDMA connect failed";
  }

  void OnConnected(struct rdma_cm_id *id) {
    struct connection *conn = (struct connection *)id->context;
    conn->connected = 1;
    num_connections_++;
  }

  void OnDisconnected(struct rdma_cm_id *id) {
    struct connection *conn = (struct connection *)id->context;
    rdma_destroy_qp(id);

    ibv_dereg_mr(conn->send_msg_mr);
    ibv_dereg_mr(conn->recv_msg_mr);

    free(conn->send_msg);
    free(conn->recv_msg);

    free(conn);
    rdma_destroy_id(id);
    CHECK_GE(--num_connections_, 0);
    if (num_connections_ == 0) event_poller_should_stop_ = true;
  }

  void BuildContext(struct ibv_context *verbs) {
    if (context_) {
      CHECK(context_->ctx == verbs)
          << "cannot handle events in more than one context";
      context_->cnt++;
      CHECK_EQ(
          ibv_resize_cq(context_->cq, context_->cnt * (kRxDepth + kTxDepth)),
          0);
      return;
    }

    context_ = (struct context *)malloc(sizeof(struct context));
    context_->ctx = verbs;

    context_->pd = ibv_alloc_pd(context_->ctx);
    CHECK(context_->pd) << "allocate protected domain failed";
    NICAllocator::GetNICAllocator()->set_pd(context_->pd);

    context_->cnt = 1;
    context_->cq =
        ibv_create_cq(context_->ctx, kRxDepth + kTxDepth, nullptr, nullptr, 0);
    CHECK(context_->cq) << "create completion queue failed";

    /* register data memory region */
    context_->rdma_mr = ibv_reg_mr(
        context_->pd, NICAllocator::GetNICAllocator()->ptr(), kDefaultSize,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    CHECK(context_->rdma_mr) << "register region failed";

    cq_poller_should_stop_ = false;
    cq_poller_thread_ = new std::thread(&RDMAVan::PollCQ, this);
  }

  /* register control message region */
  /* TODO(cjr) align memory, because these mem buf are access frequently in
   * datapath */
  void RegisterMemory(struct connection *conn) {
    int rdma_msg_size = sizeof(struct rdma_msg);

    conn->send_msg = (struct rdma_msg *)malloc(rdma_msg_size);
    conn->send_msg_mr = ibv_reg_mr(context_->pd, conn->send_msg, rdma_msg_size,
                                   IBV_ACCESS_LOCAL_WRITE);
    CHECK(conn->send_msg_mr) << "register send_msg region failed";

    conn->recv_msg = (struct rdma_msg *)malloc(rdma_msg_size);
    conn->recv_msg_mr = ibv_reg_mr(context_->pd, conn->recv_msg, rdma_msg_size,
                                   IBV_ACCESS_LOCAL_WRITE);
    CHECK(conn->recv_msg_mr) << "register recv_msg region failed";
  }

  void PostSendRDMAMsg(struct connection *conn, int send_flags = 0) {
    // debug("send_flags = %d", send_flags);
    struct ibv_send_wr wr, *bad_wr = nullptr;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)conn;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = send_flags;

    sge.addr = (uintptr_t)conn->send_msg;
    sge.length = sizeof(struct rdma_msg);
    sge.lkey = conn->send_msg_mr->lkey;

    // debug("sr_slots = %d", conn->sr_slots);
    while (conn->sr_slots >= kTxDepth - 1) {
    }
    conn->sr_slots++;
    CHECK(ibv_post_send(conn->qp, &wr, &bad_wr) == 0)
        << "send RDMA message failed with errno: " << errno;
  }

  void PostRecvRDMAMsg(struct connection *conn, int n) {
    struct ibv_recv_wr *bad_wr = nullptr;
    struct ibv_recv_wr wr;
    struct ibv_sge sge;

    wr.wr_id = (uintptr_t)conn;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)conn->recv_msg;
    sge.length = sizeof(struct rdma_msg);
    sge.lkey = conn->recv_msg_mr->lkey;

    for (int i = 0; i < n; i++)
      CHECK(ibv_post_recv(conn->qp, &wr, &bad_wr) == 0)
          << "post receive MSG failed with errno: " << errno;
  }

  void PollRDMAMsg(struct ibv_cq *cq, struct ibv_wc *wc) {
    do {
      int ret = 0;
      while ((ret = ibv_poll_cq(cq, 1, wc)) == 0) {
      }
      CHECK_GT(ret, 0) << "error happens in ibv_poll_cq";
      CHECK_EQ(wc->status, IBV_WC_SUCCESS)
          << "the worker completion status is not ibv_wc_success, but "
          << wc->status;
      struct connection *conn = (struct connection *)wc->wr_id;
      if (wc->opcode == IBV_WC_RECV) {
        if (--conn->rr_slots <= 1) {
          PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
          conn->rr_slots = kRxDepth;
        }
        return;
      }
    } while (1);
  }

  void BuildConnection(struct rdma_cm_id *id, bool active) {
    struct connection *conn = (struct connection *)id->context;

    BuildContext(id->verbs);

    conn->cq = !active ? context_->cq
                       : ibv_create_cq(context_->ctx, kRxDepth + kTxDepth,
                                       nullptr, nullptr, 0);

    CHECK(conn->cq) << "create completion queue failed";
    CHECK(ibv_req_notify_cq(conn->cq, 0) == 0)
        << "request notification from completion queue failed";

    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(struct ibv_qp_init_attr));
    qp_attr.send_cq = conn->cq;
    qp_attr.recv_cq = conn->cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = kTxDepth;
    qp_attr.cap.max_recv_wr = kRxDepth;
    qp_attr.cap.max_send_sge = kSGEntry;
    qp_attr.cap.max_recv_sge = kSGEntry;
    qp_attr.sq_sig_all = 0;

    CHECK(rdma_create_qp(id, context_->pd, &qp_attr) == 0)
        << "create RDMA queue pair failed";
    conn->qp = id->qp;
    conn->max_inline_data = qp_attr.cap.max_inline_data;

    // debug("%d\n", qp_attr.cap.max_send_wr);
    // debug("%d\n", qp_attr.cap.max_recv_wr);
    // debug("%d\n", qp_attr.cap.max_send_sge);
    // debug("%d\n", qp_attr.cap.max_recv_sge);
    // debug("%d\n", qp_attr.cap.max_inline_data);

    RegisterMemory(conn);

    conn->sr_slots = 0;
    conn->rr_slots = kRxDepth;
    PostRecvRDMAMsg(conn, kRxDepth);
  }

  void BuildConnParam(struct rdma_conn_param *param) {
    memset(param, 0, sizeof(*param));
    param->retry_count = 7;
    param->rnr_retry_count = 7;
  }

  struct rdma_cm_event *event_ = nullptr;
  struct rdma_cm_id *listener_ = nullptr;
  std::unordered_map<int, std::vector<rdma_cm_id *>> connections_;
  volatile int num_connections_ = 0;

  struct rdma_event_channel *event_channel_ = nullptr;

  bool event_poller_should_stop_ = false;
  std::thread *rdma_cm_event_poller_thread_;

  std::vector<void *> recv_addr_;
  ThreadsafeQueue<void *> write_done_queue_;

  bool cq_poller_should_stop_ = false;
  std::thread *cq_poller_thread_;

  std::mutex s_send_mutex_;

  struct context *context_ = nullptr;
};

};  // namespace ps

#endif  // MXNET_USE_RDMA
#endif  // PS_RDMA_VAN_H_
