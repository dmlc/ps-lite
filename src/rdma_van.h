/**
 *  Copyright (c) 2017 by Junxue ZHANG, Jingrong CHEN
 */
#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_
#ifdef MXNET_USE_RDMA

#include <errno.h>
#include <netdb.h>
#include <stdlib.h>

#include <rdma/rdma_cma.h>

#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ps/internal/allocator.h"
#include "ps/internal/bfc_allocator.h"
#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"
#include "ps/srmem.h"

namespace ps {

const int kRxDepth = 500;
const int kTxDepth = 500;
const int kSGEntry = 5;
const int kTimeoutms = 1000;

enum rdma_msg_type {
  MSG_REQ_REGION,
  MSG_RES_REGION,
  MSG_WRITE_DONE,
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
      struct {
        void *addr[5];
        uint32_t rkey;
      } mr;
      /* MSG_WRITE_DONE */
      struct rdma_write_header header;
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

  int sr_slots, rr_slots;

  struct rdma_msg *send_msg;
  struct rdma_msg *recv_msg;
  struct ibv_mr *send_msg_mr;
  struct ibv_mr *recv_msg_mr;

  volatile int connected;
  int active_side;
};

class RDMAVan : public Van {
 public:
  RDMAVan() {}
  ~RDMAVan() {}

 protected:
  void Start(int customer_id) override {
    start_mu_.lock();
    event_channel_ = rdma_create_event_channel();
    CHECK(event_channel_) << "create RDMA event channel failed";
    event_poller_should_stop_ = false;
    rdma_cm_event_poller_thread_ = new std::thread(&RDMAVan::OnEvent, this);
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
    CHECK(rdma_listen(listener_, 10) == 0) << "listen RDMA connection failed";

    return port;
  }

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
    CHECK(getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(), nullptr, &addr) ==
          0)
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

  void make_sge(struct ibv_sge *sge, void *addr, uint32_t length, uint32_t lkey) {
    sge->addr = (uintptr_t)addr;
    sge->length = length;
    sge->lkey = lkey;
  }

  void WriteToPeer(struct connection *conn, int recver, const SRMem<char> &srmem, void *addr,
                   uint32_t rkey, uint32_t lkey) {
    struct ibv_sge sge;
    struct ibv_send_wr wr, *bad_wr = nullptr;

    memset(&wr, 0, sizeof(wr));

    // wr.wr_id = (uintptr_t)conn;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = (uintptr_t)addr;
    wr.wr.rdma.rkey = rkey;
    wr.send_flags = IBV_SEND_SIGNALED;

    make_sge(&sge, srmem.data(), srmem.size(), lkey);

    /* Post Write Request */
    while (conn->sr_slots >= kTxDepth - 1) {
    }
    conn->sr_slots++;
    CHECK(ibv_post_send(conn->qp, &wr, &bad_wr) == 0)
        << "RDMA post send failed with errno: " << errno;

    /* Poll RDMA_WRITE Completion */
    int ret;
    struct ibv_wc wc;
    do {
      while ((ret = ibv_poll_cq(conn->cq, 1, &wc)) == 0) {
      }
      CHECK_GE(ret, 0);
      CHECK(wc.status == IBV_WC_SUCCESS) << "poll cq failed: " << wc.status;
      if (wc.opcode == IBV_WC_RECV) {
        if (--conn->rr_slots <= 1) {
          PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
          conn->rr_slots = kRxDepth;
        }
      }
    } while (wc.opcode != IBV_WC_RDMA_WRITE);
    CHECK(wc.opcode == IBV_WC_RDMA_WRITE) << "opcode != IBV_WC_RDMA_WRITE" << wc.opcode;

    conn->sr_slots--;
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
    PostSendRDMAMsg(conn);

    /* 2. Busy polling region response */
    struct ibv_wc wc;
    PollRDMAMsg(conn->cq, &wc);

    CHECK(conn->recv_msg->type == MSG_RES_REGION)
        << "receive message type != MSG_RES_REGION, " << conn->recv_msg->type;
    conn->sr_slots--;

    /* 3. Send the data using RDMA_WRITE */

    /* Fill the RDMA header */
    struct rdma_write_header *header;
    header = &conn->send_msg->data.header;
    header->sender = my_node_.id;
    header->recver = recver_id;
    header->length[0] = meta_size;

    SRMem<char> srmem(meta_size);
    meta.SerializeToArray(srmem.data(), meta_size);
    WriteToPeer(conn, recver_id, srmem, conn->recv_msg->data.mr.addr[0],
                conn->recv_msg->data.mr.rkey, context_->rdma_mr->lkey);

    for (size_t i = 0; i < msg.data.size(); i++) {
      /* This should be tune further */
      SRMem<char> srmem(msg.data[i]);
      uint32_t lkey = context_->rdma_mr->lkey;
      if (NICAllocator::GetNICAllocator()->registered(srmem.data(), 0))
        lkey = NICAllocator::GetNICAllocator()->mr(srmem.data())->lkey;
      WriteToPeer(conn, recver_id, srmem, conn->recv_msg->data.mr.addr[i + 1],
                  conn->recv_msg->data.mr.rkey, lkey);

      header->length[i + 1] = msg.data[i].size();
    }
    header->length[msg.data.size() + 1] = -1;

    /* 4. Notify remote side WRITE_DONE */
    conn->send_msg->type = MSG_WRITE_DONE;
    conn->send_msg->data.mr = conn->recv_msg->data.mr;
    PostSendRDMAMsg(conn);

    /* 5. Receive ACK_WRITE_DONE */
    do {
      int ret;
      while ((ret = ibv_poll_cq(conn->cq, 1, &wc)) == 0) {
      }
      CHECK_GE(ret, 0);
      CHECK(wc.status == IBV_WC_SUCCESS) << "poll cq failed: " << wc.status;
      if (wc.opcode == IBV_WC_RECV) {
        if (--conn->rr_slots <= 1) {
          PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
          conn->rr_slots = kRxDepth;
        }
        if (conn->recv_msg->type == MSG_WRITE_DONE) break;
      }
    } while (1);

    CHECK(conn->recv_msg->type == MSG_WRITE_DONE)
        << "type != WRITE_DONE, bug" << conn->recv_msg->type;

    conn->sr_slots--;

    return send_bytes;
  }

  void PollCQ() {
    struct connection *conn;
    struct ibv_wc wc;

    while (!cq_poller_should_stop_) {
      int ret = ibv_poll_cq(context_->cq, 1, &wc);
      CHECK(ret >= 0) << "error happens in ibv_poll_cq";

      if (ret == 0) continue;
      CHECK(wc.status == IBV_WC_SUCCESS)
          << "the worker completion status is not ibv_wc_success, but " << wc.status;

      conn = (struct connection *)wc.wr_id;

      if (wc.opcode == IBV_WC_RECV) {
        if (--conn->rr_slots <= 1) {
          PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
          conn->rr_slots = kRxDepth;
        }
      }
      if (wc.opcode == IBV_WC_SEND) {
        conn->sr_slots--;
        continue;
      }

      if (conn->recv_msg->type == MSG_REQ_REGION) {
        /* 1. Response MSG_REQ_REGION */
        conn->send_msg->type = MSG_RES_REGION;
        auto &data = conn->recv_msg->data;

        for (int i = 0, length; (length = data.length[i]) != -1; i++) {
          if (length == 0) length = 256;
          conn->send_msg->data.mr.addr[i] = NICAllocator::GetNICAllocator()->Allocate(length);
        }
        conn->send_msg->data.mr.rkey = context_->rdma_mr->rkey;
        PostSendRDMAMsg(conn, IBV_SEND_SIGNALED);

      } else if (conn->recv_msg->type == MSG_WRITE_DONE) {
        write_done_queue_.Push(*conn->recv_msg);
        conn->send_msg->type = MSG_WRITE_DONE;
        PostSendRDMAMsg(conn, IBV_SEND_SIGNALED);
      } else {
        LOG(ERROR) << "Unexpected msg: " << conn->recv_msg->type;
        exit(-1);
      }
    }
  }

  int RecvMsg(Message *msg) override {
    size_t recv_bytes = 0;
    struct rdma_write_header *header;
    struct rdma_msg recv_msg;

    msg->data.clear();

    void *addr;

    write_done_queue_.WaitAndPop(&recv_msg);

    /* handle message meta */
    addr = recv_msg.data.mr.addr[0];

    header = &recv_msg.data.header;
    msg->meta.sender = header->sender;
    msg->meta.recver = my_node_.id;

    UnpackMeta(static_cast<char *>(addr), header->length[0], &msg->meta);
    NICAllocator::GetNICAllocator()->Deallocate(addr);

    recv_bytes += header->length[0];

    // Zero-copy receiving
    for (int i = 1; header->length[i] != -1; i++) {
      addr = recv_msg.data.mr.addr[i];
      SRMem<char> srmem(static_cast<char *>(addr), header->length[i], true);
      SArray<char> sarray(srmem);
      msg->data.push_back(sarray);

      recv_bytes += header->length[i];
    }

    return recv_bytes;
  }

 private:
  bool IsConnected(struct connection *conn) { return conn->connected == 1; }

  void InitConnection(struct rdma_cm_id *id, bool active_side) {
    struct connection *conn = (struct connection *)malloc(sizeof(struct connection));
    id->context = conn;
    conn->id = id;
    conn->connected = 0;
    conn->active_side = active_side;
  }

  void OnEvent() {
    struct rdma_cm_event *event;

    while (!event_poller_should_stop_ && rdma_get_cm_event(event_channel_, &event) == 0) {
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
        CHECK(0) << "OnEvent: unknown event";
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
    CHECK(rdma_resolve_route(id, kTimeoutms) == 0) << "resolve RDMA route failed";
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
      CHECK(context_->ctx == verbs) << "cannot handle events in more than one context";
      context_->cnt++;
      CHECK_EQ(ibv_resize_cq(context_->cq, context_->cnt * (kRxDepth + kTxDepth)), 0);
      return;
    }

    context_ = (struct context *)malloc(sizeof(struct context));
    context_->ctx = verbs;

    context_->pd = ibv_alloc_pd(context_->ctx);
    CHECK(context_->pd) << "allocate protected domain failed";
    NICAllocator::GetNICAllocator()->set_pd(context_->pd);

    context_->cnt = 1;
    context_->cq = ibv_create_cq(context_->ctx, kRxDepth + kTxDepth, nullptr, nullptr, 0);
    CHECK(context_->cq) << "create completion queue failed";

    /* register data memory region */
    context_->rdma_mr = ibv_reg_mr(context_->pd, NICAllocator::GetNICAllocator()->ptr(),
                                   kDefaultSize, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    CHECK(context_->rdma_mr) << "register region failed";

    cq_poller_should_stop_ = false;
    cq_poller_thread_ = new std::thread(&RDMAVan::PollCQ, this);
  }

  /* register control message region */
  void RegisterMemory(struct connection *conn) {
    int rdma_msg_size = sizeof(struct rdma_msg);

    conn->send_msg = (struct rdma_msg *)malloc(rdma_msg_size);
    conn->send_msg_mr =
        ibv_reg_mr(context_->pd, conn->send_msg, rdma_msg_size, IBV_ACCESS_LOCAL_WRITE);
    CHECK(conn->send_msg_mr) << "register send_msg region failed";

    conn->recv_msg = (struct rdma_msg *)malloc(rdma_msg_size);
    conn->recv_msg_mr =
        ibv_reg_mr(context_->pd, conn->recv_msg, rdma_msg_size, IBV_ACCESS_LOCAL_WRITE);
    CHECK(conn->recv_msg_mr) << "register recv_msg region failed";
  }

  void PostSendRDMAMsg(struct connection *conn, int send_flags = 0) {
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
    while (1) {
      int ret = 0;
      while ((ret = ibv_poll_cq(cq, 1, wc)) == 0) {
      }
      CHECK(ret >= 0) << "error happens in ibv_poll_cq";
      CHECK(wc->status == IBV_WC_SUCCESS)
          << "the worker completion status is not ibv_wc_success, but " << wc->status;
      struct connection *conn = (struct connection *)wc->wr_id;
      if (wc->opcode == IBV_WC_RECV) {
        if (--conn->rr_slots <= 1) {
          PostRecvRDMAMsg(conn, kRxDepth - conn->rr_slots);
          conn->rr_slots = kRxDepth;
        }
        return;
      }
      if (wc->opcode == IBV_WC_SEND || wc->opcode == IBV_WC_RDMA_WRITE) conn->sr_slots--;
    }
  }

  void BuildConnection(struct rdma_cm_id *id, bool active) {
    struct connection *conn = (struct connection *)id->context;

    BuildContext(id->verbs);

    conn->cq = !active ? context_->cq
                       : ibv_create_cq(context_->ctx, kRxDepth + kTxDepth, nullptr, nullptr, 0);

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

    CHECK(rdma_create_qp(id, context_->pd, &qp_attr) == 0) << "create RDMA queue pair failed";
    conn->qp = id->qp;

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

  ThreadsafeQueue<struct rdma_msg> write_done_queue_;
  bool cq_poller_should_stop_ = false;
  std::thread *cq_poller_thread_;

  std::mutex s_send_mutex_;

  struct context *context_ = nullptr;
};

};  // namespace ps

#endif  // MXNET_USE_RDMA
#endif  // PS_RDMA_VAN_H_
