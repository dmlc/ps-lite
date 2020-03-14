/**
 * Copyright (c) 2020 by Contributors
 * Authors: access2rohit@gmail.com (Rohit Srivastava)
 *          linhaibin.eric@gmail.com (Haibin Lin)
*/
#ifndef PS_FABRIC_VAN_H_
#define PS_FABRIC_VAN_H_

//#ifdef DMLC_USE_RDMA

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <zmq.h>

#include <rdma/rdma_cma.h>

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
#include "rdma_common.h"


#include <rdma/fi_errno.h>
#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>

namespace ps {

static const int kStartDepth = 128;
static const int kWriteDepth = kStartDepth;

static const int kRxDepth = kStartDepth * 2;
static const int kReplyDepth = kRxDepth;

static const int kSGEntry = 4;
static const int kTimeoutms = 1000;
static const int kRdmaListenBacklog = 128;
static const int kMaxConcurrentWorkRequest =
    kRxDepth + kStartDepth + kReplyDepth + kWriteDepth;
static const int kMaxHostnameLength = 16;
static const size_t kAlignment = 8;

static const int kMaxResolveRetry = 50000;
static const int kBasePort = 9010;

#define check_err(ret, msg) do {                          \
        if (ret != 0) {                                   \
          LOG(FATAL) << msg << ". Return Code: " << ret   \
                     << ". ERROR: " << fi_strerror(-ret); \
        }                                                 \
} while (false)

#define MASK_KEY   (0x3FFFFFFFFFFFFFFFULL)
#define MASK_START (0x8000000000000000ULL)
#define MASK_REPLY (0x4000000000000000ULL)

class FabricTransport;
struct FabricMemoryAllocator;
struct FabricEndpoint;

// TODO: make an union
struct RendezvousMsg {
  uint64_t meta_len;
  uint64_t data_num;
  uint64_t data_len[kMaxDataFields];
  uint64_t origin_addr; // the original address of the message buffer
  uint64_t idx; // the tag for tsend / trecv
  MessageTypes type;
};

std::string RendezvousDebugStr(const RendezvousMsg& msg) {
  std::stringstream ss;
  ss << "meta_len = " << msg.meta_len
     << ", data_num = " << msg.data_num
     << ", data_len = " << msg.data_len[0]
     << ", origin_addr = " << msg.origin_addr
     << ", idx = " << msg.idx
     << ", type = " << (msg.type == 0 ? "start" : "reply");
  return ss.str();
}

struct BufferContext {
  char *buffer;
  size_t meta_len;
  size_t data_num;
  size_t data_len[kMaxDataFields];
  FabricEndpoint *endpoint;
  size_t buf_size;
};


struct FabricWRContext {
  WRContextType type;
  void *buffer = nullptr;
  void *private_data = nullptr;
};

struct MessageBuffer {
  size_t inline_len;
  char *inline_buf;
  FabricWRContext *reserved_context;
  std::vector<SArray<char>> data;
  // std::vector<std::pair<MRPtr, size_t>> mrs;
};

struct RequestContext {
  uint32_t node;
  uint16_t port;
  char hostname[kMaxHostnameLength];
};

static_assert(std::is_pod<RendezvousMsg>::value,
              "RendezvousMsg must be a POD type.");
static_assert(std::is_pod<RequestContext>::value,
              "RequestContext must be a POD type.");

static const size_t kMempoolChunkSize = sizeof(RendezvousMsg);

class FabricMemoryAllocator {
 public:
  explicit FabricMemoryAllocator() {}

  ~FabricMemoryAllocator() {}

  char *Alloc(size_t size) {
    if (size == 0) {
      return nullptr;
    }
    // TODO(haibin) 8KB?
    // align to page size (usually 4KB)
    size = align_ceil(size, pagesize_);

    char *p;
    aligned_malloc((void**) &p, size);
    CHECK(p);

    std::lock_guard<std::mutex> lk(mu_);
    used_list.emplace(p, size);
    return p;
  }

  std::mutex mu_;
  size_t pagesize_ = sysconf(_SC_PAGESIZE);
  std::unordered_map<char *, size_t> used_list;
};


struct FabricAddr {
  // endpoint name
  char name[64] = {};
  // length of endpoint name
  size_t len = sizeof(name);

  std::string DebugStr() const {
    std::string debug_str = "";
    for (size_t i = 0; i < len; i++) {
      debug_str += std::to_string(name[i]) + ",";
    }
    return debug_str;
  }

  void CopyFrom(const char* ep_name, const size_t ep_name_len) {
    len = ep_name_len;
    memcpy(name, ep_name, sizeof(name));
  }

  void CopyTo(char* ep_name, size_t* ep_name_len) {
    *(ep_name_len) = len;
    memcpy(ep_name, name, sizeof(name));
  }

};

struct FabricContext {
  // fabric provider info
  struct fi_info *info;
  // fabric top-level object
  struct fid_fabric *fabric;
  // domains which maps to a specific local network interface adapter
  struct fid_domain *domain;
  // completion queue
  struct fid_cq *cq;
  // address vector
  struct fid_av *av;
  // the endpoint
  struct fid_ep *ep;
  // endpoint name
  struct FabricAddr addr;
  // readable endpoint name
  struct FabricAddr readable_addr;
  // maximum tag
  uint64_t max_tag;

  void Init() {
    struct fi_info *hints = nullptr;
    struct fi_cq_attr cq_attr = {};
    struct fi_av_attr av_attr = {};
    int fi_version, ret;

    // set hints for capacity and modes, create fabric, domain and cq
    hints = fi_allocinfo();
    CHECK(hints != nullptr) << "Failed to allocate hints";

    // hints to filter providers
    hints->ep_attr->type = FI_EP_RDM;
    hints->caps = FI_TAGGED | FI_MSG | FI_DIRECTED_RECV;
    hints->mode = FI_CONTEXT;
    hints->domain_attr->av_type = FI_AV_TABLE;
    hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
    hints->tx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->msg_order = FI_ORDER_SAS;

    // request for EFA as the provider
    hints->fabric_attr->prov_name = strdup("efa");
    fi_version = FI_VERSION(1, 8);

    // fi_getinfo
    ret = fi_getinfo(fi_version, nullptr, 0, 0, hints, &info);
    if (ret == -FI_ENODATA) {
      LOG(FATAL) << "Could not find any optimal provider";
      return;
    }
    check_err(ret, "fi_getinfo failed");
    fi_freeinfo(hints);

    // An example of info->ep_attr->mem_tag_format:
    // 12297829382473034410 =
    // 1010101010101010101010101010101010101010101010101010101010101010
    CHECK_EQ(info->ep_attr->mem_tag_format, 12297829382473034410ULL)
      << "unexpected structured tag format: "
      << info->ep_attr->mem_tag_format;

    // fi_fabric: create fabric
    ret = fi_fabric(info->fabric_attr, &fabric, nullptr);
    check_err(ret, "Couldn't open a fabric provider");

    // fi_domain: create domain
    ret = fi_domain(fabric, info, &domain, nullptr);
    check_err(ret, "Couldn't open a fabric access domain");

    // fi_av_open: create address vector
    av_attr.type = FI_AV_TABLE;
    ret = fi_av_open(domain, &av_attr, &av, nullptr);
    check_err(ret, "Couldn't open AV");

    // fi_cq_open: open completion queue
    cq_attr.format = FI_CQ_FORMAT_TAGGED;
    ret = fi_cq_open(domain, &cq_attr, &cq, nullptr);
    check_err(ret, "Couldn't open CQ");

    // fi_endpoint: create transport level communication endpoint(s)
    ret = fi_endpoint(domain, info, &ep, nullptr);
    check_err(ret, "Couldn't allocate endpoint");

    // fi_ep_bind: bind CQ and AV to the endpoint
    ret = fi_ep_bind(ep, (fid_t) cq, FI_SEND | FI_RECV);
    check_err(ret, "Couldn't bind EP-CQ");
    ret = fi_ep_bind(ep, (fid_t) av, 0);
    check_err(ret, "Couldn't bind EP-AV");

    // fi_enable: enable endpoint for communication
    ret = fi_enable(ep);
    check_err(ret, "Couldn't enable endpoint");

    // fi_getname: get endpoint name
    ret = fi_getname((fid_t) ep, addr.name, &addr.len);
    check_err(ret, "Call to fi_getname() failed");
    // fi_av_straddr: human readable name
    fi_av_straddr(av, addr.name, readable_addr.name, &readable_addr.len);
    PS_VLOG(1) << "Endpoint created: " << addr.DebugStr()
               << "readable endpoint = "
               << std::string(readable_addr.name, readable_addr.len);
  }

  void Close() {
    // fi_close((fid_t)ep);
    // fi_close((fid_t)cq);
    // fi_close((fid_t)av);
    // fi_close((fid_t)domain);
    // fi_close((fid_t)fabric);
    // fi_freeinfo(fi);
  }
};

struct FabricEndpoint {
  enum ConnectionStatus { IDLE, CONNECTING, CONNECTED, REJECTED };
  ConnectionStatus status;

  int node_id = Node::kEmpty;
  std::string hostport;
  std::condition_variable cv;
  std::mutex connect_mu;
  std::shared_ptr<FabricTransport> trans;
  fi_addr_t peer_addr;
  std::string readable_peer_addr;
  struct fid_ep *endpoint;

  FabricWRContext rx_ctx[kRxDepth];
  FabricWRContext start_ctx[kStartDepth];
  FabricWRContext reply_ctx[kReplyDepth];

  ThreadsafeQueue<FabricWRContext *> free_start_ctx;
  ThreadsafeQueue<FabricWRContext *> free_reply_ctx;

  void Init(const char* ep_name, struct fid_av *av, struct fid_ep *ep) {
    // fi_av_insert: insert address vector
    int ret = fi_av_insert(av, ep_name, 1, &peer_addr, 0, nullptr);
    CHECK_EQ(ret, 1) << "Call to fi_av_insert() failed. Return Code: "
                     << ret << ". ERROR: " << fi_strerror(-ret);

    // fi_av_straddr: human readable name
    FabricAddr readable_addr;
    fi_av_straddr(av, ep_name, readable_addr.name, &readable_addr.len);
    readable_peer_addr = std::string(readable_addr.name, readable_addr.len);
    PS_VLOG(1) << "Peer endpoint connected: " << readable_peer_addr;

    endpoint = ep;

    InitSendContextHelper(start_ctx, &free_start_ctx, kStartDepth,
                          kRendezvousStartContext);
    InitSendContextHelper(reply_ctx, &free_reply_ctx, kReplyDepth,
                          kRendezvousReplyContext);

    for (size_t i = 0; i < kRxDepth; ++i) {
      void *buf;
      aligned_malloc((void**) &buf, kMempoolChunkSize);
      CHECK(buf);

      // TODO(haibin) when to release the buffer
      rx_ctx[i].type = kReceiveContext;
      rx_ctx[i].buffer = buf;
      rx_ctx[i].private_data = this;

      PostRecv(&rx_ctx[i]);
    }
  }

  void PostRecv(FabricWRContext *ctx) {
    while (true) {
      int ret = fi_recv(endpoint, ctx->buffer, kMempoolChunkSize,
                        nullptr, peer_addr, ctx);
      if (ret == -FI_EAGAIN) {
        // no resources
        continue;
      } else if (ret != 0) {
        check_err(ret, "Unable to do fi_recv message");
      }
      break;
    }
    PS_VLOG(2) << "Posted recv buffer for " << readable_peer_addr
               << ". context = " << ctx << ". peer = "
               << readable_peer_addr;
  }

  void PostRecvTagged(void *buffer, uint64_t tag, size_t size) {
    while (true) {
      int ret = fi_trecv(endpoint, buffer, size, nullptr,
                         peer_addr, tag, 0, nullptr);
      if (ret == -FI_EAGAIN) {
        // no resources
        continue;
      } else if (ret != 0) {
        check_err(ret, "Unable to do fi_recv message");
      }
      break;
    }
    PS_VLOG(2) << "Posted tagged recv buffer " << buffer << " for " << readable_peer_addr
               << ". size = " << size;
  }

  void InitSendContextHelper(FabricWRContext *ctx,
                             ThreadsafeQueue<FabricWRContext *> *queue, size_t num,
                             WRContextType type) {
    for (size_t i = 0; i < num; ++i) {
      void *buf;
      // TODO(haibin) when to release the buffer
      aligned_malloc((void**) &buf, kMempoolChunkSize);
      CHECK(buf);

      ctx[i].type = type;
      ctx[i].buffer = buf;
      ctx[i].private_data = this;
      queue->Push(&ctx[i]);
    }
  }

  void SetNodeID(int id) { node_id = id; }

  void SetHostPort(std::string hp) { hostport = hp; }

  void SetTransport(std::shared_ptr<FabricTransport> t) { trans = t; }

  std::shared_ptr<FabricTransport> GetTransport() { return trans; }

};

class FabricTransport {
 public:
  explicit FabricTransport(FabricEndpoint *endpoint, FabricMemoryAllocator *allocator) {
    endpoint_ = CHECK_NOTNULL(endpoint);
    allocator_ = CHECK_NOTNULL(allocator);
    pagesize_ = sysconf(_SC_PAGESIZE);

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    is_server_ = (role=="server");
  };

  ~FabricTransport() {};

  void Send(FabricWRContext *context) {
    while (true) {
      int ret = fi_send(endpoint_->endpoint, context->buffer, kMempoolChunkSize,
                        nullptr, endpoint_->peer_addr, context);
      if (ret == -FI_EAGAIN) {
        LOG(WARNING) << "fi_send: FI_EAGAIN";
      } else if (ret != 0) {
        check_err(ret, "Unable to do fi_send message");
      } else {
        break;
      }
    }
    PS_VLOG(2) << "Posted fi_send to endpoint " << endpoint_->readable_peer_addr;
  }

  void TaggedSend(MessageBuffer* msg_buf, uint32_t idx) {
    while (true) {
      int ret = fi_tsend(endpoint_->endpoint, msg_buf->inline_buf, msg_buf->inline_len,
                         nullptr, endpoint_->peer_addr, idx, nullptr);
      if (ret == -FI_EAGAIN) {
        LOG(WARNING) << "fi_tsend: FI_EAGAIN";
      } else if (ret != 0) {
        check_err(ret, "Unable to do fi_send message");
      } else {
        break;
      }
    }
    PS_VLOG(2) << "Posted fi_send to endpoint " << endpoint_->readable_peer_addr;
  }

  void SendRendezvousBegin(Message &msg, MessageBuffer *msg_buf) {
    FabricWRContext *context = nullptr;
    endpoint_->free_start_ctx.WaitAndPop(&context);

    RendezvousMsg *req =
        reinterpret_cast<RendezvousMsg *>(context->buffer);
    req->meta_len = msg_buf->inline_len;
    req->origin_addr = reinterpret_cast<uint64_t>(msg_buf);
    req->data_num = msg_buf->data.size();
    req->type = kRendezvousStart;
    for (size_t i = 0; i < req->data_num; ++i) {
      req->data_len[i] = msg.data[i].size();
    }
    PS_VLOG(2) << "SendRendezvousBegin " << RendezvousDebugStr(*req);
    Send(context);
  }

  void SendRendezvousReply(RendezvousMsg *req, AddressPool<BufferContext> &addrpool) {
    BufferContext *buf_ctx = new BufferContext();
    buf_ctx->meta_len = req->meta_len;
    buf_ctx->data_num = req->data_num;
    buf_ctx->endpoint = endpoint_;

    size_t data_len = 0;
    for (size_t i = 0; i < req->data_num; ++i) {
      buf_ctx->data_len[i] = req->data_len[i];
      data_len += req->data_len[i];
    }

    // worker only needs a buffer for receving meta
    size_t alloc_size = is_server_ ? (align_ceil(req->meta_len, pagesize_) + data_len) : req->meta_len;
    buf_ctx->buf_size = alloc_size;
    char *buffer = allocator_->Alloc(alloc_size);
    CHECK(buffer);
    buf_ctx->buffer = buffer;

    FabricWRContext *reply_ctx = nullptr;
    endpoint_->free_reply_ctx.WaitAndPop(&reply_ctx);

    RendezvousMsg *resp =
        reinterpret_cast<RendezvousMsg *>(reply_ctx->buffer);

    resp->origin_addr = req->origin_addr;
    resp->type = kRendezvousReply;
    resp->idx = addrpool.StoreAddress(buf_ctx);

    endpoint_->PostRecvTagged(buffer, resp->idx, alloc_size);

    LOG(INFO) << "SendRendezvousReply " << RendezvousDebugStr(*resp);
    Send(reply_ctx);
  }

 protected:
  size_t pagesize_ = 8192;
  FabricEndpoint *endpoint_;
  FabricMemoryAllocator *allocator_;
  bool is_server_;

}; // class Transport

class FabricVan : public Van {
 public:
  FabricVan() {}
  ~FabricVan() {}

 protected:
  void Start(int customer_id, bool standalone) override {
    start_mu_.lock();
    should_stop_ = false;

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    LOG(INFO) << "This is a " << role;

    val = Environment::Get()->find("ENABLE_RDMA_LOG");
    enable_rdma_log_ = val? atoi(val) : false;
    if (enable_rdma_log_) LOG(INFO) << "Enable RDMA logging";
    else LOG(INFO) << "RDMA logging is disabled, you can enable it with ENABLE_RDMA_LOG=1";

    start_mu_.unlock();
    zmq_ = Van::Create("zmq");
    zmq_->Start(customer_id, true);
    Van::Start(customer_id, false);
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
    event_polling_thread_->join();
    event_polling_thread_.reset();

    PS_VLOG(1) << "Clearing mempool.";
    mem_allocator_.reset();


//    auto map_iter = memory_mr_map.begin();
//    while (map_iter != memory_mr_map.end()) {
//      ibv_dereg_mr(map_iter->second);
//      map_iter++;
//    }
//
    PS_VLOG(1) << "Clearing endpoints.";
    incoming_.clear();
    {
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      endpoints_.clear();
    }

//    PS_VLOG(1) << "Destroying cq and pd.";
//    CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
//    TODO destructor for FabricContext
    zmq_->Stop();
  }

  int Bind(const Node &node, int max_retry) override {
    std::lock_guard<std::mutex> lk(mu_);
    context_ = std::unique_ptr<FabricContext>(new FabricContext());
    if (enable_rdma_log_) LOG(INFO) << "Initializing a fabric endpoint";
    CHECK(context_ != nullptr) << "Failed to allocate Endpoint";

    InitContext();

    int my_port = zmq_->Bind(node, max_retry);
    PS_VLOG(1) << "Done zmq->Bind. My port is " << my_port;
    event_polling_thread_.reset(new std::thread(&FabricVan::PollEvents, this));
    return my_port;
  }

  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    PS_VLOG(1) << "Connect: " << node.DebugString();
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role)) {
      return;
    }
    const std::string remote_hostport = host_port(node.hostname, node.port);
    {
      std::lock_guard<std::mutex> lk(mu_);
      hostport_id_map_[remote_hostport] = node.id;
    }

    if (node.id != Node::kEmpty) {
      endpoints_mu_.lock();
      auto it = endpoints_.find(node.id);

      // if there is an endpoint with pending connection
      if (it != endpoints_.end()) {
        endpoints_.erase(it);
      }

      FabricEndpoint *endpoint;
      endpoints_[node.id] = std::make_unique<FabricEndpoint>();
      endpoint = endpoints_[node.id].get();
      endpoints_mu_.unlock();

      endpoint->SetNodeID(node.id);

      while (endpoint->status != FabricEndpoint::CONNECTED) {
        std::unique_lock<std::mutex> lk(endpoint->connect_mu);
        endpoint->status = FabricEndpoint::CONNECTING;

        Message req;
        req.meta.recver = node.id;
        req.meta.control.cmd = Control::ADDR_REQUEST;
        Node req_info;
        req_info.hostname = my_node_.hostname;
        req_info.port = my_node_.port;
        req_info.aux_id = node.id;
        context_->addr.CopyTo(req_info.endpoint_name, &req_info.endpoint_name_len);
        req.meta.control.node.push_back(req_info);
        // connect zmq. node id is recorded in hostport_id_map_
        zmq_->Connect(node);
        zmq_->Send(req);

        endpoint->cv.wait(lk, [endpoint] {
          return endpoint->status != FabricEndpoint::CONNECTING;
        });

        if (endpoint->status == FabricEndpoint::CONNECTED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
      std::shared_ptr<FabricTransport> t =
          std::make_shared<FabricTransport>(endpoint, mem_allocator_.get());
      endpoint->SetTransport(t);
    }
  }

  uint64_t DecodeKey(SArray<char> keys) { // just a translation, the decoded key might not be readable when we have multiple servers
    ps::Key key = 0;
    uint64_t coef = 1;
    for (unsigned int i = 0; i < keys.size(); ++i) {
      key += coef * (uint8_t) keys.data()[i];
      coef *= 256; // 256=2^8 (uint8_t)
    }
    return key;
  }

  void StoreMsgBuf(MessageBuffer *msg_buf, Message& msg) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    CHECK_EQ(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
    msgbuf_cache_[msg_buf] = msg;
  }

  MessageBuffer* PrepareNewMsgBuf(Message& msg) {
    MessageBuffer *msg_buf = new MessageBuffer();
    auto meta_len = GetPackMetaLen(msg.meta);
    msg_buf->inline_len = meta_len;
    msg_buf->inline_buf = mem_allocator_->Alloc(meta_len);
    msg_buf->data = msg.data;
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);
    return msg_buf;
  }

  int SendMsg(Message &msg) override {
    PS_VLOG(2) << "SendMsg: " << msg.DebugString();
    int remote_id = msg.meta.recver;
    CHECK_NE(remote_id, Meta::kEmpty);

    endpoints_mu_.lock();
    CHECK_NE(endpoints_.find(remote_id), endpoints_.end());
    FabricEndpoint *endpoint = endpoints_[remote_id].get();
    endpoints_mu_.unlock();

    int meta_len = GetPackMetaLen(msg.meta);
    size_t data_len = msg.meta.data_size;
    size_t total_len = meta_len + data_len;
    CHECK(meta_len);

    // pack meta info
    if (IsValidPushpull(msg)) {
      LOG(FATAL) << "NOT IMPLEMENTED";
      // AddMeta(msg);
      // PackWorkerTensorAddress(msg);
    }

    auto trans = CHECK_NOTNULL(endpoint->GetTransport());

    // start rendezvous if no remote info
    if (!IsValidPushpull(msg)) {
      MessageBuffer *msg_buf = PrepareNewMsgBuf(msg);
      StoreMsgBuf(msg_buf, msg);
      trans->SendRendezvousBegin(msg, msg_buf);
      return total_len;

    } else {
      LOG(FATAL) << "NOT IMPLEMENTED";
      // auto is_push = msg.meta.push;
      // auto key = msg.meta.key;
      // if (!HasRemoteInfo(msg, key, is_push, remote_id)) {
      //   MessageBuffer *msg_buf = PrepareNewMsgBuf(msg);
      //   StoreMsgBuf(msg_buf, msg);
      //   PrepareData(msg, msg_buf);
      //   trans->SendRendezvousBegin(msg, msg_buf);
      //   return total_len;
      // }
    }

    LOG(FATAL) << "NOT IMPLEMENTED";
    // TODO: ?
    /*
    auto addr_tuple = GetRemoteAndLocalInfo(msg.meta.key, msg.meta.push, remote_id);
    MessageBuffer *msg_buf = std::get<3>(addr_tuple); // local message buffer

    // prepare new meta and data
    CHECK_EQ(msg_buf->inline_len, (size_t) meta_len);
    CHECK(msg_buf->inline_buf);
    msg_buf->data = msg.data; // may not need this
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);

    // PrintSendLog(msg, msg_buf, addr_tuple);

    // already know remote address, directly use RDMA-write
    if (msg.meta.push && msg.meta.request) {
      // worker, push request
      LOG(FATAL) << "NOT IMPLEMENTED";
      // trans->SendPushRequest(msg, msg_buf, addr_tuple);
    } else if (msg.meta.push && !msg.meta.request) {
      // server, push response
      LOG(FATAL) << "NOT IMPLEMENTED";
      // trans->SendPushResponse(msg, msg_buf, addr_tuple);
    } else if (!msg.meta.push && msg.meta.request) {
      // worker, pull request
      LOG(FATAL) << "NOT IMPLEMENTED";
      // trans->SendPullRequest(msg, msg_buf, addr_tuple);
    } else if (!msg.meta.push && !msg.meta.request) {
      // server, pull response
      LOG(FATAL) << "NOT IMPLEMENTED";
      // map_mu_.lock();
      // auto temp_mr = mem_mr_.find(msg_buf->data[1].data());
      // CHECK_NE(temp_mr, mem_mr_.end());
      // map_mu_.unlock();
      // trans->SendPullResponse(msg, msg_buf, addr_tuple, temp_mr->second->lkey);
    } else {
      CHECK(0) << "unexpected message type";
    }
    */
    return total_len;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    // recv the meta first
    BufferContext* buffer_ctx;
    recv_buffers_.WaitAndPop(&buffer_ctx);
    FabricEndpoint *endpoint = buffer_ctx->endpoint;

    msg->meta.recver = my_node_.id;
    msg->meta.sender = endpoint->node_id;

    // the second argument is actually deprecated,
    // we keep it as is in order to be compatible
    UnpackMeta(buffer_ctx->buffer, buffer_ctx->meta_len, &msg->meta);
    int meta_len = GetPackMetaLen(msg->meta);

    int total_len = 0;
    total_len += meta_len;

    auto trans = CHECK_NOTNULL(endpoint->GetTransport());

    // PrintRecvLog(msg, buffer_ctx, meta_len);

    if (!IsValidPushpull(*msg)) {
      LOG(INFO) << "Recved " << total_len << " bytes";
      return total_len;
    }
    LOG(FATAL) << "NOT IMPLEMENTED";

    // // valid data message
    // if (msg->meta.push && msg->meta.request) {
    //   // push request
    //   total_len += trans->RecvPushRequest(msg, buffer_ctx, meta_len);
    //   StoreWorkerTensorAddress(msg);
    // } else if (!msg->meta.push && msg->meta.request) {
    //   // pull request
    //   total_len += trans->RecvPullRequest(msg, buffer_ctx, meta_len);
    // } else if (msg->meta.push && !msg->meta.request) {
    //   // push response
    //   total_len += trans->RecvPushResponse(msg, buffer_ctx, meta_len);
    // } else if (!msg->meta.push && !msg->meta.request) {
    //   // pull response
    //   total_len += trans->RecvPullResponse(msg, buffer_ctx, meta_len);
    // } else {
    //   CHECK(0) << "unknown msg type";
    // }

    return total_len;
  }

 private:
  void StoreRemoteAndLocalInfo(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t idx) {
    std::lock_guard<std::mutex> lk(addr_mu_);

    CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());

    auto& msg = msgbuf_cache_[msg_buf];

    auto key = msg.meta.key;
    auto is_push = msg.meta.push;
    auto recver = msg.meta.recver;

    // TODO: how is push and pull addr used later?
    // auto t = std::make_tuple(remote_addr, rkey, idx, msg_buf);
    // if (is_push) {
    //   push_addr_[key][recver] = t;
    // } else {
    //   pull_addr_[key][recver] = t;
    // }
  }

  void InitContext() {
    context_->Init();
    mem_allocator_.reset(new FabricMemoryAllocator());
  }

  void ReleaseWRContext(FabricWRContext *context, FabricEndpoint *endpoint) {
    switch (context->type) {
      case kRendezvousStartContext:
        endpoint->free_start_ctx.Push(context);
        break;
      case kRendezvousReplyContext:
        endpoint->free_reply_ctx.Push(context);
        break;
      //case kWriteContext:
      case kReceiveContext:
        endpoint->PostRecv(context);
        break;
      default:
        CHECK(0);
    }
  }

  Message* GetFirstMsg(MessageBuffer *msg_buf) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
    return &msgbuf_cache_[msg_buf];
  }

  void PollCQ() {
    // Pre-allocated work completions array used for polling
    struct fi_cq_err_entry err_entry;
    struct fi_cq_tagged_entry cq_entries[kMaxConcurrentWorkRequest];
    while (!should_stop_.load()) {
      int ret = fi_cq_read(context_->cq, cq_entries, kMaxConcurrentWorkRequest);
      if (ret == -FI_EAGAIN) {
        continue;
      } else if (ret == -FI_EAVAIL) {
        ret = fi_cq_readerr(context_->cq, &err_entry, 1);
        if (ret == FI_EADDRNOTAVAIL) {
          LOG(INFO) << "FI_EADDRNOTAVAIL";
        } else if (ret < 0) {
          LOG(FATAL) << "fi_cq_readerr failed. Return Code: " << ret
                     << ". ERROR: "
                     << fi_cq_strerror(context_->cq, err_entry.prov_errno,
                                       err_entry.err_data, nullptr,
                                       err_entry.err_data_size);
        } else {
          check_err(-err_entry.err, "fi_cq_read failed. retrieved error: ");
        }
      } else if (ret < 0) {
        check_err(ret, "fi_cq_read failed");
      } else {
        CHECK_NE(ret, 0) << "at least one completion event is expected";
        PS_VLOG(2) << ret << " completions ... ";
        for (int i = 0; i < ret; ++i) {
          uint64_t flags = cq_entries[i].flags;
          bool is_tagged = flags & FI_TAGGED;
          bool is_send = flags & FI_SEND;
          bool is_recv = flags & FI_RECV;
          if (is_tagged) {
            uint64_t tag = cq_entries[i].tag;
            if (is_send) {
              PS_VLOG(2) << "DONE FI_SEND tagged: " << tag;

            } else if (is_recv) {
              // the tag is the address of the buffer context posted for recving
              BufferContext *buf_ctx = addr_pool_.GetAddress(tag);
              recv_buffers_.Push(buf_ctx);
              buf_ctx->endpoint->PostRecvTagged(buf_ctx->buffer, tag,
                                                buf_ctx->buf_size);
              PS_VLOG(2) << "DONE FI_RECV tagged: " << tag;

            } else {
              LOG(FATAL) << "unknown completion entry" << flags;
            }
          } else {

            // op_context is the local context
            FabricWRContext *context = static_cast<FabricWRContext *>(cq_entries[i].op_context);
            FabricEndpoint *endpoint = static_cast<FabricEndpoint *>(context->private_data);
            CHECK_NE(endpoint, nullptr) << "FabricEndpoint should not be null";
            // Rendezvous messages
            CHECK_EQ(context->buffer, cq_entries[i].buf) << "buffer address does not match";
            RendezvousMsg *req = static_cast<RendezvousMsg*>(context->buffer);
            bool rendezvous_start = req->type == 0;
            if (is_send) {
              PS_VLOG(2) << "DONE FI_SEND " << RendezvousDebugStr(*req);
              ReleaseWRContext(context, endpoint);
            } else if (is_recv) {
              if (rendezvous_start) {
                // kRendezvousStart
                auto trans = CHECK_NOTNULL(endpoint->GetTransport());

                PS_VLOG(2) << "DONE FI_RECV " << RendezvousDebugStr(*req);
                trans->SendRendezvousReply(req, addr_pool_);
                ReleaseWRContext(context, endpoint);
              } else {
                // kRendezvousReply
                uint64_t origin_addr = req->origin_addr;
                uint32_t idx = req->idx;

                MessageBuffer *msg_buf =
                    reinterpret_cast<MessageBuffer *>(origin_addr);

                // Before RDMA write, store the remote info so that
                // subsequent write does not need repeated rendezvous
                // TODO: how to use the cached value?
                // no need for "remote_addr"? We may need to cache which endpoint it is
                // StoreRemoteAndLocalInfo(msg_buf, remote_addr, idx);

                Message *msg = GetFirstMsg(msg_buf);

                // auto addr_tuple = GetRemoteAndLocalInfo(msg->meta.key, msg->meta.push, msg->meta.recver);

                // PrintSendLog(*msg, msg_buf, addr_tuple);

                auto trans = CHECK_NOTNULL(endpoint->GetTransport());
                if (!IsValidPushpull(*msg)) {
                  // control message
                  trans->TaggedSend(msg_buf, idx);
                } else {
                  LOG(FATAL) << "NOT IMPLEMENTED";
                }

                // release the msg_buf from msgbuf_cache_
                // ReleaseFirstMsg(msg_buf);

                PS_VLOG(2) << "DONE FI_RECV kRendezvousReply";
                ReleaseWRContext(context, endpoint);
              }
            } else {
              LOG(FATAL) << "unknown completion entry" << flags;
            }
          }
        }
      }
    }

  }

  void PollEvents() {
    while (!should_stop_) {
      Message msg;
      int recv_bytes = zmq_->RecvMsg(&msg);
      // For debug, drop received message
      CHECK_NE(recv_bytes, -1) << "unexpected message size " << recv_bytes;
      PS_VLOG(2) << "received ZMQ message " << msg.DebugString();
      CHECK(!msg.meta.control.empty()) << "msg.meta.control is empty";
      auto &ctrl = msg.meta.control;
      if (ctrl.cmd == Control::ADDR_REQUEST) {
        OnConnectRequest(msg);
      } else if (ctrl.cmd == Control::ADDR_RESOLVED) {
        OnConnected(msg);
      } else {
        LOG(FATAL) << "Drop unknown typed message " << msg.DebugString();
      }
    }
  }

  std::string host_port(const std::string& host, const int port) {
    return host + ":" + std::to_string(port);
  }

  std::string get_host(const std::string& host_port) {
    return host_port.substr(0, host_port.find(":"));
  }

  int get_port(const std::string& host_port) {
    std::string port = host_port.substr(host_port.find(":") + 1);
    return std::stoi(port);
  }

  void OnConnected(const Message &msg) {
    const auto& addr_info = msg.meta.control.node[0];
    struct FabricAddr sender_addr;
    sender_addr.CopyFrom(addr_info.endpoint_name, addr_info.endpoint_name_len);
    const int sender_id = addr_info.aux_id;
    const std::string hostport = host_port(addr_info.hostname, addr_info.port);

    PS_VLOG(2) << "handling connected reply" << addr_info.DebugString();
    // retrieve and init endpoint
    FabricEndpoint *endpoint = endpoints_[sender_id].get();
    CHECK(endpoint) << "Endpoint not found.";
    endpoint->SetHostPort(hostport);
    endpoint->Init(sender_addr.name, context_->av, context_->ep);

    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&FabricVan::PollCQ, this));
    }

    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = FabricEndpoint::CONNECTED;
    }
    endpoint->cv.notify_all();
  }

  void OnRejected(const Message &msg) {
    LOG(FATAL) << "NOT IMPLEMENTED";
  }

  void OnConnectRequest(const Message &msg) {
    const auto& req_info = msg.meta.control.node[0];
    Node addr_info;
    FabricAddr src_addr;
    src_addr.CopyFrom(req_info.endpoint_name, req_info.endpoint_name_len);

    int sender_id;
    const std::string req_hostport = host_port(req_info.hostname, req_info.port);
    PS_VLOG(2) << "handling connection request " << req_info.DebugString() << ". " << req_hostport;
    {
      std::lock_guard<std::mutex> lk(mu_);
      // not connected before
      if (hostport_id_map_.find(req_hostport) == hostport_id_map_.end()) {
        sender_id = 30000 + hostport_id_map_.size();
        // connect to the remote node
        Node conn_node;
        // XXX the sender_id is not consistent with the actual node id
        conn_node.id = sender_id;
        conn_node.hostname = req_info.hostname;
        conn_node.port = req_info.port;
        // XXX: make sure the node differs such that connection is not skipped
        if (my_node_.role == Node::SCHEDULER) conn_node.role = Node::WORKER;
        else conn_node.role = Node::SCHEDULER;
        PS_VLOG(1) << "connect to unseen node " << req_hostport << " with id = " << sender_id;
        zmq_->Connect(conn_node);
        hostport_id_map_[req_hostport] = sender_id;
      }
      sender_id = hostport_id_map_[req_hostport];
    }
    context_->addr.CopyTo(addr_info.endpoint_name, &addr_info.endpoint_name_len);
    addr_info.hostname = my_node_.hostname;
    addr_info.port = my_node_.port;
    addr_info.aux_id = req_info.aux_id;

    Message reply;
    reply.meta.recver = sender_id;
    reply.meta.control.cmd = Control::ADDR_RESOLVED;
    reply.meta.control.node.push_back(addr_info);
    zmq_->Send(reply);

    const auto r = incoming_.emplace(std::make_unique<FabricEndpoint>());
    FabricEndpoint *endpoint = r.first->get();
    endpoint->SetHostPort(req_hostport);
    endpoint->Init(src_addr.name, context_->av, context_->ep);
    std::shared_ptr<FabricTransport> t =
        std::make_shared<FabricTransport>(endpoint, mem_allocator_.get());
    endpoint->SetTransport(t);

    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&FabricVan::PollCQ, this));
    }

  }

  void OnDisconnected(const Message &msg) {
    LOG(FATAL) << "NOT REACHED";
  }


  // TODO: a shared address pool may not have sufficient address
  AddressPool<BufferContext> addr_pool_;
  std::unique_ptr<FabricMemoryAllocator> mem_allocator_;

  std::atomic<bool> should_stop_;

  std::mutex endpoints_mu_;
  std::unordered_map<int, std::unique_ptr<FabricEndpoint>> endpoints_;
  std::unordered_set<std::unique_ptr<FabricEndpoint>> incoming_;

  //std::unordered_map<char *, struct ibv_mr *> memory_mr_map;

  // ibverbs protection domain
  struct ibv_pd *pd_ = nullptr;

  // cq thread
  std::unique_ptr<std::thread> cq_polling_thread_;
  // event thread
  std::unique_ptr<std::thread> event_polling_thread_;

  // Recv buffer queue
  ThreadsafeQueue<BufferContext *> recv_buffers_;

  // JYM: the following are for push/pull Fabricbuffer reuse

  // whether my role is server or not
  bool is_server;
  // RDMA logging info
  bool enable_rdma_log_ = false;

  // macros for key_meta_map
  using MetaInfo = std::tuple<int, uint64_t, int>; // len, addr, rkey
  using SenderMeta = std::unordered_map<int, MetaInfo>; // sender as the key
  // (key, sender) --> MetaInfo
  std::unordered_map<ps::Key, SenderMeta> key_meta_map_;
  // a static address for the key
  std::unordered_map<ps::Key, ps::Key> key_addr_map_;
  // a static address for the length
  std::unordered_map<ps::Key, int> key_len_map_;

  std::mutex map_mu_;

  // note that ZMQ use the node id to identify the senders.
  // to setup the connection for libfabric, we don't know the node id ahead of time
  // therefore, we need to use the add sender / receiver hostport to the message meta
  // such that when we unpack the message, we can still know where the message was sent
  // this requires that when calling these APIs:
  // - zmq_->Connect
  // - zmq_->Send
  // - zmq_->RecvMsg
  // we need to make sure req.meta.recver is set correctly.
  // we use hostport_id_map_ to map host:port to IDs. The ID can be arbtrary, as long
  // as the id is unique.
  std::unordered_map<std::string, int> hostport_id_map_;
  Van* zmq_;

  std::unique_ptr<FabricContext> context_;

  std::mutex mu_;
  bool is_worker_;

  void* receiver_ = nullptr; // for incoming connect queries

  std::mutex addr_mu_;
  // <key, recver>, (<remote_addr, rkey, idx, local_addr>)
  //std::unordered_map<uint64_t, RemoteAndLocalAddress> push_addr_;
  //std::unordered_map<uint64_t, RemoteAndLocalAddress> pull_addr_;
  std::unordered_map<MessageBuffer*, Message> msgbuf_cache_; // msg_buf, msg

  std::unordered_map<char*, struct fid_mr*> mem_mr_; // (memory address, fid_mr)
};  // namespace ps
};  // namespace ps

//#endif  // DMLC_USE_RDMA
#endif  // PS_FABRIC_VAN_H_
