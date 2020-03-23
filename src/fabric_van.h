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


static const int kStartDepth = 2;
static const int kRxDepth = 4; // should be larger than kStartDepth
static const int kReplyDepth = kRxDepth;

static const int kTimeoutms = 1000;
static const int kRdmaListenBacklog = 128;
static const int kMaxConcurrentWorkRequest =
    kRxDepth + kStartDepth + kReplyDepth;
static const int kMaxHostnameLength = 16;


static const uint64_t kRendezvousStartMask = 0x8000000000000000ULL;
static const uint64_t kRendezvousReplyMask = 0x4000000000000000ULL;
static const uint64_t kDataMask = 0x3FFFFFFFFFFFFFFFULL;
// 1010101010101010101010101010101010101010101010101010101010101010
static const uint64_t kEFAMemTagFormat = 12297829382473034410ULL;

#define check_err(ret, msg) do {                          \
        if (ret != 0) {                                   \
          LOG(FATAL) << msg << ". Return Code: " << ret   \
                     << ". ERROR: " << fi_strerror(-ret); \
        }                                                 \
} while (false)

class FabricTransport;
struct FabricMemoryAllocator;
struct FabricEndpoint;

struct RendezvousMsg {
  uint64_t meta_len;
  uint64_t data_num;
  uint64_t data_len[kMaxDataFields];
  uint64_t origin_addr; // the original address of the message buffer
  uint64_t idx; // the tag for tsend / trecv
};

std::string RendezvousDebugStr(const RendezvousMsg& msg) {
  std::stringstream ss;
  ss << "meta_len = " << msg.meta_len
     << ", data_num = " << msg.data_num
     << ", data_len = " << msg.data_len[0]
     << ", origin_addr = " << msg.origin_addr
     << ", idx = " << msg.idx;
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

enum WRContextType {
  kRendezvousStartContext,
  kRendezvousReplyContext,
  kWriteContext,
  kReadContext,
  kReceiveContext
};

struct FabricWRContext {
  WRContextType type;
  void *private_data = nullptr;
  uint64_t tag;
  struct iovec buf_vec[2];
  size_t num_buf_vec;
};


std::string WRContextDebugStr(const FabricWRContext& ctx) {
  std::stringstream ss;
  ss << "type = " << ctx.type
     << ", private_data = " << ctx.private_data
     << ", tag = " << ctx.tag
     << ", num_buf_vec = " << ctx.num_buf_vec;
  for (size_t i = 0; i < ctx.num_buf_vec; i++) {
     ss << ", buf_vec[" << i << "].iov_base = " << ctx.buf_vec[i].iov_base
        << ", buf_vec[" << i << "].iov_len = " << ctx.buf_vec[i].iov_len;
  }
  return ss.str();
}

std::string host_port(const std::string& host, const int port) {
  return host + ":" + std::to_string(port);
}

void PrepareContext(FabricWRContext* context, void *meta_ptr, size_t meta_size,
                    void *data_ptr, size_t data_size) {
  context->buf_vec[0].iov_base = meta_ptr;
  context->buf_vec[0].iov_len = meta_size;
  if (data_size != 0) {
    context->num_buf_vec = 2;
    context->buf_vec[1].iov_base = data_ptr;
    context->buf_vec[1].iov_len = data_size;
  } else {
    context->num_buf_vec = 1;
  }
}


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
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < len; i++) {
      ss << std::to_string(name[i]) << ",";
    }
    ss << "]";
    return ss.str();
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
    CHECK_NE(ret, -FI_ENODATA) << "Could not find any optimal provider";
    check_err(ret, "fi_getinfo failed");
    fi_freeinfo(hints);

    CHECK_EQ(info->ep_attr->mem_tag_format, kEFAMemTagFormat)
      << "unexpected structured tag format: " << info->ep_attr->mem_tag_format;

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
               << " readable endpoint = "
               << std::string(readable_addr.name, readable_addr.len);
  }

  ~FabricContext() {
    PS_VLOG(2) << "~FabricContext";
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
  ConnectionStatus status = IDLE;
  // the node id
  int node_id = Node::kEmpty;
  // host:port
  std::string hostport;
  // mutex for connection
  std::mutex connect_mu;
  // cv for connection
  std::condition_variable cv;
  // the transport
  std::shared_ptr<FabricTransport> trans;
  // the endpoint name of the remote peer
  fi_addr_t peer_addr;
  // the readable endpoint name of the remote peer
  std::string readable_peer_addr;
  // the fabric endpoint
  struct fid_ep *endpoint;
  // whether it is initialized
  bool initialized = false;

  // send ctxs
  FabricWRContext start_tx_ctx[kStartDepth];
  FabricWRContext reply_tx_ctx[kReplyDepth];

  // receive ctxs
  FabricWRContext start_rx_ctx[kRxDepth / 2];
  FabricWRContext reply_rx_ctx[kRxDepth / 2];
  std::unordered_map<uint64_t, std::unique_ptr<FabricWRContext>> data_rx_ctx;

  // queues for send ctxs
  ThreadsafeQueue<FabricWRContext *> free_start_ctx;
  ThreadsafeQueue<FabricWRContext *> free_reply_ctx;

  ~FabricEndpoint() {
    // TODO: properly release resources
    PS_VLOG(2) << "~FabricEndpoint";
  }

  void InitEndpoint(const char* ep_name, struct fid_av *av, struct fid_ep *ep,
                    const std::string& hp) {
    // fi_av_insert: insert address vector of the remote endpoint
    int ret = fi_av_insert(av, ep_name, 1, &peer_addr, 0, nullptr);
    CHECK_EQ(ret, 1) << "Call to fi_av_insert() failed. Return Code: "
                     << ret << ". ERROR: " << fi_strerror(-ret);

    // fi_av_straddr: human readable name
    FabricAddr readable_addr;
    fi_av_straddr(av, ep_name, readable_addr.name, &readable_addr.len);
    readable_peer_addr = std::string(readable_addr.name, readable_addr.len);
    PS_VLOG(3) << "Peer endpoint connected: " << readable_peer_addr;

    // set endpoint
    endpoint = ep;
    hostport = hp;
  }

  void Init(const char* ep_name, struct fid_av *av, struct fid_ep *ep,
            const std::string& hp, FabricMemoryAllocator *allocator) {
    // set the endpoint and its address
    InitEndpoint(ep_name, av, ep, hp);
    // set contexts
    InitSendContext(start_tx_ctx, &free_start_ctx, kStartDepth,
                    kRendezvousStartContext, kRendezvousStartMask);
    InitSendContext(reply_tx_ctx, &free_reply_ctx, kReplyDepth,
                    kRendezvousReplyContext, kRendezvousReplyMask);
    InitRecvContext(start_rx_ctx, kRxDepth / 2,
                    kReceiveContext, kRendezvousStartMask);
    InitRecvContext(reply_rx_ctx, kRxDepth / 2,
                    kReceiveContext, kRendezvousReplyMask);
    // set transport
    trans = std::make_shared<FabricTransport>(this, allocator);
    initialized = true;
    PS_VLOG(3) << "Endpoint initialized";
  }

  void PostRecv(FabricWRContext *ctx) {
    while (true) {
      int ret = fi_trecvv(endpoint, ctx->buf_vec, nullptr,
                          ctx->num_buf_vec, peer_addr, ctx->tag, 0, static_cast<void *>(ctx));
      if (ret == -FI_EAGAIN) {
        // no resources
        LOG(WARNING) << "fi_trecv: FI_EAGAIN";
        continue;
      } else if (ret != 0) {
        check_err(ret, "Unable to do fi_recv message");
      }
      break;
    }
    PS_VLOG(3) << "Posted recv buffer " << ctx->buf_vec << " for "
               << readable_peer_addr << ". size = " << ctx->num_buf_vec // size
               << " ctx = " << static_cast<void *>(ctx);
  }

  void InitRecvContext(FabricWRContext *ctx, size_t num,
                       WRContextType type, uint64_t tag) {
    for (size_t i = 0; i < num; ++i) {
      void *buf;
      aligned_malloc((void**) &buf, kMempoolChunkSize);
      CHECK(buf);

      ctx[i].private_data = this;
      ctx[i].tag = tag;
      PS_VLOG(4) << "InitRecvContext " << i << " / " << num;
      PrepareContext(ctx + i, buf, kMempoolChunkSize, static_cast<char*>(buf) + kMempoolChunkSize, 0);
      PostRecv(&ctx[i]);
    }
  }

  void InitSendContext(FabricWRContext *ctx,
                       ThreadsafeQueue<FabricWRContext *> *queue, size_t num,
                       WRContextType type, uint64_t tag) {
    for (size_t i = 0; i < num; ++i) {
      void *buf;
      aligned_malloc((void**) &buf, kMempoolChunkSize);
      CHECK(buf);

      ctx[i].private_data = this;
      ctx[i].tag = tag;
      PrepareContext(ctx + i, buf, kMempoolChunkSize, static_cast<char*>(buf) + kMempoolChunkSize, 0);
      queue->Push(&ctx[i]);
    }
  }

  void SetNodeID(int id) { node_id = id; }

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

  ~FabricTransport() {
    PS_VLOG(2) << "~FabricTransport";
  }

  void Send(FabricWRContext *context) {
    while (true) {
      CHECK_NE(context->private_data, nullptr) << "private data must not be nullptr";
      int ret = fi_tsendv(endpoint_->endpoint, context->buf_vec, nullptr,
                          2, endpoint_->peer_addr, context->tag, context);
      if (ret == -FI_EAGAIN) {
        LOG(WARNING) << "fi_tsend: FI_EAGAIN";
      } else if (ret != 0) {
        check_err(ret, "Unable to do fi_send message");
      } else {
        break;
      }
    }
    PS_VLOG(3) << "Posted fi_send to endpoint " << endpoint_->readable_peer_addr;
  }

  void SendRendezvousBegin(Message &msg, MessageBuffer *msg_buf) {
    FabricWRContext *context = nullptr;
    endpoint_->free_start_ctx.WaitAndPop(&context);

    RendezvousMsg *req =
        reinterpret_cast<RendezvousMsg *>(context->buf_vec[0].iov_base);
    CHECK(req != nullptr);
    req->meta_len = msg_buf->inline_len;
    req->origin_addr = reinterpret_cast<uint64_t>(msg_buf);
    req->data_num = msg_buf->data.size();
    for (size_t i = 0; i < req->data_num; ++i) {
      req->data_len[i] = msg.data[i].size();
    }
    PS_VLOG(3) << "SendRendezvousBegin " << RendezvousDebugStr(*req);
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

    // worker only needs a buffer for receving meta???? why not both????
    // size_t meta_size = is_server_ ? align_ceil(req->meta_len, pagesize_) : req->meta_len;
    size_t meta_size = align_ceil(req->meta_len, pagesize_);
    // TODO: why including all data_len[i] instead of data_len[1] ?
    size_t data_size = data_len;
    size_t alloc_size = meta_size + data_size;
    buf_ctx->buf_size = alloc_size;
    char *buffer = allocator_->Alloc(alloc_size);
    CHECK(buffer);
    buf_ctx->buffer = buffer;

    FabricWRContext *reply_ctx = nullptr;
    endpoint_->free_reply_ctx.WaitAndPop(&reply_ctx);

    RendezvousMsg *resp =
        reinterpret_cast<RendezvousMsg *>(reply_ctx->buf_vec[0].iov_base);

    resp->origin_addr = req->origin_addr;
    resp->idx = addrpool.StoreAddress(buf_ctx);
    // TODO: proper masking
    CHECK_EQ(resp->idx, resp->idx & kDataMask) << "tag out of bound";

    endpoint_->data_rx_ctx.emplace(resp->idx, new FabricWRContext());
    FabricWRContext* recv_ctx = endpoint_->data_rx_ctx[resp->idx].get();
    recv_ctx->type = kReadContext;
    CHECK_NE(endpoint_, nullptr) << "endpoint must be initialized";
    recv_ctx->tag = resp->idx;
    recv_ctx->private_data = endpoint_;
    PrepareContext(recv_ctx, buffer, meta_size, buffer + meta_size, data_size);
    endpoint_->PostRecv(recv_ctx);
    PS_VLOG(3) << "SendRendezvousReply " << RendezvousDebugStr(*resp);
    Send(reply_ctx);
  }

  int RecvPushResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    CHECK_EQ(buffer_ctx->data_num, 0);
    return 0;
  }

  void SendPullRequest(FabricWRContext *send_context) {
    // CHECK_EQ(msg_buf->mrs.size(), 0);
    Send(send_context);
  }

  void SendPushResponse(FabricWRContext *send_context) {
    // CHECK_EQ(msg_buf->mrs.size(), 0);
    Send(send_context);
  }

  void SendPushRequest(FabricWRContext *send_context) {
    Send(send_context);
  }

  void SendPullResponse(FabricWRContext *send_context) {
    Send(send_context);
  }

  int RecvPullRequest(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));

    SArray<char> vals; // add an empty sarray to pass kvapp check

    msg->data.push_back(keys);
    msg->data.push_back(vals);

    return keys.size() + vals.size();
  }

  int RecvPushRequest(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    CHECK(msg->meta.push && msg->meta.request);
    CHECK_EQ(buffer_ctx->data_num, 3);

    SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));

    uint32_t len = buffer_ctx->data_len[1];
    SArray<char> vals;
    char* cur = buffer_ctx->buffer + align_ceil((size_t) meta_len, pagesize_);
    vals.reset(cur, len, [](void *) {});  // no need to delete

    SArray<char> lens = CreateFunctionalSarray(&msg->meta.val_len, sizeof(int));
    CHECK(msg->meta.val_len == len) << "Length does not match";

    msg->data.push_back(keys);
    msg->data.push_back(vals);
    msg->data.push_back(lens);

    LOG(INFO) << "RecvPushRequest: len = " << len << " vals.size() = " << vals.size()
              << " msg->data[1].size() = " << msg->data[1].size() << " val_len = " << msg->meta.val_len;
    return keys.size() + vals.size() + lens.size();
  }

  int RecvPullResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));

    uint32_t len = buffer_ctx->data_len[1];
    SArray<char> vals;
    char* cur = buffer_ctx->buffer + align_ceil((size_t) meta_len, pagesize_);
    vals.reset(cur, len, [](void *) {});  // no need to delete

    SArray<char> lens = CreateFunctionalSarray(&msg->meta.val_len, sizeof(int));

    msg->data.push_back(keys);
    msg->data.push_back(vals);
    msg->data.push_back(lens);

    return keys.size() + vals.size() + lens.size();
  }

  SArray<char> CreateFunctionalSarray(void *value, size_t size) {
    SArray<char> sarr;
    void *p = malloc(size);
    memcpy(p, value, size);
    sarr.reset((char *) p, size, [p](void *) { free(p); });
    return sarr;
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

  virtual std::string GetType() const {
    return std::string("fabric");
  }

 protected:
  void Start(int customer_id, bool standalone) override {
    start_mu_.lock();
    should_stop_ = false;

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    LOG(INFO) << "This is a " << role;

    val = Environment::Get()->find("ENABLE_RDMA_LOG");

    start_mu_.unlock();
    zmq_ = Van::Create("zmq");
    zmq_->Start(customer_id, true);
    Van::Start(customer_id, false);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    zmq_->Stop();
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

    PS_VLOG(1) << "Clearing endpoints.";
    incoming_.clear();
    {
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      endpoints_.clear();
    }

//    PS_VLOG(1) << "Destroying cq and pd.";
//    CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
//    TODO destructor for FabricContext
  }

  int Bind(const Node &node, int max_retry) override {
    std::lock_guard<std::mutex> lk(mu_);
    InitContext();

    int my_port = zmq_->Bind(node, max_retry);
    PS_VLOG(1) << "Done zmq->Bind. My port is " << my_port;
    event_polling_thread_.reset(new std::thread(&FabricVan::PollEvents, this));

    // update zmq node info
    return my_port;
  }

  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    PS_VLOG(1) << "Connect: " << node.DebugString() << " from " << my_node_.DebugString();
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      PS_VLOG(1) << "Fabric skipped connection to node " << node.DebugString()
                 << ". My node is " << my_node_.DebugString();
      return;
    }
    if (node.id != Node::kEmpty) {
      FabricEndpoint *endpoint;
      endpoints_mu_.lock();
      auto it = endpoints_.find(node.id);
      // if there is an endpoint with pending connection
      if (it == endpoints_.end()) {
        endpoints_[node.id] = std::make_unique<FabricEndpoint>();
      }
      endpoint = endpoints_[node.id].get();
      endpoints_mu_.unlock();

      {
        std::lock_guard<std::mutex> lk(endpoint->connect_mu);
        endpoint->status = FabricEndpoint::CONNECTING;
        endpoint->SetNodeID(node.id);
      }

      const std::string remote_hostport = host_port(node.hostname, node.port);
      int recver_id = -1;
      {
        std::lock_guard<std::mutex> lk(mu_);
        // connect zmq if not done before
        if (zmq_id_map_.find(remote_hostport) == zmq_id_map_.end()) {
          zmq_id_map_[remote_hostport] = node.id;
          zmq_->Connect(node);
        }
        recver_id = zmq_id_map_[remote_hostport];
      }

      while (endpoint->status != FabricEndpoint::CONNECTED) {
        std::unique_lock<std::mutex> lk(endpoint->connect_mu);
        endpoint->status = FabricEndpoint::CONNECTING;
        // addr_request message
        Message req;
        req.meta.recver = recver_id;
        req.meta.control.cmd = Control::ADDR_REQUEST;
        Node req_info;
        req_info.role = my_node_.role;
        req_info.hostname = my_node_.hostname;
        req_info.port = my_node_.port;
        req_info.id = my_node_.id;
        // auxilary id for tracking connection requests
        req_info.aux_id = node.id;
        context_->addr.CopyTo(req_info.endpoint_name, &req_info.endpoint_name_len);
        req.meta.control.node.push_back(req_info);
        zmq_->Send(req);

        endpoint->cv.wait(lk, [endpoint] {
          return endpoint->status != FabricEndpoint::CONNECTING;
        });

        if (endpoint->status == FabricEndpoint::CONNECTED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
  }


  void StoreMsgBuf(MessageBuffer *msg_buf, Message& msg) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    CHECK_EQ(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
    msgbuf_cache_[msg_buf] = msg;
  }

  MessageBuffer* PrepareNewMsgBuf(Message& msg, FabricEndpoint *endpoint) {
    MessageBuffer *msg_buf = new MessageBuffer();
    auto meta_len = GetPackMetaLen(msg.meta);
    msg_buf->inline_len = meta_len;
    msg_buf->inline_buf = mem_allocator_->Alloc(meta_len);
    msg_buf->data = msg.data;
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);
    // prepare send context
    FabricWRContext* ctx = new FabricWRContext();
    ctx->type = kWriteContext;
    ctx->private_data = endpoint;
    CHECK_NE(endpoint, nullptr) << "endpoint must be initialized";
    msg_buf->reserved_context = ctx;
    void *data_ptr = nullptr;
    size_t data_size = 0;
    if (msg.data.size() != 0) {
      // pull response / push request : size = 3
      // push response / pull request : size = 2
      CHECK_GE(msg.data.size(), 2) << "Unexpected number of data: " << msg.data.size();
      data_ptr = static_cast<char*>(msg.data[1].data());
      data_size = msg.data[1].size();
    }
    PrepareContext(ctx, msg_buf->inline_buf, msg_buf->inline_len, data_ptr, data_size);
    return msg_buf;
  }

  std::string EndpointsDebugStr() const {
    std::stringstream ss;
    ss << "{";
    for (auto it = endpoints_.begin(); it != endpoints_.end(); it++) {
      ss << std::to_string(it->first) << ",";
    }
    ss << "}";
    return ss.str();
  }

  void AddMeta(Message &msg) {
    if (msg.meta.request) {
      msg.meta.key = DecodeKey(msg.data[0]);
    }
    if (msg.meta.push && msg.meta.request) {
      // push request
      CHECK_EQ(msg.data.size(), 3) << msg.data.size();
      auto& vals = msg.data[1];
      msg.meta.val_len = vals.size();
    }
  }

  int SendMsg(Message &msg) override {
    int remote_id = msg.meta.recver;
    PS_VLOG(3) << "SendMsg: " << msg.DebugString() << " to node " << remote_id;
    CHECK_NE(remote_id, Node::kEmpty);

    endpoints_mu_.lock();
    CHECK_NE(endpoints_.find(remote_id), endpoints_.end()) << EndpointsDebugStr();
    FabricEndpoint *endpoint = endpoints_[remote_id].get();
    endpoints_mu_.unlock();

    int meta_len = GetPackMetaLen(msg.meta);
    size_t data_len = msg.meta.data_size;
    size_t total_len = meta_len + data_len;
    CHECK(meta_len);
    CHECK(meta_len);

    // pack meta info
    if (IsValidPushpull(msg)) {
      AddMeta(msg);
    }

    auto trans = CHECK_NOTNULL(endpoint->GetTransport());

    // start rendezvous if no remote info
    if (!IsValidPushpull(msg)) {
      MessageBuffer *msg_buf = PrepareNewMsgBuf(msg, endpoint);
      StoreMsgBuf(msg_buf, msg);
      trans->SendRendezvousBegin(msg, msg_buf);
      return total_len;
    } else {
      auto is_push = msg.meta.push;
      auto key = msg.meta.key;
      // TODO: implement caching
      //if (!HasRemoteInfo(msg, key, is_push, remote_id)) {
        MessageBuffer *msg_buf = PrepareNewMsgBuf(msg, endpoint);
        StoreMsgBuf(msg_buf, msg);
        // PrepareData(msg, msg_buf);
        trans->SendRendezvousBegin(msg, msg_buf);
        return total_len;
      //}
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

    PrintRecvLog(msg, buffer_ctx, meta_len);

    if (!IsValidPushpull(*msg)) {
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

    return total_len;
  }

  void PrintRecvLog(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    std::lock_guard<std::mutex> lock(log_mu_);

    if (!IsValidPushpull(*msg)) {
      PS_VLOG(4) << "Recv Control Message" << std::flush;
    } else if (msg->meta.push && msg->meta.request) {
      // push request
      PS_VLOG(4) << "Recv Push Request: key=" << msg->meta.key
          << "\t timestamp=" << msg->meta.timestamp
          << "\t sender=" << msg->meta.sender
          << "\t tensor_len=" << buffer_ctx->data_len[1]
          << std::flush;
    } else if (!msg->meta.push && msg->meta.request) {
      // pull request
      PS_VLOG(4) << "Recv Pull Request: key=" << msg->meta.key
          << "\t timestamp=" << msg->meta.timestamp
          << "\t sender=" << msg->meta.sender
          << std::flush;
    } else if (msg->meta.push && !msg->meta.request) {
      // push response
      PS_VLOG(4) << "Recv Push Response: key=" << msg->meta.key
          << "\t timestamp=" << msg->meta.timestamp
          << "\t sender=" << msg->meta.sender
          << std::flush;
    } else if (!msg->meta.push && !msg->meta.request) {
      // pull response
      PS_VLOG(4) << "Recv Pull Response: key=" << msg->meta.key
          << "\t timestamp=" << msg->meta.timestamp
          << "\t sender=" << msg->meta.sender
          << "\t tensor_len=" << msg->meta.val_len;
    }
  }

  inline void SetNode(const Node& node) {
    my_node_ = node;
    zmq_->SetNode(node);
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
    PS_VLOG(3) << "Initializing a fabric endpoint";
    context_ = std::unique_ptr<FabricContext>(new FabricContext());
    CHECK(context_ != nullptr) << "Failed to allocate Endpoint";
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
      case kReadContext:
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

  void HandleCQError(struct fid_cq *cq) {
    struct fi_cq_err_entry err_entry;
    int ret = fi_cq_readerr(cq, &err_entry, 1);
    if (ret == FI_EADDRNOTAVAIL) {
      LOG(WARNING) << "fi_cq_readerr: FI_EADDRNOTAVAIL";
    } else if (ret < 0) {
      LOG(FATAL) << "fi_cq_readerr failed. Return Code: " << ret
                 << ". ERROR: "
                 << fi_cq_strerror(cq, err_entry.prov_errno,
                                   err_entry.err_data, nullptr,
                                   err_entry.err_data_size);
    } else {
      check_err(-err_entry.err, "fi_cq_read failed. retrieved error: ");
    }
  }

  std::string CQDebugStr(const struct fi_cq_tagged_entry& cq_entry) const {
    uint64_t flags = cq_entry.flags;
    bool is_tagged = flags & FI_TAGGED;
    bool is_send = flags & FI_SEND;
    bool is_recv = flags & FI_RECV;
    uint64_t tag = cq_entry.tag;
    std::stringstream ss;
    ss << "op_context = " << cq_entry.op_context
       << ", flags = " << flags;
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

  void ReleaseFirstMsg(MessageBuffer *msg_buf) {
    std::lock_guard<std::mutex> lk(addr_mu_);
    CHECK_NE(msgbuf_cache_.find(msg_buf), msgbuf_cache_.end());
    msgbuf_cache_.erase(msg_buf);
  }


  void HandleCompletionEvent(const struct fi_cq_tagged_entry& cq_entry) {
    uint64_t flags = cq_entry.flags;
    bool is_tagged = flags & FI_TAGGED;
    bool is_send = flags & FI_SEND;
    bool is_recv = flags & FI_RECV;
    CHECK(is_tagged) << "Completion events must be tagged";
    uint64_t tag = cq_entry.tag;

    // op_context is the local context
    PS_VLOG(4) << CQDebugStr(cq_entry);
    FabricWRContext *context = static_cast<FabricWRContext *>(cq_entry.op_context);
    CHECK_NE(context, nullptr) << "FabricWRContext should not be null: " << context;
    FabricEndpoint *endpoint = static_cast<FabricEndpoint *>(context->private_data);
    CHECK_NE(endpoint, nullptr) << "FabricEndpoint should not be null: "
                                << endpoint << ". Context = " << context << " " << WRContextDebugStr(*context);
    // Rendezvous messages
    CHECK_EQ(context->buf_vec[0].iov_base, cq_entry.buf) << "buffer address does not match";
    bool start_msg = tag == kRendezvousStartMask;
    bool reply_msg = tag == kRendezvousReplyMask;
    // TODO: proper masking
    bool data_msg = not start_msg and not reply_msg;
    if (is_send) {
      if (start_msg || reply_msg) {
        ReleaseWRContext(context, endpoint);
        RendezvousMsg *req = static_cast<RendezvousMsg*>(context->buf_vec[0].iov_base);
        PS_VLOG(3) << "DONE FI_SEND " << RendezvousDebugStr(*req);
      } else {
        PS_VLOG(3) << "DONE FI_SEND data";
      }
    } else if (is_recv) {
      // receive
      if (start_msg) {
        // kRendezvousStart
        RendezvousMsg *req = static_cast<RendezvousMsg*>(context->buf_vec[0].iov_base);
        auto trans = CHECK_NOTNULL(endpoint->GetTransport());
    
        PS_VLOG(3) << "DONE FI_RECV " << RendezvousDebugStr(*req);
        trans->SendRendezvousReply(req, addr_pool_);
        ReleaseWRContext(context, endpoint);
      } else if (data_msg) {
        // the tag is the address of the buffer context posted for recving
        BufferContext *buf_ctx = addr_pool_.GetAddress(tag);
        recv_buffers_.Push(buf_ctx);
        buf_ctx->endpoint->PostRecv(context);
        PS_VLOG(3) << "DONE FI_RECV tagged: " << tag;
      } else {
        RendezvousMsg *req = static_cast<RendezvousMsg*>(context->buf_vec[0].iov_base);

        // kRendezvousReply
        uint64_t origin_addr = req->origin_addr;
        // idx used to retrieve prepared context
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
        FabricWRContext* send_context = msg_buf->reserved_context;
        send_context->tag = idx;

        auto trans = CHECK_NOTNULL(endpoint->GetTransport());
        if (!IsValidPushpull(*msg)) {
          // control message
          trans->Send(send_context);
        } else if (msg->meta.push && msg->meta.request) {
          // worker, push request
          trans->SendPushRequest(send_context);
        } else if (msg->meta.push && !msg->meta.request) {
          // server, push response
          trans->SendPushResponse(send_context);
        } else if (!msg->meta.push && msg->meta.request) {
          // worker, pull request
          trans->SendPullRequest(send_context);
        } else if (!msg->meta.push && !msg->meta.request) {
          // server, pull response
          // map_mu_.lock();
          // auto temp_mr = mem_mr_.find(msg_buf->data[1].data());
          // CHECK_NE(temp_mr, mem_mr_.end());
          // map_mu_.unlock();
          // trans->SendPullResponse(*msg, msg_buf, addr_tuple, temp_mr->second->lkey);
          trans->SendPushResponse(send_context);
        } else {
          LOG(FATAL) << "unknown message type";
        }

        // release the msg_buf from msgbuf_cache_
        ReleaseFirstMsg(msg_buf);

        PS_VLOG(3) << "DONE FI_RECV kRendezvousReply";
        ReleaseWRContext(context, endpoint);
      }
    } else {
      LOG(FATAL) << "unknown completion entry" << flags;
    }
  }

  void PollCQ() {
    // Pre-allocated work completions array used for polling
    struct fi_cq_tagged_entry cq_entries[kMaxConcurrentWorkRequest];
    while (!should_stop_.load()) {
      int ret = fi_cq_read(context_->cq, cq_entries, kMaxConcurrentWorkRequest);
      if (ret == -FI_EAGAIN) {
        continue;
      } else if (ret == -FI_EAVAIL) {
        HandleCQError(context_->cq);
      } else if (ret < 0) {
        check_err(ret, "fi_cq_read failed");
      } else {
        CHECK_NE(ret, 0) << "at least one completion event is expected";
        PS_VLOG(3) << ret << " completion events ... ";
        for (int i = 0; i < ret; ++i) {
          const auto& cq_entry = cq_entries[i];
          HandleCompletionEvent(cq_entry);
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
      PS_VLOG(3) << "zmq recv: " << msg.DebugString();
      CHECK(!msg.meta.control.empty()) << "msg.meta.control is empty";
      auto &ctrl = msg.meta.control;
      if (ctrl.cmd == Control::ADDR_REQUEST) {
        OnConnectRequest(msg);
      } else if (ctrl.cmd == Control::ADDR_RESOLVED) {
        OnConnected(msg);
      } else if (ctrl.cmd == Control::TERMINATE) {
        should_stop_ = true;
      } else {
        LOG(FATAL) << "Drop unknown typed message " << msg.DebugString();
      }
    }
  }

  void OnConnectRequest(const Message &msg) {
    const auto& req_info = msg.meta.control.node[0];
    Node addr_info;
    FabricAddr src_addr;
    src_addr.CopyFrom(req_info.endpoint_name, req_info.endpoint_name_len);

    // the zmq_sender_id is NOT the actual node id
    int zmq_sender_id;
    const std::string req_hostport = host_port(req_info.hostname, req_info.port);
    PS_VLOG(3) << "Handling connection request " << req_info.DebugString() << ". " << req_hostport;
    {
      std::lock_guard<std::mutex> lk(mu_);
      // not connected before
      if (zmq_id_map_.find(req_hostport) == zmq_id_map_.end()) {
        zmq_sender_id = 30000 + zmq_id_map_.size();
        // connect to the remote node
        Node conn_node;
        conn_node.id = zmq_sender_id;
        conn_node.hostname = req_info.hostname;
        conn_node.port = req_info.port;
        zmq_->Connect(conn_node);
        zmq_id_map_[req_hostport] = zmq_sender_id;
      }
      zmq_sender_id = zmq_id_map_[req_hostport];
    }
    context_->addr.CopyTo(addr_info.endpoint_name, &addr_info.endpoint_name_len);
    addr_info.hostname = my_node_.hostname;
    addr_info.port = my_node_.port;
    addr_info.aux_id = req_info.aux_id;

    Message reply;
    reply.meta.recver = zmq_sender_id;
    reply.meta.control.cmd = Control::ADDR_RESOLVED;
    reply.meta.control.node.push_back(addr_info);
    zmq_->Send(reply);

    FabricEndpoint *endpoint;
    // create the endpoint if it does not exist
    if (incoming_.find(req_hostport) == incoming_.end()) {
      incoming_.emplace(req_hostport, std::make_unique<FabricEndpoint>());
    }
    endpoint = incoming_[req_hostport].get();

    // initialize the endpoint
    if (!endpoint->initialized) {
      endpoint->Init(src_addr.name, context_->av, context_->ep,
                     req_hostport, mem_allocator_.get());
    }
    // set endpoint node id
    if (req_info.id != Node::kEmpty) {
      endpoint->SetNodeID(req_info.id);
      PS_VLOG(3) << "Updated Endpoint " << req_hostport << " with ID = " << req_info.id;
    }
    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&FabricVan::PollCQ, this));
    }
  }

  void OnConnected(const Message &msg) {
    const auto& addr_info = msg.meta.control.node[0];
    struct FabricAddr sender_addr;
    sender_addr.CopyFrom(addr_info.endpoint_name, addr_info.endpoint_name_len);
    const int sender_id = addr_info.aux_id;
    const std::string hostport = host_port(addr_info.hostname, addr_info.port);

    PS_VLOG(3) << "Handling connected reply" << addr_info.DebugString();
    // retrieve and init endpoint
    FabricEndpoint *endpoint = endpoints_[sender_id].get();
    CHECK(endpoint) << "Endpoint not found.";
    if (!endpoint->initialized) {
      endpoint->Init(sender_addr.name, context_->av, context_->ep,
                     hostport, mem_allocator_.get());
    }
    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&FabricVan::PollCQ, this));
    }

    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = FabricEndpoint::CONNECTED;
    }
    endpoint->cv.notify_all();
  }

  AddressPool<BufferContext> addr_pool_;
  std::unique_ptr<FabricMemoryAllocator> mem_allocator_;

  std::atomic<bool> should_stop_;

  std::mutex endpoints_mu_;
  // node id -> endpoint for outgoing connections
  std::unordered_map<int, std::unique_ptr<FabricEndpoint>> endpoints_;
  // host:port -> endpoint for incoming connections
  std::unordered_map<std::string, std::unique_ptr<FabricEndpoint>> incoming_;

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

  // macros for key_meta_map
  using MetaInfo = std::tuple<int, uint64_t, int>; // len, addr, rkey
  using SenderMeta = std::unordered_map<int, MetaInfo>; // sender as the key
  // (key, sender) --> MetaInfo
  std::unordered_map<ps::Key, SenderMeta> key_meta_map_;
  // a static address for the key
  std::unordered_map<ps::Key, ps::Key> key_addr_map_;
  // a static address for the length
  std::unordered_map<ps::Key, int> key_len_map_;
  // mutex for memory map
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
  // we use zmq_id_map_ to map host:port to IDs. The ID can be arbtrary, as long
  // as the id is unique.
  std::unordered_map<std::string, int> zmq_id_map_;
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

  // for data. But for libfabric, we change the type
  std::unordered_map<char*, struct fid_mr*> mem_mr_; // (memory address, fid_mr)
  std::mutex log_mu_;
};  // namespace ps
};  // namespace ps

//#endif  // DMLC_USE_RDMA
#endif  // PS_FABRIC_VAN_H_
