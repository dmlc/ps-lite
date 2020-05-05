/**
 * Copyright (c) 2020 by Contributors
 * Authors: access2rohit@gmail.com (Rohit Srivastava)
 *          linhaibin.eric@gmail.com (Haibin Lin)
*/
#ifndef PS_FABRIC_VAN_H_
#define PS_FABRIC_VAN_H_

#ifdef DMLC_USE_FABRIC

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <zmq.h>
#include <sys/uio.h>

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
static const int kRxDepth = 2048; // should be larger than kStartDepth
// static const int kStartDepth = 2;
// static const int kRxDepth = 4; // should be larger than kStartDepth
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
  // length of the meta data
  uint64_t meta_len;
  // msg.data.size()
  uint64_t data_num;
  // msg.data[i].size()
  uint64_t data_len[kMaxDataFields];
 // the original address of the message buffer
  uint64_t origin_addr;
 // the tag for tsend / trestatus_cv. Used for Rendezvous reply
  uint64_t tag;
};

std::string RendezvousDebugStr(const RendezvousMsg& msg) {
  std::stringstream ss;
  ss << "meta_len = " << msg.meta_len
     << ", data_num = " << msg.data_num
     << ", data_len = " << msg.data_len[0]
     << ", origin_addr = " << msg.origin_addr
     << ", tag = " << msg.tag;
  return ss.str();
}

struct BufferContext {
  // the buffer (including meta and val)
  char *buffer;
  // size of msg.meta
  size_t meta_len;
  // msg.data.size()
  size_t data_num;
  // msg.data[i].size()
  size_t data_len[kMaxDataFields];
  // the remote endpoint
  FabricEndpoint *endpoint;
};

enum WRContextType {
  // send rendezvous start
  kSendRendStart,
  // send rendezvous reply
  kSendRendReply,
  // recv rendezvous msgs
  kReceiveRend,
  // send with data msg
  kSendWithData,
  // recv with data msg
  kReceiveWithData
};

struct FabricWRContext {
  // type of the WR context
  WRContextType type;
  // libfabric tag
  uint64_t tag;
  // send/recv buffers:
  // buffers[0] for meta, buffers[1] for data
  struct iovec buffers[2];
  // number of send/recv buffers
  size_t num_buffers;
  // private msg_buf point
  void* private_data;
};

std::string WRContextDebugStr(const FabricWRContext& ctx) {
  std::stringstream ss;
  ss << "type = " << ctx.type
     << ", tag = " << ctx.tag
     << ", num_buffers = " << ctx.num_buffers;
  for (size_t i = 0; i < ctx.num_buffers; i++) {
     ss << ", buffers[" << i << "].iov_base = " << ctx.buffers[i].iov_base
        << ", buffers[" << i << "].iov_len = " << ctx.buffers[i].iov_len;
  }
  return ss.str();
}

std::string hostport_str(const std::string& host, const int port) {
  return host + ":" + std::to_string(port);
}

void PrepareWRContext(FabricWRContext* context, void *meta_buff, size_t meta_size,
                      void *data_buff, size_t data_size) {
  context->buffers[0].iov_base = meta_buff;
  context->buffers[0].iov_len = meta_size;
  if (data_size != 0) {
    context->num_buffers = 2;
    context->buffers[1].iov_base = data_buff;
    context->buffers[1].iov_len = data_size;
  } else {
    context->num_buffers = 1;
  }
}

struct MessageBuffer {
  // inline meta data size
  size_t inline_len;
  // inline meta data
  char *inline_buf;
  // WR context
  FabricWRContext *reserved_context;
  // msg.data
  std::vector<SArray<char>> data;
};

static_assert(std::is_pod<RendezvousMsg>::value,
              "RendezvousMsg must be a POD type.");

static const size_t kMempoolChunkSize = sizeof(RendezvousMsg);

class FabricMemoryAllocator {
 public:
  explicit FabricMemoryAllocator() {
    PS_VLOG(4) << "aligned to pagesize " << pagesize_;
  }

  ~FabricMemoryAllocator() {}

  char *Alloc(size_t size) {
    if (size == 0) {
      return nullptr;
    }
    // align to page size (usually 4KB)
    size = align_ceil(size, pagesize_);

    char *p;
    aligned_malloc((void**) &p, size);
    CHECK(p);
    return p;
  }

  size_t pagesize_ = sysconf(_SC_PAGESIZE);
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

  void Init(fi_info* info) {
    struct fi_cq_attr cq_attr = {};
    struct fi_av_attr av_attr = {};

    // fi_fabric: create fabric
    int ret = fi_fabric(info->fabric_attr, &fabric, nullptr);
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
    PS_VLOG(3) << "Endpoint created: " << addr.DebugStr()
               << " readable endpoint = "
               << std::string(readable_addr.name, readable_addr.len);
  }

  ~FabricContext() {
    PS_VLOG(3) << "~FabricContext";
    fi_close((fid_t) ep);
    fi_close((fid_t) cq);
    fi_close((fid_t) av);
    fi_close((fid_t) domain);
    fi_close((fid_t) fabric);
  }
};

struct FabricEndpoint {
  enum ConnectionStatus { EMPTY, IDLE, CONNECTING, CONNECTED, REJECTED };
  ConnectionStatus status = EMPTY;
  // the node id
  int node_id = Node::kEmpty;
  // the connection id
  int connection_id = -1;
  // host:port
  std::string hostport;
  // mutex for connection
  std::mutex status_mu;
  // mutex for initialization
  std::mutex addr_mu;
  // cv for connection status
  std::condition_variable status_cv;
  // cv for peer address
  std::condition_variable addr_cv;
  // the name of the peer endpoint
  FabricAddr peer_ep_name;
  // the endpoint name of the remote peer
  fi_addr_t peer_addr;
  // the readable endpoint name of the remote peer
  std::string readable_peer_addr;
  // whether the peer address is set
  bool peer_addr_ready = false;
  // fabric context
  std::unique_ptr<FabricContext> fabric_ctx;
  // the mem allocator
  std::unique_ptr<FabricMemoryAllocator> mem_allocator;
  // address pool
  AddressPool<BufferContext> addr_pool;

  // the transport
  std::shared_ptr<FabricTransport> trans;

  // send ctxs
  FabricWRContext start_tx_ctx[kStartDepth];
  FabricWRContext reply_tx_ctx[kReplyDepth];

  // receive ctxs
  FabricWRContext start_rx_ctx[kRxDepth / 2];
  FabricWRContext reply_rx_ctx[kRxDepth / 2];

  // queues for send ctxs
  ThreadsafeQueue<FabricWRContext *> free_start_ctx;
  ThreadsafeQueue<FabricWRContext *> free_reply_ctx;

  // queues for send requests
  ThreadsafeQueue<Message> send_queue;

  std::unordered_map<MessageBuffer*, Message> msgbuf_cache; // msg_buf, msg

  typedef std::tuple<FabricWRContext*, MessageBuffer*> CtxTuple;

  // key -> [<FabricWRContext*, MessageBuffer*>,]
  std::unordered_map<uint64_t, std::queue<CtxTuple>> push_context;
  std::unordered_map<uint64_t, std::queue<CtxTuple>> pull_context;

  ~FabricEndpoint() {
    PS_VLOG(3) << "~FabricEndpoint";
  }

  void SetNodeID(int id) { node_id = id; }

  std::shared_ptr<FabricTransport> GetTransport() { return trans; }

  void SetPeerAddr(const char* ep_name, const size_t ep_name_len) {
    {
      std::lock_guard<std::mutex> lk(addr_mu);
      if (!peer_addr_ready) {
        peer_ep_name.CopyFrom(ep_name, ep_name_len);
        peer_addr_ready = true;
      }
    }
    addr_cv.notify_all();
  }

  void Create(const std::string& hp, fi_info* info) {
    fabric_ctx = std::unique_ptr<FabricContext>(new FabricContext());
    CHECK(fabric_ctx != nullptr);
    fabric_ctx->Init(info);
    mem_allocator.reset(new FabricMemoryAllocator());
    hostport = hp;
  }

  void InitPeerAddr() {
    // fi_av_insert: insert address vector of the remote endpoint
    int ret = fi_av_insert(fabric_ctx->av, peer_ep_name.name, 1, &peer_addr, 0, nullptr);
    CHECK_EQ(ret, 1) << "Call to fi_av_insert() failed. Return Code: "
                     << ret << ". ERROR: " << fi_strerror(-ret);
    // fi_av_straddr: human readable name
    FabricAddr readable_addr;
    fi_av_straddr(fabric_ctx->av, peer_ep_name.name, readable_addr.name, &readable_addr.len);
    readable_peer_addr = std::string(readable_addr.name, readable_addr.len);
    PS_VLOG(3) << "Peer endpoint connected: " << readable_peer_addr;
  }

  void Init() {
    // set the peer addr
    InitPeerAddr();
    // set transport
    trans = std::make_shared<FabricTransport>(this, mem_allocator.get());
    // set contexts
    InitSendContext(start_tx_ctx, &free_start_ctx, kStartDepth,
                    kSendRendStart, kRendezvousStartMask);
    InitSendContext(reply_tx_ctx, &free_reply_ctx, kReplyDepth,
                    kSendRendReply, kRendezvousReplyMask);
    InitRecvContext(start_rx_ctx, kRxDepth / 2,
                    kReceiveRend, kRendezvousStartMask);
    InitRecvContext(reply_rx_ctx, kRxDepth / 2,
                    kReceiveRend, kRendezvousReplyMask);
    PS_VLOG(3) << "Endpoint initialized";
  }

  void PostRecv(FabricWRContext *ctx) {
    while (true) {
      int ret = fi_trecvv(fabric_ctx->ep, ctx->buffers, nullptr, ctx->num_buffers,
                          peer_addr, ctx->tag, 0, static_cast<void *>(ctx));
      if (ret == -FI_EAGAIN) {
        // no resources
        LOG(WARNING) << "fi_trecv: FI_EAGAIN";
        continue;
      } else if (ret != 0) {
        check_err(ret, "Unable to do fi_recv message");
      }
      break;
    }
    PS_VLOG(4) << "Posted recv buffer " << ctx->buffers << " for "
               << node_id << ". size = " << ctx->num_buffers
               << " ctx = " << static_cast<void *>(ctx) << " tag = " << ctx->tag;
  }

  void InitRecvContext(FabricWRContext *ctx, size_t num,
                       WRContextType type, uint64_t tag) {
    for (size_t i = 0; i < num; ++i) {
      void *buf;
      aligned_malloc((void**) &buf, kMempoolChunkSize);
      CHECK(buf);

      ctx[i].tag = tag;
      ctx[i].type = type;
      PS_VLOG(4) << "InitRecvContext " << i << "/" << num;
      PrepareWRContext(ctx + i, buf, kMempoolChunkSize,
                       static_cast<char*>(buf) + kMempoolChunkSize, 0);
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

      ctx[i].tag = tag;
      ctx[i].type = type;
      PrepareWRContext(ctx + i, buf, kMempoolChunkSize,
                       static_cast<char*>(buf) + kMempoolChunkSize, 0);
      queue->Push(&ctx[i]);
    }
  }

  bool HasRemoteInfo(uint64_t key, bool is_push) {
    if (is_push) return !push_context[key].empty();
    if (!is_push) return !pull_context[key].empty();
    return false;
  }

  void StoreRemoteContext(MessageBuffer *msg_buf, FabricWRContext* ctx) {
    CHECK_NE(msgbuf_cache.find(msg_buf), msgbuf_cache.end());
    auto& msg = msgbuf_cache[msg_buf];
    auto key = msg.meta.key;
    auto is_push = msg.meta.push;
    auto& queue = is_push ? push_context[key] : pull_context[key];
    queue.emplace(std::make_pair(ctx, msg_buf));
  }

  CtxTuple GetRemoteContext(uint64_t key, bool is_push) {
    auto& queue = is_push ? push_context[key] : pull_context[key];
    CHECK(!queue.empty());
    CtxTuple tuple = queue.front();
    queue.pop();
    return tuple;
  }

  void StoreMsgBuf(MessageBuffer *msg_buf, Message& msg) {
    CHECK_EQ(msgbuf_cache.find(msg_buf), msgbuf_cache.end());
    msgbuf_cache[msg_buf] = msg;
  }

  void ReleaseFirstMsg(MessageBuffer *msg_buf) {
    CHECK_NE(msgbuf_cache.find(msg_buf), msgbuf_cache.end());
    PS_VLOG(6) << "Release msg_buf " << msg_buf;
    msgbuf_cache.erase(msg_buf);
  }
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
    PS_VLOG(3) << "~FabricTransport";
  }

  void Send(FabricWRContext *context) {
    while (true) {
      int ret = fi_tsendv(endpoint_->fabric_ctx->ep, context->buffers, nullptr,
                          2, endpoint_->peer_addr, context->tag, context);
      if (ret == -FI_EAGAIN) {
        LOG(WARNING) << "fi_tsend: FI_EAGAIN";
      } else if (ret != 0) {
        check_err(ret, "Unable to do fi_send message");
      } else {
        break;
      }
    }
    PS_VLOG(3) << "Posted fi_send to endpoint " << endpoint_->node_id
               << " tag = " << context->tag;
  }

  void SendRendezvousBegin(Message &msg, MessageBuffer *msg_buf) {
    FabricWRContext *context = nullptr;
    endpoint_->free_start_ctx.WaitAndPop(&context);

    RendezvousMsg *req =
        reinterpret_cast<RendezvousMsg *>(context->buffers[0].iov_base);
    CHECK(req != nullptr);
    req->meta_len = msg_buf->inline_len;
    req->origin_addr = reinterpret_cast<uint64_t>(msg_buf);
    req->data_num = msg_buf->data.size();
    for (size_t i = 0; i < req->data_num; ++i) {
      req->data_len[i] = msg.data[i].size();
    }
    Send(context);
    PS_VLOG(3) << "SendRendezvousBegin " << RendezvousDebugStr(*req);
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

    size_t meta_size = align_ceil(req->meta_len, pagesize_);
    // TODO: why including all data_len[i]?
    size_t data_size = data_len;
    size_t alloc_size = meta_size + data_size;
    char *buffer = allocator_->Alloc(alloc_size);
    CHECK(buffer);
    buf_ctx->buffer = buffer;

    FabricWRContext *reply_ctx = nullptr;
    endpoint_->free_reply_ctx.WaitAndPop(&reply_ctx);

    RendezvousMsg *resp =
        reinterpret_cast<RendezvousMsg *>(reply_ctx->buffers[0].iov_base);

    resp->origin_addr = req->origin_addr;
    resp->tag = addrpool.StoreAddress(buf_ctx);
    CHECK_EQ(resp->tag, resp->tag & kDataMask) << "tag out of bound";

    FabricWRContext* recv_ctx = new FabricWRContext();
    recv_ctx->type = kReceiveWithData;
    CHECK_NE(endpoint_, nullptr);
    recv_ctx->tag = resp->tag;
    PrepareWRContext(recv_ctx, buffer, meta_size, buffer + meta_size, data_size);
    endpoint_->PostRecv(recv_ctx);
    Send(reply_ctx);
    PS_VLOG(3) << "SendRendezvousReply " << RendezvousDebugStr(*resp);
  }

  int RecvPushResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    CHECK_EQ(buffer_ctx->data_num, 0);
    return 0;
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
    SArray<char> lens = CreateFunctionalSarray(&len, sizeof(uint32_t));

    msg->data.push_back(keys);
    msg->data.push_back(vals);
    msg->data.push_back(lens);

    return keys.size() + vals.size() + lens.size();
  }

  int RecvPullResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));
    uint32_t len = buffer_ctx->data_len[1];
    SArray<char> vals;
    char* cur = buffer_ctx->buffer + align_ceil((size_t) meta_len, pagesize_);
    vals.reset(cur, len, [](void *) {});  // no need to delete
    SArray<char> lens = CreateFunctionalSarray(&len, sizeof(int));

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
    struct fi_info* providers = info_;
    while (providers) {
      LOG(INFO) << "Found a fabric provider " << providers->fabric_attr->prov_name;
      providers = providers->next;
    }
  }

  ~FabricVan() {
    PS_VLOG(3) << "~FabricVan";
  }

  virtual std::string GetType() const {
    return std::string("lffabric");
  }

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
      for (auto& t : workers_) {
        t.second->join();
      }
      // endpoint must be cleared after thread->join
      worker_endpoints_.clear();
    }
    // free fabric info
    fi_freeinfo(info_);
  }

  int Bind(const Node &node, int max_retry) override {
    std::lock_guard<std::mutex> lk(endpoints_mu_);
    int my_port = zmq_->Bind(node, max_retry);
    PS_VLOG(3) << "Done zmq->Bind. My port is " << my_port;
    event_polling_thread_.reset(new std::thread(&FabricVan::EventPollingThread, this));
    return my_port;
  }

  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    PS_VLOG(3) << "Connect: " << node.DebugString() << " from " << my_node_.DebugString();
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      PS_VLOG(3) << "Fabric skipped connection to " << node.DebugString()
                 << ". My node is " << my_node_.DebugString();
      return;
    }
    if (node.id != Node::kEmpty) {
      FabricEndpoint *endpoint = GetOrCreateEndpoint(node.hostname, node.port, node.role);

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
    CHECK_NE(endpoints_.find(remote_id), endpoints_.end()) << remote_id << " v.s. " << EndpointsDebugStr();
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

  inline void SetNode(const Node& node) {
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

  MessageBuffer* PrepareNewMsgBuf(Message& msg, FabricEndpoint *endpoint) {
    MessageBuffer *msg_buf = new MessageBuffer();
    auto meta_len = GetPackMetaLen(msg.meta);
    msg_buf->inline_len = meta_len;
    msg_buf->inline_buf = endpoint->mem_allocator->Alloc(meta_len);
    msg_buf->data = msg.data;
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);

    // prepare send context
    FabricWRContext* ctx = new FabricWRContext();
    ctx->type = kSendWithData;
    msg_buf->reserved_context = ctx;
    ctx->private_data = msg_buf;
    void *data_buff = nullptr;
    size_t data_size = 0;
    if (msg.data.size() != 0) {
      // pull response / push request : size = 3
      // push response / pull request : size = 2
      CHECK_GE(msg.data.size(), 2) << "Unexpected number of data: " << msg.data.size();
      data_buff = static_cast<char*>(msg.data[1].data());
      data_size = msg.data[1].size();
    }
    PrepareWRContext(ctx, msg_buf->inline_buf, msg_buf->inline_len, data_buff, data_size);
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
        endpoint->StoreRemoteContext(reinterpret_cast<MessageBuffer *>(context->private_data), context);
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
      LOG(FATAL) << "fi_cq_readerr failed. Return Code: " << ret
                 << ". ERROR: "
                 << fi_cq_strerror(cq, err_entry.prov_errno,
                                   err_entry.err_data, nullptr,
                                   err_entry.err_data_size);
    } else {
      check_err(-err_entry.err, "fi_cq_read failed. retrieved error: ");
    }
  }

  std::string EntryDebugStr(const struct fi_cq_tagged_entry& cq_entry) const {
    uint64_t flags = cq_entry.flags;
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

  void HandleCompletionEvent(const struct fi_cq_tagged_entry& cq_entry, FabricEndpoint *endpoint) {
    uint64_t flags = cq_entry.flags;
    bool is_tagged = flags & FI_TAGGED;
    bool is_send = flags & FI_SEND;
    bool is_recv = flags & FI_RECV;
    CHECK(is_tagged) << "Completion events must be tagged";
    uint64_t tag = cq_entry.tag;

    // op_context is the local context
    PS_VLOG(5) << EntryDebugStr(cq_entry);
    FabricWRContext *context = static_cast<FabricWRContext *>(cq_entry.op_context);
    CHECK_NE(context, nullptr) << "FabricWRContext should not be null: " << context;

    CHECK_EQ(context->buffers[0].iov_base, cq_entry.buf) << "buffer address does not match";
    bool start_msg = (tag == kRendezvousStartMask);
    bool reply_msg = (tag == kRendezvousReplyMask);
    bool data_msg = !start_msg && !reply_msg;
    if (is_send) {
      if (start_msg || reply_msg) {
        RendezvousMsg *req = static_cast<RendezvousMsg*>(context->buffers[0].iov_base);
        PS_VLOG(3) << "CQ: START FI_SEND RendezvousMsg <endpoint,tag> = <" << endpoint->node_id << "," << tag << "> content = " << "start = " << start_msg << " " << RendezvousDebugStr(*req);
        ReleaseWRContext(context, endpoint);
        PS_VLOG(3) << "CQ: DONE  FI_SEND RendezvousMsg <endpoint,tag> = <" << endpoint->node_id << "," << tag << "> content = " << "start = " << start_msg << " " << RendezvousDebugStr(*req);
      } else {
        PS_VLOG(3) << "CQ: START FI_SEND data: <endpoint,tag> = <" << endpoint->node_id << "," << tag << ">";
        ReleaseWRContext(context, endpoint);
        PS_VLOG(3) << "CQ: DONE  FI_SEND data: <endpoint,tag> = <" << endpoint->node_id << "," << tag << ">";
      }
    } else if (is_recv) {
      // receive
      if (start_msg) {
        // kRendezvousStart
        RendezvousMsg *req = static_cast<RendezvousMsg*>(context->buffers[0].iov_base);
        auto trans = CHECK_NOTNULL(endpoint->GetTransport());
        PS_VLOG(3) << "CQ: START FI_RECV RendezvousStart <endpoint,tag> = <" << endpoint->node_id << "," << tag << "> content = " << RendezvousDebugStr(*req);
        trans->SendRendezvousReply(req, endpoint->addr_pool);
        ReleaseWRContext(context, endpoint);
        PS_VLOG(3) << "CQ: DONE  FI_RECV RendezvousStart <endpoint,tag> = <" << endpoint->node_id << "," << tag << "> content = " << RendezvousDebugStr(*req);
      } else if (data_msg) {
        // the tag is the address of the buffer context posted for recving
        PS_VLOG(3) << "CQ: START FI_RECV data <endpoint,tag> = <" << endpoint->node_id << "," << tag << ">";
        BufferContext *buf_ctx = endpoint->addr_pool.GetAddress(tag);
        recv_buffers_.Push(buf_ctx);
        ReleaseWRContext(context, buf_ctx->endpoint);
        PS_VLOG(3) << "CQ: DONE  FI_RECV data <endpoint,tag> = <" << endpoint->node_id << "," << tag << ">";
      } else {
        RendezvousMsg *req = static_cast<RendezvousMsg*>(context->buffers[0].iov_base);

        // kRendezvousReply
        uint64_t origin_addr = req->origin_addr;
        MessageBuffer *msg_buf =
    	reinterpret_cast<MessageBuffer *>(origin_addr);
        CHECK(msg_buf != nullptr);

        FabricWRContext* send_context = msg_buf->reserved_context;
        // now we know which tag to use to reach the peer
        send_context->tag = req->tag;
        PS_VLOG(3) << "CQ: START FI_RECV kRendezvousReply: <endpoint,tag> = <" << endpoint->node_id << "," << tag << "> origin_addr = " << origin_addr << " req = " << req << " msg_buf = " << msg_buf;

        endpoint->StoreRemoteContext(msg_buf, send_context);

        // PrintSendLog(*msg, msg_buf, addr_tuple);

        auto trans = CHECK_NOTNULL(endpoint->GetTransport());
        trans->Send(send_context);

        ReleaseWRContext(context, endpoint);
        PS_VLOG(3) << "CQ: DONE  FI_RECV kRendezvousReply: <endpoint,tag> = <" << endpoint->node_id << "," << tag << "> origin_addr = " << origin_addr << " req = " << req << " msg_buf = " << msg_buf;
      }
    } else {
      LOG(FATAL) << "unknown completion entry" << flags;
    }
  }

  FabricEndpoint* WorkerCreateEndpoint(const std::string hostname, const int port,
                                       const Node::Role role, fi_info* info) {
    FabricEndpoint* endpoint = nullptr;
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

  void WorkerInitPeerAddr(FabricEndpoint* endpoint) {
    // wait for peer addr
    PS_VLOG(4) << "wait peer addr info to initialize the endpoint";
    {
      std::unique_lock<std::mutex> lk(endpoint->addr_mu);
      endpoint->addr_cv.wait(lk, [endpoint] {
        return endpoint->peer_addr_ready;
      });
    }
    endpoint->Init();
  }

  void WorkerSendMsg(FabricEndpoint* endpoint) {
    Message msg;
    endpoint->send_queue.WaitAndPop(&msg);
    int meta_len = GetPackMetaLen(msg.meta);
    auto trans = CHECK_NOTNULL(endpoint->GetTransport());
    // start rendezvous if no remote info
    if (!IsValidPushpull(msg)) {
      MessageBuffer *msg_buf = PrepareNewMsgBuf(msg, endpoint);
      endpoint->StoreMsgBuf(msg_buf, msg);
      trans->SendRendezvousBegin(msg, msg_buf);
      return;
    } else {
      auto is_push = msg.meta.push;
      auto key = msg.meta.key;
      if (!endpoint->HasRemoteInfo(key, is_push)) {
        MessageBuffer *msg_buf = PrepareNewMsgBuf(msg, endpoint);
        endpoint->StoreMsgBuf(msg_buf, msg);
        trans->SendRendezvousBegin(msg, msg_buf);
        PS_VLOG(6) << "SendRendezvousBegin " << msg.DebugString();
        return;
      }
    }
    auto context_tuple = endpoint->GetRemoteContext(msg.meta.key, msg.meta.push);
    FabricWRContext *ctx = std::get<0>(context_tuple); // WR context
    MessageBuffer *msg_buf = std::get<1>(context_tuple); // local message buffer

    // prepare new meta and data
    CHECK_EQ(msg_buf->inline_len, (size_t) meta_len);
    CHECK(msg_buf->inline_buf);
    msg_buf->data = msg.data; // may not need this

    // the meta data might change. e.g. timestamp
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);

    // PrintSendLog(msg, msg_buf, addr_tuple);

    // knew remote address, directly send
    trans->Send(ctx);
  }

  void WorkerPollCQ(FabricEndpoint* endpoint, struct fi_cq_tagged_entry *cq_entries) {
    int ret = fi_cq_read(endpoint->fabric_ctx->cq, cq_entries, kMaxConcurrentWorkRequest);
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
        const auto& cq_entry = cq_entries[i];
        HandleCompletionEvent(cq_entry, endpoint);
      }
    }
  }


  void WorkerThread(const std::string hostname, const int port, const Node::Role role) {
    FabricEndpoint* endpoint = WorkerCreateEndpoint(hostname, port, role, info_);
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

  FabricEndpoint* GetOrCreateEndpoint(const std::string& host, const int port, Node::Role role) {
    const std::string hostport = hostport_str(host, port);
    FabricEndpoint* endpoint;
    // create worker thread
    {
      std::lock_guard<std::mutex> lk(endpoints_mu_);
      if (workers_.find(hostport) == workers_.end()) {
        CHECK(worker_endpoints_.find(hostport) == worker_endpoints_.end());
        worker_endpoints_[hostport].reset(new FabricEndpoint());
        auto thread = new std::thread(&FabricVan::WorkerThread, this,
                                      host, port, role);
        workers_[hostport].reset(thread);
      }
      endpoint = worker_endpoints_[hostport].get();
    }
    CHECK(endpoint != nullptr);
    // wait for endpoint context to be initialized
    {
      std::unique_lock<std::mutex> lk(endpoint->status_mu);
      endpoint->status_cv.wait(lk, [endpoint] {
        return endpoint->status != FabricEndpoint::EMPTY;
      });
    }
    return endpoint;
  };

  void OnConnectRequest(const Message &msg) {
    // start worker thread if necessary
    const auto& req_info = msg.meta.control.node[0];
    PS_VLOG(3) << "EQ: connection request " << req_info.DebugString();
    FabricEndpoint *endpoint = GetOrCreateEndpoint(req_info.hostname, req_info.port,
                                                   req_info.role);
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
    const auto& addr_info = msg.meta.control.node[0];
    const int node_id = addr_info.aux_id;
    const std::string hostport = hostport_str(addr_info.hostname, addr_info.port);
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
  std::unordered_map<int, FabricEndpoint*> endpoints_;
  // host:port -> endpoint for connections
  std::unordered_map<std::string, std::unique_ptr<FabricEndpoint>> worker_endpoints_;
  // worker threads
  std::unordered_map<std::string, std::unique_ptr<std::thread>> workers_;

  // event thread
  std::unique_ptr<std::thread> event_polling_thread_;
  // Recv buffer queue
  ThreadsafeQueue<BufferContext *> recv_buffers_;

  // whether my role is server or not
  bool is_server;

  // note that ZMQ use the node id to identify the senders.
  // to setup the connection for libfabric, we don't know the node id ahead of time
  // therefore, we need to use the add sender / receiver hostport to the message meta
  // such that when we unpack the message, we can still know where the message was sent
  std::unordered_map<std::string, int> zmq_connections_;
  Van* zmq_;

  bool is_worker_;
  // fabric provider info
  struct fi_info *info_;

  std::mutex log_mu_;
};  // namespace ps
};  // namespace ps

#endif  // DMLC_USE_FABRIC
#endif  // PS_FABRIC_VAN_H_
