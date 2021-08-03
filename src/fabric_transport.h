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

#ifndef PS_FABRIC_TRANSPORT_H_
#define PS_FABRIC_TRANSPORT_H_

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

#include "fabric_utils.h"
#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"
#include "van_common.h"

namespace ps {

struct FabricTransport;

static const size_t kFabricMempoolChunkSize = sizeof(RendezvousMsg);

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

  void Init(fi_info *info) {
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
    ret = fi_ep_bind(ep, (fid_t)cq, FI_SEND | FI_RECV);
    check_err(ret, "Couldn't bind EP-CQ");
    ret = fi_ep_bind(ep, (fid_t)av, 0);
    check_err(ret, "Couldn't bind EP-AV");

    // fi_enable: enable endpoint for communication
    ret = fi_enable(ep);
    check_err(ret, "Couldn't enable endpoint");

    // fi_getname: get endpoint name
    ret = fi_getname((fid_t)ep, addr.name, &addr.len);
    check_err(ret, "Call to fi_getname() failed");

    // fi_av_straddr: human readable name
    fi_av_straddr(av, addr.name, readable_addr.name, &readable_addr.len);
    PS_VLOG(3) << "Endpoint created: " << addr.DebugStr()
               << " readable endpoint = "
               << std::string(readable_addr.name, readable_addr.len);
  }

  ~FabricContext() {
    PS_VLOG(3) << "~FabricContext";
    fi_close((fid_t)ep);
    fi_close((fid_t)cq);
    fi_close((fid_t)av);
    fi_close((fid_t)domain);
    fi_close((fid_t)fabric);
  }
};

struct FabricEndpoint {
  static const int kStartDepth = 128;
  static const int kRxDepth = 2048;  // should be larger than kStartDepth
  static const int kReplyDepth = kRxDepth;

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
  AddressPool<FabricBufferContext> addr_pool;

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

  std::unordered_map<FabricMessageBuffer *, Message>
      msgbuf_cache;  // msg_buf, msg
  std::unordered_map<uint64_t, std::pair<char *, size_t>>
      addr_cache;  // key, <addr, size>

  typedef std::tuple<FabricWRContext *, FabricMessageBuffer *> CtxTuple;

  // key -> [<FabricWRContext*, FabricMessageBuffer*>,]
  std::unordered_map<uint64_t, std::queue<CtxTuple>> push_context;
  std::unordered_map<uint64_t, std::queue<CtxTuple>> pull_context;

  ~FabricEndpoint() { PS_VLOG(3) << "~FabricEndpoint"; }

  void SetNodeID(int id) { node_id = id; }

  std::shared_ptr<FabricTransport> GetTransport() { return trans; }

  void SetPeerAddr(const char *ep_name, const size_t ep_name_len) {
    {
      std::lock_guard<std::mutex> lk(addr_mu);
      if (!peer_addr_ready) {
        peer_ep_name.CopyFrom(ep_name, ep_name_len);
        peer_addr_ready = true;
      }
    }
    addr_cv.notify_all();
  }

  void Create(const std::string &hp, fi_info *info) {
    fabric_ctx = std::unique_ptr<FabricContext>(new FabricContext());
    CHECK(fabric_ctx != nullptr);
    fabric_ctx->Init(info);
    mem_allocator.reset(new FabricMemoryAllocator());
    hostport = hp;
  }

  void InitPeerAddr() {
    // fi_av_insert: insert address vector of the remote endpoint
    int ret = fi_av_insert(fabric_ctx->av, peer_ep_name.name, 1, &peer_addr, 0,
                           nullptr);
    CHECK_EQ(ret, 1) << "Call to fi_av_insert() failed. Return Code: " << ret
                     << ". ERROR: " << fi_strerror(-ret);
    // fi_av_straddr: human readable name
    FabricAddr readable_addr;
    fi_av_straddr(fabric_ctx->av, peer_ep_name.name, readable_addr.name,
                  &readable_addr.len);
    readable_peer_addr = std::string(readable_addr.name, readable_addr.len);
    PS_VLOG(3) << "Peer endpoint connected: " << readable_peer_addr;
  }

  void Init() {
    // set the peer addr
    InitPeerAddr();
    // set transport
    trans = std::make_shared<FabricTransport>(this, mem_allocator.get());
    // set contexts
    InitSendContext(start_tx_ctx, &free_start_ctx, kStartDepth, kSendRendStart,
                    kRendezvousStartMask);
    InitSendContext(reply_tx_ctx, &free_reply_ctx, kReplyDepth, kSendRendReply,
                    kRendezvousReplyMask);
    InitRecvContext(start_rx_ctx, kRxDepth / 2, kReceiveRend,
                    kRendezvousStartMask);
    InitRecvContext(reply_rx_ctx, kRxDepth / 2, kReceiveRend,
                    kRendezvousReplyMask);
    PS_VLOG(3) << "Endpoint initialized";
  }

  void PostRecv(FabricWRContext *ctx) {
    while (true) {
      int ret =
          fi_trecvv(fabric_ctx->ep, ctx->buffers, nullptr, ctx->num_buffers,
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
    PS_VLOG(4) << "Posted recv buffer " << ctx->buffers << " for " << node_id
               << ". size = " << ctx->num_buffers
               << " ctx = " << static_cast<void *>(ctx)
               << " tag = " << ctx->tag;
  }

  void InitRecvContext(FabricWRContext *ctx, size_t num,
                       FabricWRContextType type, uint64_t tag) {
    for (size_t i = 0; i < num; ++i) {
      void *buf;
      aligned_malloc((void **)&buf, kFabricMempoolChunkSize);
      CHECK(buf);

      ctx[i].tag = tag;
      ctx[i].type = type;
      PS_VLOG(4) << "InitRecvContext " << i << "/" << num;
      PrepareWRContext(ctx + i, buf, kFabricMempoolChunkSize,
                       static_cast<char *>(buf) + kFabricMempoolChunkSize, 0);
      PostRecv(&ctx[i]);
    }
  }

  void InitSendContext(FabricWRContext *ctx,
                       ThreadsafeQueue<FabricWRContext *> *queue, size_t num,
                       FabricWRContextType type, uint64_t tag) {
    for (size_t i = 0; i < num; ++i) {
      void *buf;
      aligned_malloc((void **)&buf, kFabricMempoolChunkSize);
      CHECK(buf);

      ctx[i].tag = tag;
      ctx[i].type = type;
      PrepareWRContext(ctx + i, buf, kFabricMempoolChunkSize,
                       static_cast<char *>(buf) + kFabricMempoolChunkSize, 0);
      queue->Push(&ctx[i]);
    }
  }

  bool HasRemoteInfo(uint64_t key, bool is_push) {
    if (is_push) return !push_context[key].empty();
    if (!is_push) return !pull_context[key].empty();
    return false;
  }

  void StoreRemoteContext(FabricMessageBuffer *msg_buf, FabricWRContext *ctx) {
    CHECK_NE(msgbuf_cache.find(msg_buf), msgbuf_cache.end());
    auto &msg = msgbuf_cache[msg_buf];
    auto key = msg.meta.key;
    auto is_push = msg.meta.push;
    auto &queue = is_push ? push_context[key] : pull_context[key];
    queue.emplace(std::make_pair(ctx, msg_buf));
  }

  CtxTuple GetRemoteContext(uint64_t key, bool is_push) {
    auto &queue = is_push ? push_context[key] : pull_context[key];
    CHECK(!queue.empty());
    CtxTuple tuple = queue.front();
    queue.pop();
    return tuple;
  }

  void StoreMsgBuf(FabricMessageBuffer *msg_buf, Message &msg) {
    CHECK_EQ(msgbuf_cache.find(msg_buf), msgbuf_cache.end());
    msgbuf_cache[msg_buf] = msg;
  }

  std::pair<char *, size_t> GetPullAddr(uint64_t key) {
    CHECK_NE(addr_cache.find(key), addr_cache.end())
        << "Cannot find key " << key << " in addr_cache";
    return addr_cache[key];
  }

  void StorePullAddr(Message &msg) {
    auto key = msg.meta.key;
    CHECK_GE(msg.data.size(), 2)
        << "Unexpected number of data: " << msg.data.size();
    addr_cache[key] = std::make_pair((char *)(msg.meta.addr), msg.meta.val_len);
  }

  void ReleaseFirstMsg(FabricMessageBuffer *msg_buf) {
    CHECK_NE(msgbuf_cache.find(msg_buf), msgbuf_cache.end());
    PS_VLOG(6) << "Release msg_buf " << msg_buf;
    msgbuf_cache.erase(msg_buf);
  }
};

class FabricTransport {
 public:
  explicit FabricTransport(FabricEndpoint *endpoint,
                           FabricMemoryAllocator *allocator) {
    endpoint_ = CHECK_NOTNULL(endpoint);
    allocator_ = CHECK_NOTNULL(allocator);
    pagesize_ = sysconf(_SC_PAGESIZE);

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    is_server_ = (role == "server");
  };

  ~FabricTransport() { PS_VLOG(3) << "~FabricTransport"; }

  void Send(FabricWRContext *context) {
    while (true) {
      int ret = fi_tsendv(endpoint_->fabric_ctx->ep, context->buffers, nullptr,
                          context->num_buffers, endpoint_->peer_addr,
                          context->tag, context);
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

  void SendRendezvousBegin(Message &msg, FabricMessageBuffer *msg_buf) {
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
    // if it is a pull response, let the worker know and reuse the address
    if (IsValidPushpull(msg) && !msg.meta.request) {
      req->pull_response = !msg.meta.push;
      req->key = msg.meta.key;
    } else {
      req->pull_response = false;
    }
    Send(context);
    PS_VLOG(3) << "SendRendezvousBegin " << RendezvousDebugStr(*req);
  }

  void SendRendezvousReply(RendezvousMsg *req,
                           AddressPool<FabricBufferContext> &addrpool) {
    FabricBufferContext *buf_ctx = new FabricBufferContext();
    buf_ctx->meta_len = req->meta_len;
    buf_ctx->data_num = req->data_num;
    buf_ctx->endpoint = endpoint_;
    for (size_t i = 0; i < req->data_num; ++i) {
      buf_ctx->data_len[i] = req->data_len[i];
    }
    size_t data_size = 0;
    if (req->pull_response) {
      CHECK(req->data_num >= 2);
    }
    if (req->data_num >= 2) {
      data_size = req->data_len[1];
    }
    size_t meta_size = align_ceil(req->meta_len, pagesize_);
    char *meta_buffer = nullptr;
    char *data_buffer = nullptr;
    if (req->pull_response) {
      size_t alloc_size = meta_size;
      meta_buffer = allocator_->Alloc(meta_size, &alloc_size);
      auto addr_size_pair = endpoint_->GetPullAddr(req->key);
      CHECK_EQ(addr_size_pair.second, data_size)
          << "Inconsistent data buffer size";
      data_buffer = addr_size_pair.first;
    } else {
      size_t alloc_size = meta_size + data_size;
      meta_buffer = allocator_->Alloc(alloc_size, &alloc_size);
      data_buffer = meta_buffer + meta_size;
    }
    CHECK(meta_buffer) << "Unable to allocate memory";
    buf_ctx->meta_buffer = meta_buffer;
    buf_ctx->data_buffer = data_buffer;

    FabricWRContext *reply_ctx = nullptr;
    endpoint_->free_reply_ctx.WaitAndPop(&reply_ctx);

    RendezvousMsg *resp =
        reinterpret_cast<RendezvousMsg *>(reply_ctx->buffers[0].iov_base);

    resp->origin_addr = req->origin_addr;
    resp->tag = addrpool.StoreAddress(buf_ctx);
    CHECK_EQ(resp->tag, resp->tag & kDataMask) << "tag out of bound";

    FabricWRContext *recv_ctx = new FabricWRContext();
    recv_ctx->type = kReceiveWithData;
    CHECK_NE(endpoint_, nullptr);
    recv_ctx->tag = resp->tag;
    PrepareWRContext(recv_ctx, meta_buffer, meta_size, data_buffer, data_size);
    endpoint_->PostRecv(recv_ctx);
    Send(reply_ctx);
    PS_VLOG(3) << "SendRendezvousReply " << RendezvousDebugStr(*resp);
  }

  int RecvPushResponse(Message *msg, FabricBufferContext *buffer_ctx,
                       int meta_len) {
    CHECK_EQ(buffer_ctx->data_num, 0);
    return 0;
  }

  int RecvPullRequest(Message *msg, FabricBufferContext *buffer_ctx,
                      int meta_len) {
    SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));
    SArray<char> vals;  // add an empty sarray to pass kvapp check
    msg->data.push_back(keys);
    msg->data.push_back(vals);
    return keys.size() + vals.size();
  }

  int RecvPushRequest(Message *msg, FabricBufferContext *buffer_ctx,
                      int meta_len) {
    CHECK(msg->meta.push && msg->meta.request);
    CHECK_EQ(buffer_ctx->data_num, 3);

    SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));
    uint32_t len = buffer_ctx->data_len[1];
    SArray<char> vals;
    char *cur = buffer_ctx->data_buffer;
    vals.reset(cur, len, [](void *) {});  // no need to delete
    SArray<char> lens = CreateFunctionalSarray(&len, sizeof(uint32_t));

    msg->data.push_back(keys);
    msg->data.push_back(vals);
    msg->data.push_back(lens);

    return keys.size() + vals.size() + lens.size();
  }

  int RecvPullResponse(Message *msg, FabricBufferContext *buffer_ctx,
                       int meta_len) {
    SArray<char> keys = CreateFunctionalSarray(&msg->meta.key, sizeof(Key));
    uint32_t len = buffer_ctx->data_len[1];
    SArray<char> vals;
    char *cur = buffer_ctx->data_buffer;
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
    sarr.reset((char *)p, size, [p](void *) { free(p); });
    return sarr;
  }

 protected:
  size_t pagesize_ = 8192;
  FabricEndpoint *endpoint_;
  FabricMemoryAllocator *allocator_;
  bool is_server_;

};  // class Transport

};  // namespace ps

#endif  // DMLC_USE_FABRIC
#endif  // PS_FABRIC_TRANSPORT_H_
