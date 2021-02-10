//  Copyright (C) Mellanox Technologies Ltd. 2020. All Rights Reserved.
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

#ifndef PS_UCX_VAN_H_
#define PS_UCX_VAN_H_

#ifdef DMLC_USE_UCX

#include <ucp/api/ucp.h>
#include <netdb.h>
#include <queue>
#include <set>

#if DMLC_USE_CUDA
#include <cuda_runtime.h>
#endif

namespace ps {

std::mutex g_log_mutex;

#define CHECK_STATUS(_status) CHECK((_status) == UCS_OK)

#define UCX_LOG_BASE(_prio, _my_node, _x) \
 do { \
   if (_prio <= Postoffice::Get()->verbose()) { \
     std::lock_guard<std::mutex> lock(g_log_mutex); \
     LOG(INFO) << (_my_node)->ShortDebugString() << " " << _x; \
   } \
 } while(0)

#define UCX_LOG(_prio, _x) UCX_LOG_BASE(_prio, &my_node_, _x)
#define UCX_LOGE(_prio, _x) UCX_LOG_BASE(_prio, my_node_, _x)

#define UCX_REQUEST_FREE(_req) \
  do { \
    (_req)->data.buffer   = nullptr; \
    (_req)->data.raw_meta = nullptr; \
    (_req)->completed     = false; \
    ucp_request_free(_req); \
  } while(0)

enum class Tags : uint64_t {
  UCX_TAG_META = 0,
  UCX_TAG_DATA = 1ULL << 63,
  UCX_TAG_MASK = 1ULL << 63
};

const int UCX_OPTION_META = -1;
const int UCX_OPTION_DATA = -2;
const int MAX_SID_COUNT = 1 << 31;

class UCXVan;
class UCXContext;

struct UCXBuffer {
  char       *raw_meta;
  char       *buffer;
  int        sender;
};

struct UCXRequest {
  UCXContext *ctx;
  bool       completed;
  UCXBuffer  data;
};

struct UCXAddress {
  UCXAddress() : address(nullptr), length(0), external(false) {}
  UCXAddress (char *addr, size_t len, bool ext) :
    address(addr), length(len), external(ext) {}
  char       *address;
  size_t     length;
  bool       external;
};

struct UCXEp {
  UCXEp() : ep(nullptr), id(-1), connected(false), remote_addr(nullptr) {}
  UCXEp(ucp_ep_h ep, uint64_t node_id) : ep(ep), id(node_id), connected(false),
                                         remote_addr(nullptr) {}
  ucp_ep_h                ep;
  uint64_t                id;
  bool                    connected;
  std::condition_variable cv;
  std::mutex              mu;
  struct addrinfo         *remote_addr;
};

class UCXEndpointsPool {
public:
  UCXEndpointsPool() {
      errh_enable_   = GetEnv("BYTEPS_UCX_ERRH_ENABLE", 1);
      reconnect_tmo_ = GetEnv("BYTEPS_UCX_RECONNECT_TMO", 1000);
  }

  void Init(ucp_worker_h worker, Node *node) {
    worker_  = worker;
    my_node_ = node;
    CHECK_STATUS(ucp_worker_set_am_handler(worker_, UCX_AM_NODE_INFO_REQ,
                                           AmRxNodeInfoReq, this,
                                           UCP_AM_FLAG_WHOLE_MSG));
    CHECK_STATUS(ucp_worker_set_am_handler(worker_, UCX_AM_NODE_INFO_REPLY,
                                           AmRxNodeInfoReply, this,
                                           UCP_AM_FLAG_WHOLE_MSG));
  }

  void Create(const Node &node) {
    if (client_eps_.find(EpId(node.id, node.dev_ids[0])) != client_eps_.end()) {
      // Either duplicate conn request or ep is already connected by peer request
      UCX_LOGE(1, "ep create, exist for node " << node.id);
      return;
    }

    UCXEp *ucx_ep;

    for (int i = 0; i < node.num_ports; ++i) {
      UCX_LOGE(1, "ep create TO: id " << node.id << ", hn " << node.hostname <<
               ", port " << node.ports[i] << ", device id " << node.dev_ids[i]);

      struct addrinfo *remote_addr;
      CHECK_EQ(getaddrinfo(node.hostname.c_str(), std::to_string(node.ports[i]).c_str(),
                  nullptr, &remote_addr),
               0);

      std::lock_guard<std::mutex> lock(mu_);
      int dev_id          = node.dev_ids[i];
      uint64_t id         = EpId(node.id, dev_id);
      client_eps_[id]     = std::make_unique<UCXEp>(nullptr, id);
      ucx_ep              = client_eps_[id].get();
      ucx_ep->remote_addr = remote_addr;

      Create(ucx_ep);
    }

    for (auto &it : client_eps_) {
      if (NodeId(it.first) == node.id) {
        ucx_ep = it.second.get();
        std::unique_lock<std::mutex> lk(ucx_ep->mu);
        ucx_ep->cv.wait(lk, [ucx_ep] { return ucx_ep->connected; } );
      }
    }

    UCX_LOGE(1, "ep create: connected to all ports of " << node.id);
  }

  void Create(UCXEp *ucx_ep) {
    CHECK_EQ(ucx_ep->connected, false);

    ucp_ep_params_t ep_params;
    ep_params.field_mask        = UCP_EP_PARAM_FIELD_FLAGS |
                                  UCP_EP_PARAM_FIELD_SOCK_ADDR;
    ep_params.flags             = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr     = ucx_ep->remote_addr->ai_addr;
    ep_params.sockaddr.addrlen  = ucx_ep->remote_addr->ai_addrlen;
    if (errh_enable_) {
      ep_params.field_mask     |= UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                  UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
      ep_params.err_handler.cb  = ErrorHandlerCb;
      ep_params.err_handler.arg = this;
      ep_params.err_mode        = UCP_ERR_HANDLING_MODE_PEER;
    }

    ucs_status_t status = ucp_ep_create(worker_, &ep_params, &ucx_ep->ep);
    CHECK_STATUS(status) << "ucp_ep_create failed: " << ucs_status_string(status);

    UCX_LOGE(1, "ep created " << ucx_ep->ep << " id: " << ucx_ep->id);

    // UCX ep creation is a non-blocking routine. Exchange ep ids to ensure the
    // connection is setup and ready for use.
    ucs_status_ptr_t req = ucp_am_send_nb(ucx_ep->ep, UCX_AM_NODE_INFO_REQ, &ucx_ep->id,
                                          sizeof(ucx_ep->id), ucp_dt_make_contig(1),
                                          AmReqCompletedCb, UCP_AM_SEND_REPLY);
    if (UCS_PTR_IS_PTR(req)) {
      ucp_request_free(req);
    } else {
      CHECK(!UCS_PTR_IS_ERR(req)) << "failed to send node info";
    }
  }

  void Create(ucp_conn_request_h conn_request) {
    ucp_ep_params_t ep_params;
    ep_params.field_mask        = UCP_EP_PARAM_FIELD_CONN_REQUEST;
    ep_params.err_handler.cb    = ErrorHandlerCb;
    ep_params.err_handler.arg   = this;
    ep_params.conn_request      = conn_request;
    if (errh_enable_) {
      ep_params.field_mask     |= UCP_EP_PARAM_FIELD_ERR_HANDLER;
      ep_params.err_handler.cb  = ErrorHandlerCb;
      ep_params.err_handler.arg = this;
    }

    ucp_ep_h ep;
    ucs_status_t status = ucp_ep_create(worker_, &ep_params, &ep);
    CHECK_STATUS(status) << "failed to create ep by request " << ucs_status_string(status);
    UCX_LOGE(1, "ep created by request: " << ep);
    // Wait for node id to arrive in AmRxNodeInfo, then add to the server_eps set
  }

  void Cleanup() {
    mu_.lock();
    for (auto& it : client_eps_) {
      UCX_LOGE(1, "ep close in cleanup: " << it.first << "|"  << it.second->ep);
      CloseEp(it.second->ep);
    }

    for (auto& it : server_eps_) {
      UCX_LOGE(1, "ep close in cleanup: " << it->ep);
      CloseEp(it->ep);
    }
    mu_.unlock();

    UCX_LOGE(1, "ep close all, active reqs: " << close_ep_reqs_.size());
    while (!close_ep_reqs_.empty()) {
      ucp_worker_progress(worker_);
      ucs_status_t status = ucp_request_check_status(close_ep_reqs_.front());
      if (status != UCS_INPROGRESS) {
          ucp_request_free(close_ep_reqs_.front());
          close_ep_reqs_.pop();
      }
    }
  }

  ucp_ep_h Find(int node_id, int dev_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = client_eps_.find(EpId(node_id, dev_id));
    if (it == client_eps_.end()) {
      return nullptr;
    }

    return (it->second->connected) ? it->second->ep : nullptr;
  }

 private:
  void CloseEp(ucp_ep_h ep) {
    void *req = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);
    if (UCS_PTR_IS_PTR(req)) {
      close_ep_reqs_.push(req);
    } else {
      ucs_status_t status = UCS_PTR_STATUS(req);
      if ((status != UCS_OK) && (status != UCS_ERR_ENDPOINT_TIMEOUT)) {
        LOG(ERROR) << "failed to close ep: " << ep << "("
                   << ucs_status_string(status) << ")";
      }
    }

    UCX_LOGE(2, "close ep " << ep << " with req " << req);
  }

  void ErrorHandler(ucp_ep_h ep)
  {
    mu_.lock();
    auto client_check = [ep](const auto &mo) {return mo.second->ep == ep;};
    auto client_it    = std::find_if(client_eps_.begin(), client_eps_.end(),
                                     client_check);
    if (client_it != client_eps_.end()) {
      UCXEp *uep = client_it->second.get();
      if (!uep->connected) {
        uep->ep = nullptr;
        std::this_thread::sleep_for(std::chrono::milliseconds(reconnect_tmo_));
        Create(uep);
        UCX_LOGE(1, "ep close errh: " << ep << "|" << client_it->first
                 << " Reconnect, close reqs " << close_ep_reqs_.size());
      } else {
        UCX_LOGE(1, "ep close errh: " << ep << "|" << client_it->first
                 << " peer failure");
        client_eps_.erase(client_it);
      }
    } else {
      auto server_check = [ep](const auto &mo) {return mo->ep == ep;};
      auto server_it    = std::find_if(server_eps_.begin(), server_eps_.end(),
                                       server_check);
      // ep may not be present in the set if ep id is not arrived yet (see AmRxNodeInfoReq)
      if (server_it != server_eps_.end()) {
        server_eps_.erase(server_it);
      }
      UCX_LOGE(1, "ep close errh: " << ep);
    }
    mu_.unlock();
    CloseEp(ep);
  }

  uint64_t EpId(int node_id, int dev_id) {
    return ((uint64_t)node_id << 32) | std::max(0, dev_id);
  }

  int NodeId(uint64_t ep_id) {
      return ep_id >> 32;
  }

  // UCX callbacks
  static void ErrorHandlerCb(void *arg, ucp_ep_h ep, ucs_status_t status)
  {
    UCXEndpointsPool *p = reinterpret_cast<UCXEndpointsPool*>(arg);
    UCX_LOG_BASE(1, p->my_node_, "ERRH ep " << ep << ": " << ucs_status_string(status));
    p->ErrorHandler(ep);
  }

  static void AmReqCompletedCb(void *request, ucs_status_t status)
  {
    CHECK_STATUS(status) << "node info send failed: " << ucs_status_string(status);
  }

  static ucs_status_t AmRxNodeInfoReq(void *arg, void *data, size_t length,
                                      ucp_ep_h reply_ep, unsigned flags)
  {
    CHECK_EQ(length, sizeof(uint64_t)) << "length " << length;
    CHECK(reply_ep);
    UCXEndpointsPool *p = reinterpret_cast<UCXEndpointsPool*>(arg);
    uint64_t id         = *(reinterpret_cast<uint64_t*>(data));

    UCX_LOG_BASE(1, p->my_node_, "ep create, got AM id " << id << " my id "
                 << p->my_node_->id <<", save " << reply_ep);

    const auto se = p->server_eps_.emplace(std::make_unique<UCXEp>(reply_ep, id));
    UCXEp *e = se.first->get();

    // Send reply to the peer, so it can set its ep to connected state
    ucs_status_ptr_t req = ucp_am_send_nb(reply_ep, UCX_AM_NODE_INFO_REPLY,
                                          &e->id, sizeof(e->id),
                                          ucp_dt_make_contig(1), AmReqCompletedCb,
                                          UCP_AM_SEND_REPLY);
    if (UCS_PTR_IS_PTR(req)) {
      ucp_request_free(req);
    } else {
      CHECK(!UCS_PTR_IS_ERR(req)) << "failed to send node info";
    }

    return UCS_OK;
  }

  static ucs_status_t AmRxNodeInfoReply(void *arg, void *data, size_t length,
                                        ucp_ep_h reply_ep, unsigned flags)
  {
    CHECK_EQ(length, sizeof(uint64_t)) << "length " << length;
    UCXEndpointsPool *p = reinterpret_cast<UCXEndpointsPool*>(arg);
    uint64_t id         = *(reinterpret_cast<uint64_t*>(data));

    UCX_LOG_BASE(1, p->my_node_, "ep create, got AM REPLY node id " << id
                 << " ep " << reply_ep << " eps size " << p->client_eps_.size());
    for(auto it = p->client_eps_.begin(); it != p->client_eps_.end(); ++it)
    {
        UCX_LOG_BASE(1, p->my_node_, "Key " << it->first );
    }

    std::lock_guard<std::mutex> lock(p->mu_);
    auto e = p->client_eps_.find(id);
    CHECK_NE(e, p->client_eps_.end());
    freeaddrinfo(e->second->remote_addr);

    {
      std::lock_guard<std::mutex> lk(e->second->mu);
      e->second->connected = true;
    }
    e->second->cv.notify_one();

    return UCS_OK;
  }

  typedef std::unordered_map<int, std::unique_ptr<UCXEp>> UCXEps;

  static const int                                     UCX_AM_NODE_INFO_REQ   = 0;
  static const int                                     UCX_AM_NODE_INFO_REPLY = 1;
  std::unordered_map<uint64_t, std::unique_ptr<UCXEp>> client_eps_;
  std::unordered_set<std::unique_ptr<UCXEp>>           server_eps_;
  std::queue<void*>                                    close_ep_reqs_;
  std::mutex                                           mu_;
  ucp_worker_h                                         worker_;
  Node                                                 *my_node_;
  int                                                  errh_enable_;
  int                                                  reconnect_tmo_;
};

// This class:
// 1. Manages memory allocations/registrations on the server side.
// 2. Finds proper receive buffer for the pulled data on worker (looking for
//    the memory used for push)
class UCXRecvPool {
public:
  UCXRecvPool() {}

  ~UCXRecvPool() {
    for (auto& it : rpool_) {
      for (auto& it2 : it.second) {
        if (!it2.second.external) {
          free(it2.second.address);
        }
      }
    }
  }

  char *GetRxBuffer(uint64_t key, int node_id, size_t size, bool push) {
    if (!push) {
      // Must be receive for pulled data, get the cached address
      std::lock_guard<std::mutex> lock(w_pool_mtx_);
      auto addr = w_pool_.find(key);
      CHECK(addr != w_pool_.end());
      return addr->second;
    }

    if (rpool_.find(key) != rpool_.end()) {
      auto it = rpool_[key].find(node_id);
      if (it != rpool_[key].end()) {
        if (size <= it->second.length) {
          return it->second.address;
        } else {
          // cached buffer is smaller than requested - free it and reallocate
          free(it->second.address);
        }
      }
    }

    char *buf;
    size_t page_size = sysconf(_SC_PAGESIZE);
    int ret          = posix_memalign((void**)&buf, page_size, size);
    CHECK(!ret) << "posix_memalign error: " << strerror(ret);
    CHECK(buf);
    memset(buf, 0, size);
    rpool_[key][node_id] = UCXAddress(buf, size, false);

    return buf;
  }

  void Register(Message &msg) {
    uint64_t key = msg.meta.key;
    int sender   = msg.meta.sender;

    if (rpool_.find(key) != rpool_.end()) {
      auto it = rpool_[key].find(sender);
      if ((it != rpool_[key].end()) && (!it->second.external)) {
        // Found cached buffer allocated by UCX Van, need to free it to avoid
        // memory leak
        free(it->second.address);
      }
    }

    rpool_[key][sender] = UCXAddress(msg.data[1].data(), msg.data[1].size(), true);
  }

  void Push(UCXBuffer &data) {
    recv_buffers_.Push(data);
  }

  void Pop(UCXBuffer *data) {
    recv_buffers_.WaitAndPop(data);
  }

  void PushOrdered(UCXBuffer &data) {
    ordered_recv_buffers_.Push(data);
  }

  void PopOrdered(UCXBuffer *data) {
    ordered_recv_buffers_.WaitAndPop(data);
  }

  void CacheLocalAddress(Key key, char *data) {
    std::lock_guard<std::mutex> lock(w_pool_mtx_);
    w_pool_[key] = data;
  }

private:
  typedef std::unordered_map<int, UCXAddress>       MemAddresses;
  ThreadsafeQueue<UCXBuffer>                        recv_buffers_;
  ThreadsafeQueue<UCXBuffer>                        ordered_recv_buffers_;
  std::unordered_map<Key, char*>                    w_pool_;
  std::unordered_map<Key, MemAddresses>             rpool_;
  std::mutex                                        w_pool_mtx_;
};

// This class represent UCX communication instance. It maintains UCX context,
// worker and listener objects. Also it manages UCX eps creation/destruction by
// using UCXEndpointsPool class.
class UCXContext {
public:
  UCXContext(UCXRecvPool *rx_pool, int idx) :
    src_dev_idx_(idx), rx_pool_(rx_pool) {}

  void Init(Node *my_node, UCXVan *van) {
    ucp_config_t *config;
    ucs_status_t status = ucp_config_read("PSLITE", NULL, &config);
    CHECK_STATUS(status) << "ucp_config_read failed: " << ucs_status_string(status);

    // Initialize UCX context
    ucp_params_t ctx_params;
    ctx_params.field_mask        = UCP_PARAM_FIELD_FEATURES          |
                                   UCP_PARAM_FIELD_REQUEST_SIZE      |
                                   UCP_PARAM_FIELD_TAG_SENDER_MASK   |
                                   UCP_PARAM_FIELD_REQUEST_INIT      |
                                   UCP_PARAM_FIELD_ESTIMATED_NUM_EPS;
    ctx_params.features          = UCP_FEATURE_TAG | UCP_FEATURE_AM;
    ctx_params.request_size      = sizeof(UCXRequest);
    ctx_params.tag_sender_mask   = 0ul;
    ctx_params.request_init      = RequestInit;
    ctx_params.estimated_num_eps = std::max(GetEnv("DMLC_NUM_WORKER", 0),
                                            GetEnv("DMLC_NUM_SERVER", 0));

    status = ucp_init(&ctx_params, config, &context_);
    ucp_config_release(config);
    CHECK_STATUS(status) << "ucp_init failed: " << ucs_status_string(status);

    // Create UCP worker
    ucp_worker_params_t w_params;
    w_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    w_params.thread_mode = UCS_THREAD_MODE_MULTI;
    status               = ucp_worker_create(context_, &w_params, &worker_);
    CHECK_STATUS(status) << "ucp_worker_create failed: " << ucs_status_string(status);

    // Check that UCX is compiled with multi-thread support
    ucp_worker_attr_t attr;
    attr.field_mask = UCP_WORKER_ATTR_FIELD_THREAD_MODE;
    status          = ucp_worker_query(worker_, &attr);
    CHECK_STATUS(status) << "ucp_worker_query failed: " << ucs_status_string(status);
    CHECK_EQ(attr.thread_mode, UCS_THREAD_MODE_MULTI)
             << "Single threaded UCX is not supported, build UCX with multi-thread support";

    ep_pool_.Init(worker_, my_node);
    my_node_ = my_node;
    van_     = van;
  }

  // Create UCX listener object, so the remote side could connect by the given
  // address.
  void Listen(int port) {
    auto val = Environment::Get()->find("DMLC_NODE_HOST");
    struct sockaddr_in addr = {};
    if (val) {
      UCX_LOGE(1, "binding to DMLC_NODE_HOST: " << std::string(val));
      addr.sin_addr.s_addr = inet_addr(val);
    } else {
      addr.sin_addr.s_addr = INADDR_ANY;
    }
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);

    ucp_listener_params_t params;
    params.field_mask       = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                              UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
    params.sockaddr.addr    = (const struct sockaddr*)&addr;
    params.sockaddr.addrlen = sizeof(addr);
    params.conn_handler.cb  = ConnHandlerCb;
    params.conn_handler.arg = this;
    ucs_status_t status     = ucp_listener_create(worker_, &params, &listener_);
    CHECK_STATUS(status) << "ucp_listener_create failed: " << ucs_status_string(status);

    UCX_LOGE(1, "bound to " << addr.sin_addr.s_addr << " port: " << port);
  }

  void Connect(const Node &node) {
      ep_pool_.Create(node);
  }

  void Poll() {
    ucp_tag_message_h msg;
    ucp_tag_recv_info_t info;
    int cnt = 0;

    // Poll all underlying transports(IB, shm, tcp, etc)
    cnt = ucp_worker_progress(worker_);
    if (cnt == 0) {
      return;
    }

    // Check whether any meta data or push/pull request arrived
    do {
      // only match the highest bit of tag.
      msg = ucp_tag_probe_nb(worker_,
                             static_cast<ucp_tag_t>(Tags::UCX_TAG_META),
                             static_cast<ucp_tag_t>(Tags::UCX_TAG_MASK),
                             1, &info);
      if (msg != NULL) {
        // some meta data is ready, post a receive to get it
        UCXRequest *meta_req = PostRecvMeta(msg, &info);
        if (meta_req->completed) {
          // meta data received immediately, can post receive for a real
          // data, because it's length is known from meta
          PostRecvData(meta_req);
        }
      }
    } while (msg != NULL);
  }

  ucs_status_ptr_t Send(Message &msg, void *buf, size_t count, ucp_datatype_t dt,
                        Tags tag) {
    int id         = msg.meta.recver;
    int dst_dev_id = msg.meta.dst_dev_id;
    ucp_ep_h ep    = ep_pool_.Find(id, dst_dev_id);
    ucp_tag_t stag = MakeTag(my_node_->id, tag, msg.meta.key);

    UCX_LOGE(2, "Send to ep " << ep  << " (" << id << ", " << dst_dev_id <<")");

    if (ep == nullptr) return UCS_STATUS_PTR(UCS_ERR_NOT_CONNECTED);

    return ucp_tag_send_nb(ep, buf, count, dt, stag, TxReqCompletedCb);
  }

  void Cleanup() {
    ucp_listener_destroy(listener_);
    ucp_worker_destroy(worker_);
    ucp_cleanup(context_);
  }

private:
  /**
   * from the left to the right:
   * 1 bit for UCX_TAG_META/UCX_TAG_DATA
   * 15 bits for the sender node_id
   * 48 bits for the key
   */
  ucp_tag_t MakeTag(int node_id, Tags tag, uint64_t key) {
    ucp_tag_t ret = 0;

    assert(((ucp_tag_t)node_id & 0xFFFFFFFFFFFF8000) == 0);
    ret = (ucp_tag_t)node_id << 48;
    ret &= ~static_cast<uint64_t>(Tags::UCX_TAG_MASK);
    ret |= static_cast<uint64_t>(tag);
    ret |= (key & 0xFFFFFFFFFFFF);

    return ret;
  }

  int NodeIdFromTag(ucp_tag_t tag) {
    tag &= ~static_cast<uint64_t>(Tags::UCX_TAG_MASK);
    return (int)(tag >> 48);
  }

  void PostRecvData(UCXRequest *meta_req) {
    RawMeta *meta = reinterpret_cast<RawMeta*>(meta_req->data.raw_meta);
    int val_len   = meta->val_len;
    if ((val_len == 0) || (meta->option == UCX_OPTION_META)) {
      UCX_LOGE(2, " rx just meta, sender " << meta_req->data.sender
               << " val_len: " << val_len);
      CHECK_EQ(meta_req->data.buffer, nullptr);
      rx_pool_->Push(meta_req->data);
    } else if (meta->option > 0) {
      UCX_LOGE(2, " rx meta with data, data len: " << val_len);
      meta_req->data.buffer = rx_pool_->GetRxBuffer(meta->key, meta_req->data.sender,
                                                    val_len, meta->push);
      memcpy(meta_req->data.buffer, meta_req->data.raw_meta + meta->option, val_len);
      rx_pool_->Push(meta_req->data);
    } else {
      // Add sender id to the tag to ensure message received from the proper node

      char *buf       = rx_pool_->GetRxBuffer(meta->key, meta_req->data.sender,
                                              val_len, meta->push);
      ucp_tag_t tag   = MakeTag(meta_req->data.sender, Tags::UCX_TAG_DATA,
                                meta->key);
      UCXRequest *req = (UCXRequest*)ucp_tag_recv_nb(worker_, buf, val_len,
                                                     ucp_dt_make_contig(1), tag,
                                                     std::numeric_limits<uint64_t>::max(),
                                                     RxDataCompletedCb);
      req->data.raw_meta = meta_req->data.raw_meta;
      req->data.sender   = meta_req->data.sender;
      req->data.buffer   = buf;
      req->ctx           = this;
      UCX_LOGE(2, "rx meta, post recv for data, len " << val_len << ", tag " << tag);
      if (req->completed) {
        rx_pool_->Push(req->data);
        UCX_REQUEST_FREE(req);
      }
    }
    // if request is not completed in-place, it will be handled
    // in RxDataCompletedCb callback

    UCX_REQUEST_FREE(meta_req); // meta req is not needed anymore
  }

  UCXRequest* PostRecvMeta(ucp_tag_message_h msg, ucp_tag_recv_info_t *info) {
    char *rmeta     = new char[info->length];
    UCXRequest *req = (UCXRequest*)ucp_tag_msg_recv_nb(worker_, rmeta, info->length,
                                                       ucp_dt_make_contig(1), msg,
                                                       RxMetaCompletedCb);
    req->ctx           = this;
    req->data.raw_meta = rmeta;
    req->data.sender   = NodeIdFromTag(info->sender_tag);
    UCX_LOGE(2, " rx meta, sender " << req->data.sender << " tag "
             << info->sender_tag << " compl " << req->completed);
    return req;
  }

  static void RequestInit(void *request) {
    UCXRequest *req    = reinterpret_cast<UCXRequest*>(request);
    req->data.buffer   = nullptr;
    req->data.raw_meta = nullptr;
    req->completed     = false;
  }

  // UCX Callbacks
  static void ConnHandlerCb(ucp_conn_request_h conn_request, void *arg) {
    UCXContext *ctx = reinterpret_cast<UCXContext*>(arg);
    ctx->ep_pool_.Create(conn_request);
  }

  static void RxMetaCompletedCb(void *request, ucs_status_t status,
                              ucp_tag_recv_info_t *info)
  {
    UCXRequest *req = reinterpret_cast<UCXRequest*>(request);

    CHECK_STATUS(status) << "RxMetaCompletedCb failed with " << ucs_status_string(status);
    req->completed = true;
    if (req->data.raw_meta == nullptr) {
      // immediate completion
      return;
    }
    req->ctx->PostRecvData(req);
  }

  static void RxDataCompletedCb(void *request, ucs_status_t status,
                              ucp_tag_recv_info_t *info)
  {
    UCXRequest *req = reinterpret_cast<UCXRequest*>(request);

    CHECK_STATUS(status) << "RxDataCompletedCb failed with " << ucs_status_string(status);
    req->completed = true;
    if (req->data.buffer == nullptr) {
      // immediate completion
      return;
    }
    req->ctx->rx_pool_->Push(req->data);
    UCX_REQUEST_FREE(req); // can release request back to UCX now
  }

  static void TxReqCompletedCb(void *request, ucs_status_t status)
  {
    UCXRequest *req = reinterpret_cast<UCXRequest*>(request);

    CHECK_STATUS(status) << "TX request completed with " << ucs_status_string(status);

    delete [] req->data.raw_meta;

    UCX_REQUEST_FREE(req);
  }

  UCXEndpointsPool                                  ep_pool_;
  ucp_context_h                                     context_;
  ucp_worker_h                                      worker_;
  ucp_listener_h                                    listener_;
  int                                               src_dev_idx_;
  Node                                              *my_node_;
  UCXVan                                            *van_;
  UCXRecvPool                                       *rx_pool_;
};

class UCXVan : public Van {
 public:
  UCXVan() {
    setenv("UCX_USE_MT_MUTEX", "y", 0);
    setenv("UCX_IB_NUM_PATHS", "2", 0);
    setenv("UCX_SOCKADDR_CM_ENABLE", "y", 0);
    setenv("UCX_RNDV_THRESH", "8k", 0);
    short_send_thresh_   = GetEnv("BYTEPS_UCX_SHORT_THRESH", 4096);
    force_request_order_ = GetEnv("BYTEPS_UCX_FORCE_REQ_ORDER", 0);
  }

  ~UCXVan() {
    LOG(INFO) << "~UCXVan";
  }

  virtual std::string GetType() const {
    return std::string("ucx");
  }

 protected:
  void Start(int customer_id, bool standalone) override {
    start_mu_.lock();
    should_stop_ = false;

    rx_pool_.reset(new UCXRecvPool());
    start_mu_.unlock();

    if (!standalone) Van::Start(customer_id, false);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();
    should_stop_ = true;
    reorder_thread_->join();
    reorder_thread_.reset();
    polling_thread_->join();
    polling_thread_.reset();

    for (const auto& it : contexts_) {
        it.second->Cleanup();
    }

    rx_pool_.reset();
  }

  int Bind(Node &node, int max_retry) override {
    contexts_.reserve(node.num_ports);

    UCX_LOG(1, "Start/Bind UCX Van, num ports " << node.num_ports);

    // Create separate UCX context for every device. If device is GPU, set the
    // corresponding cuda device before UCX context creation. This way UCX will
    // automatically select the most optimal NICs for using with this device.
    for (int i = 0; i < node.num_ports; ++i) {
      node.dev_ids[i] = i;
      int dev_id = node.dev_ids[i];
      CHECK_GE(dev_id, 0);

#if DMLC_USE_CUDA
      if (node.dev_types[i] == GPU) {
        cudaError_t cerr = cudaSetDevice(dev_id);

        if (cudaSuccess != cerr) {
          LOG(ERROR) << "Failed to set device " << dev_id << ": " << cerr;
        }
      }
#else
      CHECK_NE(node.dev_types[i], GPU) << "Please build with USE_CUDA=1";
#endif
      contexts_[dev_id] = std::make_unique<UCXContext>(rx_pool_.get(), dev_id);
      contexts_[dev_id]->Init(&my_node_, this);
      contexts_[dev_id]->Listen(node.ports[i]);
      UCX_LOG(1, "Create ctx[" << i << "]: dev id " << dev_id << ", port "
              << node.ports[i]);
    }

    polling_thread_.reset(new std::thread(&UCXVan::PollUCX, this));
    reorder_thread_.reset(new std::thread(&UCXVan::ReorderMsg, this));

    return node.ports[0];
  }

  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }

    // Connect every UCX context to all remote ports (UCX contexts)
    for (auto& it : contexts_) {
      it.second->Connect(node);
    }
  }

  int SendMsg(Message &msg) override {
    int id           = msg.meta.recver;
    int src_dev_id   = msg.meta.src_dev_id;
    msg.meta.val_len = 0;
    msg.meta.option  = 0;
    CHECK_NE(id, Meta::kEmpty);

    msg.meta.option = UCX_OPTION_META;

    if (IsValidPushpull(msg)) {
      if (msg.meta.request) {
        msg.meta.key = DecodeKey(msg.data[0]);
        // sequence id
        std::lock_guard<std::mutex> lk(sid_mtx_);
        int sid             = next_send_sids_[id] % MAX_SID_COUNT;
        next_send_sids_[id] = sid + 1;
        msg.meta.sid        = sid;
      }
      if (IsDataMsg(msg)) {
        msg.meta.val_len = msg.data[1].size();
        msg.meta.option  = UCX_OPTION_DATA;
      } else if (!msg.meta.push && msg.meta.request) {
        // Save pull data address
        CHECK(msg.meta.addr != 0);
        rx_pool_->CacheLocalAddress(msg.meta.key, (char*)msg.meta.addr);
      }
    }

    int len = SendMeta(src_dev_id, msg);
    // meta only
    if (msg.meta.option == UCX_OPTION_META) {
      return len;
    }
    // meta with data
    if (len == GetPackMetaLen(msg.meta) + msg.meta.val_len) {
      return len + msg.meta.data_size; // No data, or data was bundled with meta
    }
    // data
    CHECK(IsDataMsg(msg));

    ucs_status_ptr_t st = ContextById(src_dev_id)->Send(msg, msg.data[1].data(),
                                                        msg.data[1].size(),
                                                        ucp_dt_make_contig(1),
                                                        ps::Tags::UCX_TAG_DATA);
    if (UCS_PTR_IS_ERR(st)) {
      LOG(ERROR) << "failed to send data: " << ucs_status_string(UCS_PTR_STATUS(st));
      return -1;
    }
    UCX_LOG(2, "send data, len: " << msg.data[1].size() << ", to id " << id);

    return len + msg.meta.data_size;
  }

  int SendMeta(int src_dev_id, Message &msg) {
    char *meta_buf = nullptr;
    void *buf;
    int meta_size, data_size;
    size_t count;
    ucp_datatype_t dt;

    if (IsDataMsg(msg) && (msg.meta.val_len <= short_send_thresh_)) {
      // Bundle data with meta, save meta length in option (for parsing on Server)
      msg.meta.option   = GetPackMetaLen(msg.meta);
      meta_buf          = new char[msg.meta.option + sizeof(ucp_dt_iov_t)*2];
      PackMeta(msg.meta, &meta_buf, &meta_size);
      ucp_dt_iov_t *iov = reinterpret_cast<ucp_dt_iov_t*>(meta_buf + meta_size);
      iov[0].buffer     = meta_buf;
      iov[0].length     = meta_size;
      iov[1].buffer     = msg.data[1].data();
      iov[1].length     = msg.meta.val_len;
      dt                = ucp_dt_make_iov();
      buf               = iov;
      count             = 2;
      data_size         = msg.meta.val_len;
      UCX_LOG(2, "sending meta to id(" << msg.meta.recver << ") with data: "
              << msg.meta.val_len);
    } else {
      PackMeta(msg.meta, &meta_buf, &meta_size);
      dt        = ucp_dt_make_contig(1);
      buf       = meta_buf;
      count     = meta_size;
      data_size = 0;
      UCX_LOG(2, "sending meta to id(" << msg.meta.recver << "), src dev id "
              << src_dev_id);
    }

    UCX_LOG(2, "before Send");
    ucs_status_ptr_t st = ContextById(src_dev_id)->Send(msg, buf, count, dt,
                                                        ps::Tags::UCX_TAG_META);
    UCX_LOG(2, "after Send");
    if (UCS_PTR_IS_PTR(st)) {
      UCXRequest *req    = reinterpret_cast<UCXRequest*>(st);
      req->data.raw_meta = meta_buf;
    } else {
      // Send was completed immediately
      delete[] meta_buf;
      if (UCS_PTR_IS_ERR(st)) {
        LOG(ERROR) << "failed to send meta data: " << ucs_status_string(UCS_PTR_STATUS(st));
        return -1;
      }
    }

    return meta_size + data_size;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    UCXBuffer buf;
    rx_pool_->PopOrdered(&buf);

    // note size(2d param) is not really used by UnpackMeta
    UnpackMeta(buf.raw_meta, -1, &msg->meta);
    delete [] buf.raw_meta;

    size_t total_len = GetPackMetaLen(msg->meta);
    msg->meta.sender = buf.sender;
    msg->meta.recver = my_node_.id;

    // for pull request, we still need to set the key
    if (!IsValidPushpull(*msg) || (msg->meta.push && !msg->meta.request)) {
      CHECK_EQ(msg->meta.option, UCX_OPTION_META);
      return total_len;
    }

    SArray<char> keys;
    keys.CopyFrom((char*)&msg->meta.key, sizeof(Key));
    msg->data.push_back(keys);

    SArray<char> vals;

    if (!msg->meta.push && msg->meta.request) {
      // pull request - just key and empty vals (kvapp check?)
      msg->data.push_back(vals);
      return total_len + keys.size();
    }

    // Push request or pull response - add data and length
    vals.reset(buf.buffer, msg->meta.val_len, [](void *) {});
    msg->data.push_back(vals);

    SArray<char> lens;
    lens.CopyFrom((char*)(&msg->meta.val_len), sizeof(int));
    msg->data.push_back(lens);

    total_len += keys.size() + vals.size() + lens.size();

    return total_len;
  }

  bool IsPushpullRequest(const RawMeta* raw) {
    CHECK(raw != nullptr);
    auto ctrl = &(raw->control);
    Control::Command cmd = static_cast<Control::Command>(ctrl->cmd);
    if (cmd != Control::EMPTY) return false;
    if (raw->simple_app) return false;
    return raw->request;
  }

  void ReorderMsg() {
    while (!should_stop_.load()) {
      UCXBuffer buf;
      rx_pool_->Pop(&buf);
      RawMeta *raw = (RawMeta*) buf.raw_meta;
      // XXX assume a single reorder thread, so we do not need to acquire mutex
      // std::lock_guard<std::mutex> lk(sid_mtx_);
      if (!IsPushpullRequest(raw) || !force_request_order_) {
        // for non-pushpull messages, we dont check the sid
        rx_pool_->PushOrdered(buf);
        continue;
      }
      int sid = raw->sid;
      CHECK(sid >= 0) << "invalid sid " << sid;
      int sender = buf.sender;
      CHECK(sender >= 0) << "invalid sender " << sender;
      int next_sid = next_recv_sids_[sender];
      // check if this is the next sid
      auto& buffs = recv_sid_buffers_[sender];
      if (sid == next_sid) {
        rx_pool_->PushOrdered(buf);
        // also clears any existing bufs
        int count = 0;
        while (true) {
          next_sid = (next_sid + 1) % MAX_SID_COUNT;
          if (buffs.find(next_sid) != buffs.end()) {
            count++;
            rx_pool_->PushOrdered(buffs[next_sid]);
            buffs.erase(next_sid);
          } else {
            break;
          }
        }
        if (count > 0) PS_VLOG(4) << "out-of-order count = " << count;
        next_recv_sids_[sender] = next_sid;
      } else {
        PS_VLOG(4) << "out-of-order msg arrived. sid=" << sid;
        buffs[sid] = buf;
      }
    }
  }

  void RegisterRecvBuffer(Message &msg) override {
    rx_pool_->Register(msg);
  }

 private:

  UCXContext* ContextById(int id) {
      return contexts_[std::max(0, id)].get();
  }

  bool IsDataMsg(Message &msg) {
    return IsValidPushpull(msg) &&
           ((msg.meta.push && msg.meta.request) ||
           (!msg.meta.push && !msg.meta.request));
  }

  uint64_t DecodeKey(SArray<char> keys) {
    // Just one key is supported now
    CHECK_EQ(keys.size(), 8) << "Wrong key size " << keys.size();
    return *(reinterpret_cast<uint64_t*>(keys.data()));
  }

  void PollUCX() {
   UCX_LOG(2, "polling " << contexts_.size());
    while (!should_stop_.load()) {
      for (const auto& it : contexts_) {
        it.second->Poll();
      }
    }
  }

  std::unique_ptr<std::thread>                         polling_thread_;
  std::atomic<bool>                                    should_stop_;
  std::unordered_map<int, std::unique_ptr<UCXContext>> contexts_;
  std::unique_ptr<UCXRecvPool>                         rx_pool_;
  int                                                  short_send_thresh_;
  // send/recv sequence id
  std::unordered_map<int, int>                         next_send_sids_;
  std::unordered_map<int, int>                         next_recv_sids_;
  std::mutex                                           sid_mtx_;
  // buffers for message whose callback is invoked early
  std::unordered_map<int, std::unordered_map<int, UCXBuffer>> recv_sid_buffers_;
  ThreadsafeQueue<UCXBuffer>                           ordered_recv_buffers_;
  std::unique_ptr<std::thread>                         reorder_thread_;
  int                                                  force_request_order_;
};  // class UCXVan

};  // namespace ps

#endif  // DMLC_USE_UCX
#endif  // PS_UCX_VAN_H_
