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

namespace ps {

std::mutex g_log_mutex;

#define CHECK_STATUS(_status) CHECK((_status) == UCS_OK)

#define UCX_LOG_BASE(_prio, _my_node, _x) \
 do { \
   if (_prio <= Postoffice::Get()->verbose()) { \
     std::lock_guard<std::mutex> lock(g_log_mutex); \
     LOG(INFO) << (_my_node)->ShortDebugString() << " " << _x << std::endl; \
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

class UCXVan;

struct UCXBuffer {
  char       *raw_meta;
  char       *buffer;
  int        sender;
};

struct UCXRequest {
  UCXVan     *van;
  bool       completed;
 UCXBuffer   data;
};

class UCXEndpointsPool {
public:
  UCXEndpointsPool() {
      errh_enable_ = GetEnv("BYTEPS_UCX_ERRH_ENABLE", 1);
  }

  void Init(ucp_worker_h worker, Node *node) {
    worker_  = worker;
    my_node_ = node;
    CHECK_STATUS(ucp_worker_set_am_handler(worker_, UCX_AM_NODE_INFO,
                                           AmRxNodeInfo, this,
                                           UCP_AM_FLAG_WHOLE_MSG));
  }

  void Create(const Node &node) {
    ucp_ep_h ep = Find(node.id);
    if (ep) {
      // Either duplicate conn request or ep is already connected by peer request
      UCX_LOGE(1, "ep create, exists for node " << node.id);
      return;
    }

    struct addrinfo *remote_addr;
    CHECK_EQ(getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(),
                         nullptr, &remote_addr),
             0);

    ucp_ep_params_t ep_params;
    ep_params.field_mask        = UCP_EP_PARAM_FIELD_FLAGS |
                                  UCP_EP_PARAM_FIELD_SOCK_ADDR;
    ep_params.flags             = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr     = remote_addr->ai_addr;
    ep_params.sockaddr.addrlen  = remote_addr->ai_addrlen;
    if (errh_enable_) {
      ep_params.field_mask     |= UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                  UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
      ep_params.err_handler.cb  = ErrorHandlerCb;
      ep_params.err_handler.arg = this;
      ep_params.err_mode        = UCP_ERR_HANDLING_MODE_PEER;
    }

    ucs_status_t status = ucp_ep_create(worker_, &ep_params, &ep);
    CHECK_STATUS(status) << "ucp_ep_create failed: " << ucs_status_string(status);
    mu_.lock();
    client_eps_[node.id] = ep;
    mu_.unlock();

    UCX_LOGE(1, "ep create to node id " <<  ep << "|" << node.id);
    // Send my node id to the server ep of the peer
    ucs_status_ptr_t req = ucp_am_send_nb(ep, UCX_AM_NODE_INFO, &my_node_->id,
                                          sizeof(my_node_->id), ucp_dt_make_contig(1),
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
      UCX_LOGE(1, "ep close in cleanup: " << it.first << "|"  << it.second);
      CloseEp(it.second);
    }
    for (auto& it : server_eps_) {
      UCX_LOGE(1, "ep close in cleanup: " << it);
      CloseEp(it);
    }
    mu_.unlock();

    UCX_LOGE(1, "closing all eps, active reqs: " << close_ep_reqs_.size());
    while (!close_ep_reqs_.empty()) {
        ucp_worker_progress(worker_);
        ucs_status_t status = ucp_request_check_status(close_ep_reqs_.front());
        if (status != UCS_INPROGRESS) {
            ucp_request_free(close_ep_reqs_.front());
            close_ep_reqs_.pop();
        }
    }
  }

  ucp_ep_h Find(int id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = client_eps_.find(id);
    return (it == client_eps_.end()) ? nullptr : it->second;
  }

 private:
  void CloseEp(ucp_ep_h ep) {
    void *req = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);
    if (UCS_PTR_IS_PTR(req)) {
      close_ep_reqs_.push(req);
    } else if (UCS_PTR_STATUS(req) != UCS_OK) {
      LOG(ERROR) << "failed to close ep: " << ep;
    }
  }

  void ErrorHandler(ucp_ep_h ep)
  {
    mu_.lock();
    auto check  = [ep](const auto &mo) {return mo.second == ep;};
    auto result = std::find_if(client_eps_.begin(), client_eps_.end(), check);
    if (result != client_eps_.end()) {
      client_eps_.erase(result);
      UCX_LOGE(1, "ep close errh: " << ep << "|" << result->first);
    } else {
      server_eps_.erase(ep);
      UCX_LOGE(1, "ep close errh: " << ep);
    }
    mu_.unlock();
    CloseEp(ep);
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

  static ucs_status_t AmRxNodeInfo(void *arg, void *data, size_t length,
                                   ucp_ep_h reply_ep, unsigned flags) {
    CHECK_EQ(length, sizeof(my_node_->id));
    CHECK(reply_ep);
    UCXEndpointsPool *p = reinterpret_cast<UCXEndpointsPool*>(arg);
    int id              = *(reinterpret_cast<int*>(data));

    std::lock_guard<std::mutex> lock(p->mu_);
    if (id == Node::kEmpty) {
      // Servers and workers have epmty ids until assigned by the scheduler.
      // So only scheduler can receive connection request from peer with empty node id.
      CHECK(Postoffice::Get()->is_scheduler());
      UCX_LOG_BASE(1, p->my_node_, "ep create, got EMPTY Node id, save " << reply_ep);
      p->server_eps_.insert(reply_ep);
    } else {
      // If connect to the peer with this node id was not called on our van yet,
      // accept this ep
      auto it = p->client_eps_.find(id);
      if (it == p->client_eps_.end()) {
        p->client_eps_[id] = reply_ep;
        UCX_LOG_BASE(1, p->my_node_, "ep create, got node id " << id
                     << ", accept " << reply_ep);
      } else {
        p->server_eps_.insert(reply_ep);
        UCX_LOG_BASE(1, p->my_node_, "ep create, got node id " << id
                     << ", save " << reply_ep);
      }
    }
    return UCS_OK;
  }

  static const int                  UCX_AM_NODE_INFO = 0;
  std::unordered_map<int, ucp_ep_h> client_eps_;
  std::set<ucp_ep_h>                server_eps_;
  std::queue<void*>                 close_ep_reqs_;
  std::mutex                        mu_;
  ucp_worker_h                      worker_;
  Node                              *my_node_;
  int                               errh_enable_;
};

class UCXVan : public Van {
 public:
  UCXVan() {
    setenv("UCX_USE_MT_MUTEX", "y", 0);
    setenv("UCX_IB_NUM_PATHS", "2", 0);
    setenv("UCX_SOCKADDR_CM_ENABLE", "y", 0);
    setenv("UCX_RNDV_THRESH", "8k", 0);
    short_send_thresh_ = GetEnv("BYTEPS_UCX_SHORT_THRESH", 4096);
  }

  ~UCXVan() {}

  virtual std::string GetType() const {
    return std::string("ucx");
  }

 protected:
  void Start(int customer_id, bool standalone) override {
    start_mu_.lock();
    should_stop_ = false;

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

    ep_pool_.Init(worker_, &my_node_);
    polling_thread_.reset(new std::thread(&UCXVan::PollUCX, this));

    start_mu_.unlock();

    if (!standalone) Van::Start(customer_id, false);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();
    should_stop_ = true;
    polling_thread_->join();
    polling_thread_.reset();

    ep_pool_.Cleanup();

    ucp_listener_destroy(listener_);
    ucp_worker_destroy(worker_);
    ucp_cleanup(context_);

    for (auto& it : rpool_) {
      free(it.second.first);
    }
  }

  int Bind(const Node &node, int max_retry) override {
    auto val = Environment::Get()->find("DMLC_NODE_HOST");
    struct sockaddr_in addr = {};
    if (val) {
      UCX_LOG(1, "bind to DMLC_NODE_HOST: " << std::string(val));
      addr.sin_addr.s_addr = inet_addr(val);
    } else {
      addr.sin_addr.s_addr = INADDR_ANY;
    }
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(node.port);

    ucp_listener_params_t params;
    params.field_mask       = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                              UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
    params.sockaddr.addr    = (const struct sockaddr*)&addr;
    params.sockaddr.addrlen = sizeof(addr);
    params.conn_handler.cb  = ConnHandlerCb;
    params.conn_handler.arg = this;
    ucs_status_t status     = ucp_listener_create(worker_, &params, &listener_);
    CHECK_STATUS(status) << "ucp_listener_create failed: " << ucs_status_string(status);

    UCX_LOG(1, "bound to " << addr.sin_addr.s_addr << " port: " << node.port);

    return node.port;
  }

  void Connect(const Node &node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }

    ep_pool_.Create(node);
  }

  int SendMsg(Message &msg) override {
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);

    ucp_ep_h ep = ep_pool_.Find(id);
    if (!ep) {
      LOG(ERROR) << "there is no UCX endpoint to node " << id;
      return -1;
    }

    msg.meta.val_len = 0;
    msg.meta.option  = 0;
    if (IsValidPushpull(msg)) {
      if (msg.meta.request) {
        msg.meta.key = DecodeKey(msg.data[0]);
      }
      if (IsDataMsg(msg)) {
        msg.meta.val_len = msg.data[1].size();
      }
    }

    if (msg.meta.push && msg.meta.request) {
      // Save push data address for later pull
      w_pool_[msg.meta.key] = msg.data[1].data();
    }

    int len = SendMeta(ep, msg);
    if (len == GetPackMetaLen(msg.meta) + msg.meta.val_len) {
      return len + msg.meta.data_size; // No data, or data was bundled with meta
    }

    CHECK(IsDataMsg(msg));

    ucp_tag_t tag       = MakeTag(my_node_.id, UCX_TAG_DATA);
    ucs_status_ptr_t st = ucp_tag_send_nb(ep, msg.data[1].data(),
                                          msg.data[1].size(), ucp_dt_make_contig(1),
                                          tag, TxReqCompletedCb);
    if (UCS_PTR_IS_ERR(st)) {
      LOG(ERROR) << "failed to send data: " << ucs_status_string(UCS_PTR_STATUS(st));
      return -1;
    }
    UCX_LOG(2, "send data, len: " << msg.data[1].size() << ", tag " << tag);

    return len + msg.meta.data_size;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    UCXBuffer buf;
    recv_buffers_.WaitAndPop(&buf);

    // note size(2d param) is not really used by UnpackMeta
    UnpackMeta(buf.raw_meta, -1, &msg->meta);
    delete [] buf.raw_meta;

    size_t total_len = GetPackMetaLen(msg->meta);
    msg->meta.sender = buf.sender;
    msg->meta.recver = my_node_.id;

    if (!IsValidPushpull(*msg) || (msg->meta.push && !msg->meta.request)) {
      CHECK_EQ(msg->meta.val_len, 0);
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

 private:
  enum Tags { UCX_TAG_META = 0, UCX_TAG_DATA };

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

  char *GetRxBuffer(uint64_t key, size_t size, bool push) {
    if (!push) {
      // Must be receive for pulled data, get the address used for push
      auto addr = w_pool_.find(key);
      CHECK(addr != w_pool_.end());
      return addr->second;
    }

    auto it = rpool_.find(key);
    if (it != rpool_.end()) {
      if (size <= it->second.second) {
        return it->second.first;
      }
      // cached buffer is smaller than requested - free it and reallocate
      free(it->second.first);
    }

    char *buf;
    size_t page_size = sysconf(_SC_PAGESIZE);
    int ret          = posix_memalign((void**)&buf, page_size, size);
    CHECK(!ret) << "posix_memalign error: " << strerror(ret);
    CHECK(buf);
    memset(buf, 0, size);
    rpool_[key] = std::make_pair(buf, size);

    return buf;
  }

  void PollUCX() {
    while (!should_stop_.load()) {
      ucp_tag_message_h msg;
      ucp_tag_recv_info_t info;
      int cnt = 0;

      while ((cnt == 0) && !should_stop_.load()) {
        cnt = ucp_worker_progress(worker_);
      }

      do {
        // Match only 32 bits of tag. Higher bits of tag carry sender id
        msg = ucp_tag_probe_nb(worker_, UCX_TAG_META,
                               std::numeric_limits<uint32_t>::max(), 1, &info);
        if (msg != NULL) {
          // Some meta data is ready, post a receive to get it
          UCXRequest *meta_req = PostRecvMeta(msg, &info);
          if (meta_req->completed) {
            // Meta data received immediately, can post receive for a real
            // data, because it's length is known from meta
            PostRecvData(meta_req);
          }
        }
      } while ((msg != NULL) && !should_stop_.load());
    }
  }

  int SendMeta(ucp_ep_h ep, Message &msg) {
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
      UCX_LOG(2, "sending meta to ep(" << msg.meta.recver << ") " << ep
              << " with data: " << msg.meta.val_len);
    } else {
      PackMeta(msg.meta, &meta_buf, &meta_size);
      dt        = ucp_dt_make_contig(1);
      buf       = meta_buf;
      count     = meta_size;
      data_size = 0;
      UCX_LOG(2, "sending meta to ep(" << msg.meta.recver << ") " << ep);
    }

    ucp_tag_t tag       = MakeTag(my_node_.id, UCX_TAG_META);
    ucs_status_ptr_t st = ucp_tag_send_nb(ep, buf, count, dt, tag, TxReqCompletedCb);
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

  void PostRecvData(UCXRequest *meta_req) {
    RawMeta *meta = reinterpret_cast<RawMeta*>(meta_req->data.raw_meta);
    int val_len   = meta->val_len;
    if (val_len == 0) {
      UCX_LOG(2, " rx just meta, sender " << meta_req->data.sender);
      CHECK_EQ(meta_req->data.buffer, nullptr);
      recv_buffers_.Push(meta_req->data);
    } else if (meta->option) {
      UCX_LOG(2, " rx meta with data, data len: " << val_len);
      meta_req->data.buffer = GetRxBuffer(meta->key, val_len, meta->push);
      memcpy(meta_req->data.buffer, meta_req->data.raw_meta + meta->option, val_len);
      recv_buffers_.Push(meta_req->data);
    } else {
      // Add sender id to the tag to ensure message received from the proper node
      char *buf       = GetRxBuffer(meta->key, val_len, meta->push);
      ucp_tag_t tag   = MakeTag(meta_req->data.sender, UCX_TAG_DATA);
      UCXRequest *req = (UCXRequest*)ucp_tag_recv_nb(worker_, buf, val_len,
                                                     ucp_dt_make_contig(1), tag,
                                                     std::numeric_limits<uint64_t>::max(),
                                                     RxDataCompletedCb);
      req->data.raw_meta = meta_req->data.raw_meta;
      req->data.sender   = meta_req->data.sender;
      req->data.buffer   = buf;
      req->van           = this;
      UCX_LOG(2, "rx meta, post recv for data, len " << val_len << ", tag " << tag);
      if (req->completed) {
        recv_buffers_.Push(req->data);
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
    req->van           = this;
    req->data.raw_meta = rmeta;
    req->data.sender   = NodeIdFromTag(info->sender_tag);
    UCX_LOG(2, " rx meta, sender " << req->data.sender << " tag "
            << info->sender_tag << " compl " << req->completed);
    return req;
  }

  // UCX callbacks
  static void ConnHandlerCb(ucp_conn_request_h conn_request, void *arg) {
    UCXVan *van = reinterpret_cast<UCXVan*>(arg);
    van->ep_pool_.Create(conn_request);
  }

  static void TxReqCompletedCb(void *request, ucs_status_t status)
  {
    UCXRequest *req = reinterpret_cast<UCXRequest*>(request);

    CHECK_STATUS(status) << "TX request completed with " << ucs_status_string(status);

    delete [] req->data.raw_meta;

    UCX_REQUEST_FREE(req);
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
    req->van->PostRecvData(req);
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
    req->van->recv_buffers_.Push(req->data);
    UCX_REQUEST_FREE(req); // can release request back to UCX now
  }

  ucp_tag_t MakeTag(int node_id, ucp_tag_t tag) {
    return ((ucp_tag_t)node_id << 32) | tag;
  }

  int NodeIdFromTag(ucp_tag_t tag) {
    return (int)(tag >> 32);
  }

  static void RequestInit(void *request) {
    UCXRequest *req    = reinterpret_cast<UCXRequest*>(request);
    req->data.buffer   = nullptr;
    req->data.raw_meta = nullptr;
    req->completed     = false;
  }

  ucp_context_h                                     context_;
  ucp_worker_h                                      worker_;
  ucp_listener_h                                    listener_;
  std::unique_ptr<std::thread>                      polling_thread_;
  ThreadsafeQueue<UCXBuffer>                        recv_buffers_;
  UCXEndpointsPool                                  ep_pool_;
  std::unordered_map<Key, char*>                    w_pool_;
  std::atomic<bool>                                 should_stop_;
  std::unordered_map<Key, std::pair<char*, size_t>> rpool_;
  int                                               short_send_thresh_;
};  // class UCXVan

};  // namespace ps

#endif  // DMLC_USE_UCX
#endif  // PS_UCX_VAN_H_
