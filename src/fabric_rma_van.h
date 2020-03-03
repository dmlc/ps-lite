/**
 * Copyright (c) 2020 by Contributors
 * Authors: access2rohit@gmail.com (Rohit Srivastava)
 *          linhaibin.eric@gmail.com (Haibin Lin)
*/
#ifndef PS_FABRIC_RMA_VAN_H_
#define PS_FABRIC_RMA_VAN_H_

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
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"


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
static const int kMaxDataFields = 4;
static const size_t kAlignment = 8;

static const int kMaxResolveRetry = 50000;
static const int kBasePort = 9010;

#ifdef __GNUC__
#define DMLC_PS_OFI_LIKELY(x)  __builtin_expect((x), 1)
#define DMLC_PS_OFI_UNLIKELY(x)  __builtin_expect((x), 0)
#else
#define DMLC_PS_OFI_LIKELY(x)  (x)
#define DMLC_PS_OFI_UNLIKELY(x)  (x)
#endif

// We have a limit of MAX_HANDLE_SIZE = 64 bytes. Therefore, we can only
// support an endpoint name of maximum 56 bytes. We are using remaining
// 8 bytes for tags.
#define DMLC_PS_MAX_EP_ADDR (56)
#define DMLC_PS_OFI_MAJOR_VERSION  (1)
#define DMLC_PS_OFI_MINOR_VERSION  (6)
#define dmlc_ps_ofi_version    FI_VERSION(DMLC_PS_OFI_MAJOR_VERSION, \
                                          DMLC_PS_OFI_MINOR_VERSION)

#define check_err(ret, msg) do {                          \
        if (DMLC_PS_OFI_UNLIKELY(ret != 0)) {             \
          LOG(FATAL) << msg << ". RC: " << ret            \
                     << ". ERROR: " << fi_strerror(-ret); \
        }                                                 \
} while (false)

/**
 * \brief be smart on freeing recved data
 */
inline void FreeData2(void* data, void* hint) {
  if (hint == NULL) {
    delete[] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}

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
  explicit SimpleMempool(struct ibv_pd *pd, size_t size = 0x10000000) {
    std::lock_guard<std::mutex> lk(mu_);
    pd_ = pd;
    struct ibv_mr *mr;
    char *p = reinterpret_cast<char *>(aligned_alloc(kAlignment, size));
    total_allocated_size += size;
    CHECK(p);
//    CHECK(mr = ibv_reg_mr(pd, p, size,
//                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
    mr_list.emplace(p+size, mr); // this mr is associated with memory address range [p, p+size]
    free_list.emplace(size, p);
  }

  ~SimpleMempool() {
    std::lock_guard<std::mutex> lk(mu_);
    for(auto it = mr_list.begin(); it != mr_list.end(); it++){
//      CHECK_EQ(ibv_dereg_mr(it->second), 0);
      free(it->second->addr);
    }
  }

  char *Alloc(size_t size) {
    if (size == 0) {
      return nullptr;
    }

    std::lock_guard<std::mutex> lk(mu_);

    size_t proper_size = align_ceil(size, kAlignment);

    auto it = free_list.lower_bound(proper_size);

    if (it == free_list.end()) { // if there is no space left, need to allocate and register new memory
      size_t new_mem_size = total_allocated_size;
      while (proper_size > new_mem_size) {
        new_mem_size *= 2;
      }
      char *p = reinterpret_cast<char *>(aligned_alloc(kAlignment, new_mem_size));
      CHECK(p);
      struct ibv_mr *mr;
      CHECK(mr = ibv_reg_mr(pd_, p, new_mem_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
      mr_list.emplace(p+new_mem_size, mr);
      free_list.emplace(new_mem_size, p);
      it = free_list.lower_bound(proper_size);
      PS_VLOG(1) << "Not enough memory in the pool, requested size " << proper_size << ", new allocated size " << new_mem_size;
      total_allocated_size += new_mem_size;
    }

    CHECK_NE(free_list.end(), it) << "Not enough memory";
    CHECK_GE(it->first, proper_size);

    char *addr = it->second;
    size_t space_left = it->first - proper_size;

    free_list.erase(it);
    CHECK_EQ(used_list.find(addr), used_list.end())
        << "Address is already allocated";

    used_list.emplace(addr, proper_size);

    if (space_left) {
      free_list.emplace(space_left, addr + proper_size);
    }

    return addr;
  }

  void Free(char *addr) {
    if (!addr) {
      return;
    }

    std::lock_guard<std::mutex> lk(mu_);

    auto it = used_list.find(addr);
    CHECK_NE(used_list.end(), it)
        << "Cannot find info about address: " << (uintptr_t)addr;

    size_t size = it->second;
    used_list.erase(it);
    free_list.emplace(size, addr);
  }

  uint32_t LocalKey(char *addr) {
    struct ibv_mr *mr = Addr2MR(addr);
    return mr->lkey;
  }
  uint32_t RemoteKey(char *addr) {
    struct ibv_mr *mr = Addr2MR(addr);
    return mr->rkey;
  }

 private:
  std::mutex mu_;
  std::multimap<size_t, char *> free_list;
  std::unordered_map<char *, size_t> used_list;
  struct ibv_pd *pd_;
  size_t total_allocated_size = 0;

  // first: `end` of this mr address (e.g., for mr with [addr, addr+size], point to `addr+size`)
  std::map<char *, struct ibv_mr*> mr_list;

  // convert the memory address to its associated RDMA memory region
  inline struct ibv_mr* Addr2MR(char *addr) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = mr_list.lower_bound(addr);
    CHECK_NE(it, mr_list.end()) << "cannot find the associated memory region";
    return it->second;
  }

};

class Block {
 public:
  explicit Block(SimpleMempool *pool, char *addr, int count)
      : pool(pool), addr(addr), counter(count) {}

  ~Block() {
    CHECK_EQ(counter, 0);
    pool->Free(addr);
  }

  void Release() {
    int v = counter.fetch_sub(1);
    if (v == 1) {
      delete this;
    }
  }

 private:
  SimpleMempool *pool;
  char *addr;
  std::atomic<int> counter;
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

enum WRContextType {
  kRendezvousStartContext,
  kRendezvousReplyContext,
  kWriteContext,
  kReceiveContext
};

struct WRContext {
  WRContextType type;
  struct ibv_mr *buffer;
  void *private_data;
};

struct BufferContext {
  char *buffer;
  size_t meta_len;
  size_t data_num;
  size_t data_len[kMaxDataFields];
};

typedef std::unique_ptr<struct ibv_mr, std::function<void(struct ibv_mr *)>>
    MRPtr;

struct MessageBuffer {
  size_t inline_len;
  char *inline_buf;
  WRContext *reserved_context;
  std::vector<SArray<char>> data;
  std::vector<std::pair<MRPtr, size_t>> mrs;
};

struct RequestContext {
  uint32_t node;
  uint16_t port;
  char hostname[kMaxHostnameLength];
};

static_assert(std::is_pod<RendezvousStart>::value,
              "RendezvousStart must be a POD type.");
static_assert(std::is_pod<RendezvousReply>::value,
              "RendezvousReply must be a POD type.");
static_assert(std::is_pod<RequestContext>::value,
              "RequestContext must be a POD type.");

static const size_t kMempoolChunkSize =
    std::max(sizeof(RendezvousStart), sizeof(RendezvousReply));

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

struct Endpoint {
  enum ConnectionStatus { IDLE, CONNECTING, CONNECTED, REJECTED };

  ConnectionStatus status;
  int node_id;
  std::condition_variable cv;
  std::mutex connect_mu;
  struct rdma_cm_id *cm_id;

  WRContext rx_ctx[kRxDepth];

  WRContext start_ctx[kStartDepth];
  WRContext reply_ctx[kReplyDepth];
  WRContext write_ctx[kWriteDepth];

  ThreadsafeQueue<WRContext *> free_start_ctx;
  ThreadsafeQueue<WRContext *> free_reply_ctx;
  ThreadsafeQueue<WRContext *> free_write_ctx;

  Endpoint() : status(IDLE), node_id(Node::kEmpty), cm_id(nullptr), rx_ctx() {}

  ~Endpoint() {
//    for (int i = 0; i < kRxDepth; ++i) {
//      if (!(rx_ctx[i].buffer)) {
//        continue;
//      }
//      free(rx_ctx[i].buffer->addr);
//      CHECK_EQ(ibv_dereg_mr(rx_ctx[i].buffer), 0);
//    }
//
//    for (int i = 0; i < kStartDepth; ++i) {
//      if (start_ctx[i].buffer) {
//        free(start_ctx[i].buffer->addr);
//        CHECK_EQ(ibv_dereg_mr(start_ctx[i].buffer), 0);
//      }
//    }
//
//    for (int i = 0; i < kReplyDepth; ++i) {
//      if (reply_ctx[i].buffer) {
//        free(reply_ctx[i].buffer->addr);
//        CHECK_EQ(ibv_dereg_mr(reply_ctx[i].buffer), 0);
//      }
//    }
//
//    for (int i = 0; i < kWriteDepth; ++i) {
//      if (write_ctx[i].buffer) {
//        free(write_ctx[i].buffer->addr);
//        CHECK_EQ(ibv_dereg_mr(write_ctx[i].buffer), 0);
//      }
//    }
//
//    rdma_destroy_qp(cm_id);
//    CHECK_EQ(rdma_destroy_id(cm_id), 0) << strerror(errno);
  }

  void Disconnect() {
    std::unique_lock<std::mutex> lk(connect_mu);
    CHECK_EQ(rdma_disconnect(cm_id), 0) << strerror(errno);
    cv.wait(lk, [this] { return status == IDLE; });
  }

  void SetNodeID(int id) { node_id = id; }

  void InitSendContextHelper(struct ibv_pd *pd, WRContext *ctx,
                             ThreadsafeQueue<WRContext *> *queue, size_t num,
                             WRContextType type) {
    for (size_t i = 0; i < num; ++i) {
      void *buf = aligned_alloc(kAlignment, kMempoolChunkSize);
      CHECK(buf);
      struct ibv_mr *mr = ibv_reg_mr(pd, buf, kMempoolChunkSize, 0);
      CHECK(mr);

      ctx[i].type = type;
      ctx[i].buffer = mr;
      ctx[i].private_data = this;
      queue->Push(&ctx[i]);
    }
  }

  void Init(struct ibv_cq *cq, struct ibv_pd *pd) {
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_init_attr));
    attr.send_cq = cq;
    attr.recv_cq = cq;
    attr.cap.max_send_wr = kStartDepth + kReplyDepth + kWriteDepth;
    attr.cap.max_recv_wr = kRxDepth;
    attr.cap.max_send_sge = kSGEntry;
    attr.cap.max_recv_sge = kSGEntry;
    attr.qp_type = IBV_QPT_RC;
    attr.sq_sig_all = 0;

    CHECK_EQ(rdma_create_qp(cm_id, pd, &attr), 0)
        << "Create RDMA queue pair failed";

    InitSendContextHelper(pd, start_ctx, &free_start_ctx, kStartDepth,
                          kRendezvousStartContext);
    InitSendContextHelper(pd, reply_ctx, &free_reply_ctx, kReplyDepth,
                          kRendezvousReplyContext);
    InitSendContextHelper(pd, write_ctx, &free_write_ctx, kWriteDepth,
                          kWriteContext);

    for (size_t i = 0; i < kRxDepth; ++i) {
      void *buf = aligned_alloc(kAlignment, kMempoolChunkSize);
      CHECK(buf);
      struct ibv_mr *mr =
          ibv_reg_mr(pd, buf, kMempoolChunkSize, IBV_ACCESS_LOCAL_WRITE);
      CHECK(mr);

      rx_ctx[i].type = kReceiveContext;
      rx_ctx[i].buffer = mr;
      rx_ctx[i].private_data = this;

      PostRecv(&rx_ctx[i]);
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

    CHECK_EQ(ibv_post_recv(cm_id->qp, &wr, &bad_wr), 0)
        << "ibv_post_recv failed.";
  }
};

struct FabricEndpoint {
	enum ConnectionStatus { IDLE, CONNECTING, CONNECTED, REJECTED };
  ConnectionStatus status;
  int node_id;
  struct fid_ep *ep; // the endpoint
  struct fid_av *av; // address vector
  char* name; // required
  size_t name_len; // required
  std::condition_variable cv;
  std::mutex connect_mu;
//  std::shared_ptr<Transport> trans;

  // WRContext start_ctx[kStartDepth];
  // WRContext reply_ctx[kReplyDepth];

  void Init(struct fid_domain *domain, struct fi_info *info, struct fid_cq *cq) {
    // bind completion queue
  	if (cq != nullptr){
  		fi_ep_bind(ep, (fid_t) cq, FI_SEND | FI_RECV);
  	}
    // create and bind addresss vector
    struct fi_av_attr av_attr = {};
    av_attr.type = FI_AV_TABLE;
    fi_av_open(domain, &av_attr, &av, nullptr);
    fi_ep_bind(ep, (fid_t) av, 0);
    // enable endpoint
    fi_enable(ep);
    // query endpoint name
    int ret = fi_getname(&(ep->fid), (void *)&name, &name_len);
    // PostRecv for all WRContext

  }

  void PostRecv(WRContext *ctx) {
    // fi_trecv(struct fid_ep *ep, void *buf, size_t len, void *desc,
    //          fi_addr_t src_addr, uint64_t tag, uint64_t ignore, void *context);
  }
};


class FabricRMAVan : public Van {
 private:
	struct fi_info *info_; // fabric provider info
	struct fid_fabric *fabric_; // fabric top-level object
	struct fid_domain *domain; // domains, which maps to a specific local network interface adapter
	struct fid_cq *cq; // completion queue
	std::unique_ptr<FabricEndpoint> my_endpoint_;
	void* context_ = nullptr;
	struct fi_info* ofi_provider_ = nullptr;

	// name of the endpoint
	char av_name_[DMLC_PS_MAX_EP_ADDR] = {};
	// length of the name
	size_t av_name_len_ = sizeof(av_name_);
	std::mutex mu_;
	bool is_worker_;
	std::vector<std::thread*> thread_list_;

	void* receiver_ = nullptr; // for incoming connect queries
	std::unordered_map<std::string, std::unique_ptr<FabricEndpoint>> endpoints_;
	// str = "hostname:port"
	std::unordered_map<std::string, void*> zmq_sockets_;
	std::unordered_map<std::string, void*> senders_;
	// <key, recver>, (<remote_addr, rkey, idx, local_addr>)
	// std::unordered_map<uint64_t, RemoteAndLocalAddress> push_addr_;
	// std::unordered_map<uint64_t, RemoteAndLocalAddress> pull_addr_;

	std::unordered_map<char*, struct fid_mr*> mem_mr_; // (memory address, fid_mr)

	// local IPC related
	bool disable_ipc_ = false;
	std::mutex local_mu_;
	std::unordered_map<int, bool> is_local_;
 public:
  FabricRMAVan() {
	struct fi_info *hints = fi_allocinfo();
	// set hints for capacity and modes, create fabric, domain and cq
	fi_getinfo(FI_VERSION(1, 6), nullptr, 0, 0, hints, &info_);
	fi_fabric(info_->fabric_attr, &fabric_, nullptr);
	fi_domain(fabric_, info_, &domain, nullptr);
	// Open CQ
	struct fi_cq_attr cq_attr = {};
	cq_attr.format = FI_CQ_FORMAT_TAGGED;
	fi_cq_open(domain, &cq_attr, &cq, nullptr);
  }
  ~FabricRMAVan() {}


 protected:
  void Start(int customer_id) override {
    start_mu_.lock();
    should_stop_ = false;

    OfiInit();

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    is_server = role=="server";
    if (is_server) LOG(INFO) << "This is server";
    else LOG(INFO) << "This is " << ((role=="worker") ? "worker" : "scheduler");

//    val = Environment::Get()->find("ENABLE_RDMA_LOG");
//    enable_rdma_log_ = val? atoi(val) : false;
//    if (enable_rdma_log_) LOG(INFO) << "Enable RDMA logging";
//    else LOG(INFO) << "RDMA logging is disabled, you can enable it with ENABLE_RDMA_LOG=1";

//    if (event_channel_ == nullptr) {
//      event_channel_ = rdma_create_event_channel();
//      CHECK(event_channel_) << "Create RDMA event channel failed";

//      cm_event_polling_thread_.reset(
//          new std::thread(&RDMAVan::PollEvents, this));
//    }\
    start_mu_.unlock();
    ZmqStart();
    Van::Start(customer_id);
  }

  void ZmqStart() {
      start_mu_.lock();
      if (context_ == nullptr) {
        context_ = zmq_ctx_new();
        CHECK(context_ != NULL) << "create 0mq context failed";
      }
      start_mu_.unlock();

      auto val1 = Environment::Get()->find("BYTEPS_ZMQ_MAX_SOCKET");
      int byteps_zmq_max_socket = val1 ? atoi(val1) : 1024;
      zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, byteps_zmq_max_socket);
      PS_VLOG(1) << "BYTEPS_ZMQ_MAX_SOCKET set to " << byteps_zmq_max_socket;

      auto val2 = Environment::Get()->find("BYTEPS_ZMQ_NTHREADS");
      int byteps_zmq_nthreads = val2 ? atoi(val2) : 4;
      zmq_ctx_set(context_, ZMQ_IO_THREADS, byteps_zmq_nthreads);
      PS_VLOG(1) << "BYTEPS_ZMQ_NTHREADS set to " << byteps_zmq_nthreads;
    }

  void OfiInit() {
    // Get a list of fi_info structures for a single provider
    struct fi_info *hints = fi_allocinfo();
    CHECK(hints != nullptr) << "Unable to allocate fi_info";

    // Hints to filter providers
    hints->caps = FI_TAGGED | FI_MSG | FI_RMA;
    hints->mode = FI_CONTEXT;

    hints->ep_attr->type = FI_EP_RDM;

    hints->domain_attr->av_type = FI_AV_TABLE;
    hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->data_progress = FI_PROGRESS_AUTO;

    // Indicate that the application support local memory registration
    hints->domain_attr->mr_mode = FI_MR_LOCAL;

    // TODO figure out msg_order
    hints->tx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->msg_order = FI_ORDER_SAS;

    std::lock_guard<std::mutex> lock(mu_);
    int ret = fi_getinfo(dmlc_ps_ofi_version, nullptr, 0, 0, hints, &ofi_provider_);
    if (ret == -FI_ENODATA) {
      LOG(FATAL) << "Could not find any optimal provider.";
    } else {
      check_err(ret, "Could not complete fi_getinfo");
    }
    CHECK(ofi_provider_ != nullptr) << "Failed to get ofi provider";
    // If we detect the Amazon EFA provider, emulate a NIC per GPU
    // so that NCCL will build more rings and achieve better peak BW
    if (strcmp(ofi_provider_->fabric_attr->prov_name, "efa") == 0) {
      ofi_provider_->next = ofi_provider_;
    }
    fi_freeinfo(hints);
    LOG(INFO) << "Selected fabric provider is "
              << ofi_provider_->fabric_attr->prov_name;

    // Check if provider requires local memory registration
    if (ofi_provider_->domain_attr->mr_mode & FI_MR_LOCAL) {
      LOG(FATAL) << "Provider " << ofi_provider_->fabric_attr->prov_name
                 << " required registration of local memory buffers, which"
                 << " is not implemented";
    }
  }

  void Stop() override {
//    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
//    Van::Stop();
//
//    should_stop_ = true;
//    CHECK(should_stop_);
//
//    PS_VLOG(1) << "Stopping cq_polling_thread_.";
//    cq_polling_thread_->join();
//    cq_polling_thread_.reset();
//
//    PS_VLOG(1) << "Stopping cm_event_polling_thread_.";
//    cm_event_polling_thread_->join();
//    cm_event_polling_thread_.reset();
//
//    PS_VLOG(1) << "Clearing mempool.";
//    mempool_.reset();
//
//    auto map_iter = memory_mr_map.begin();
//    while (map_iter != memory_mr_map.end()) {
//      ibv_dereg_mr(map_iter->second);
//      map_iter++;
//    }
//
//    PS_VLOG(1) << "Clearing endpoints.";
//    incoming_.clear();
//    endpoints_.clear();
//
//    PS_VLOG(1) << "Destroying cq and pd.";
//    CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
//    CHECK(!ibv_destroy_comp_channel(comp_event_channel_))
//        << "Failed to destroy channel";
//
//    // TODO: ibv_dealloc_pd sometimes complains resource busy, need to fix this
//    // CHECK(!ibv_dealloc_pd(pd_)) << "Failed to deallocate PD: " <<
//    // strerror(errno);
//
//    PS_VLOG(1) << "Destroying listener.";
//    rdma_destroy_id(listener_);
//    rdma_destroy_event_channel(event_channel_);
  }

  int Bind(const Node &node, int max_retry) override {
//    CHECK(rdma_create_id(event_channel_, &listener_, nullptr, RDMA_PS_TCP) == 0)
//        << "Create RDMA connection identifier failed";
//
//    struct sockaddr_in addr;
//    memset(&addr, 0, sizeof(addr));
//
//    auto val = Environment::Get()->find("DMLC_NODE_HOST");
//    if (val) {
//      PS_VLOG(1) << "bind to DMLC_NODE_HOST: " << std::string(val);
//      addr.sin_addr.s_addr = inet_addr(val);
//    }
//
//    addr.sin_family = AF_INET;
//    int port = node.port;
//    unsigned seed = static_cast<unsigned>(time(NULL) + port);
//    for (int i = 0; i < max_retry + 1; ++i) {
//      addr.sin_port = htons(port);
//      if (rdma_bind_addr(listener_,
//                         reinterpret_cast<struct sockaddr *>(&addr)) == 0) {
//        break;
//      }
//      if (i == max_retry) {
//        port = -1;
//      } else {
//        port = 10000 + rand_r(&seed) % 40000;
//      }
//    }
//    CHECK(rdma_listen(listener_, kRdmaListenBacklog) == 0)
//        << "Listen RDMA connection failed: " << strerror(errno);
//    return port;


    // create my endpoint
    my_endpoint_ = std::unique_ptr<FabricEndpoint>(new FabricEndpoint());
    my_endpoint_->Init(domain, info_, cq);
    // ZMQ for out-of-band communication
    int my_port = ZMQBind(node, max_retry);
    std::thread(ZMQPollEvent());
    std::thread(ZMQProcessMsg());
    return my_port;
  }

  struct ZmqBufferContext { // for clarity, don't merge meta and data
    std::string sender;
    zmq_msg_t* meta_zmsg;
    std::vector<zmq_msg_t*> data_zmsg;
  };

  int GetNodeID(const char* buf, size_t size) {
    if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
      int id = 0;
      size_t i = 2;
      for (; i < size; ++i) {
        if (buf[i] >= '0' && buf[i] <= '9') {
          id = id * 10 + buf[i] - '0';
        } else {
          break;
        }
      }
      if (i == size) return id;
    }
    return Meta::kEmpty;
  }

  ThreadsafeQueue<ZmqBufferContext> recv_buffers_;

  void CallZmqRecvThread(void* socket) {
    CHECK(socket);
    LOG(INFO) << "Start ZMQ recv thread";

    while (true) {
      ZmqBufferContext *buf_ctx = new ZmqBufferContext();

      for (int i = 0;; ++i) {
        zmq_msg_t* zmsg = new zmq_msg_t;
        CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
        while (true) {
          std::lock_guard<std::mutex> lk(mu_);
          // the zmq_msg_recv should be non-blocking, otherwise deadlock will happen
          int tag = ZMQ_DONTWAIT;
          if (should_stop_ || zmq_msg_recv(zmsg, socket, tag) != -1) break;
          if (errno == EINTR) {
            std::cout << "interrupted";
            continue;
          } else if (errno == EAGAIN) { // ZMQ_DONTWAIT
            continue;
          }
          CHECK(0) << "failed to receive message. errno: " << errno << " "
                       << zmq_strerror(errno);
        }
        if (should_stop_) break;
        char* buf = CHECK_NOTNULL((char*)zmq_msg_data(zmsg));
        size_t size = zmq_msg_size(zmsg);

        if (i == 0) {
          // identify
//          buf_ctx->sender = GetNodeID(buf, size);
          CHECK(zmq_msg_more(zmsg));
          zmq_msg_close(zmsg);
          delete zmsg;
        }
        else if (i == 1) {
          // task
          buf_ctx->meta_zmsg = zmsg;
          bool more = zmq_msg_more(zmsg);
          if (!more) break;
        }
        else {
          buf_ctx->data_zmsg.push_back(zmsg);
          bool more = zmq_msg_more(zmsg);
          if (!more) break;
        }
      } // for
      if (should_stop_) break;
      recv_buffers_.Push(*buf_ctx);
    } // while
  }


  void ZMQPollEvent() {
  	CallZmqRecvThread(receiver_);
  }

  void ZMQProcessMsg() {
  	while (!should_stop_) {
  		Message* msg;
  		int len = ZMQRecvMsg(msg);
  		Node node = msg->meta.control.node[0];
			switch (msg->meta.control.cmd) {
				case Control::ADDR_REQUEST:
					OnConnectionRequest();
					break;
				case Control::ADDR_RESOLVED:
					OnConnected(node);
					break;
				// case: OnDisconnected();
				// break;
			}
		}
  }

  int ZMQRecvMsg(Message* msg) /*override */{
    msg->data.clear();

    ZmqBufferContext notification;
    recv_buffers_.WaitAndPop(&notification);

    size_t recv_bytes = 0;

//    msg->meta.sender = notification.sender;
//    msg->meta.recver = my_node_.id;

    char* meta_buf = CHECK_NOTNULL((char*)zmq_msg_data(notification.meta_zmsg));
    size_t meta_len = zmq_msg_size(notification.meta_zmsg);

    UnpackMeta(meta_buf, meta_len, &(msg->meta));
    recv_bytes += meta_len;

    for (size_t i = 0; i < notification.data_zmsg.size(); ++i) {
      auto zmsg = notification.data_zmsg[i];
      char* buf = CHECK_NOTNULL((char*)zmq_msg_data(zmsg));
      size_t size = zmq_msg_size(zmsg);
      recv_bytes += size;


      SArray<char> data;
      // zero copy
      data.reset(buf, size, [zmsg, size](void *) {
        zmq_msg_close(zmsg);
        delete zmsg;
      });
      msg->data.push_back(data);
    }

    return recv_bytes;
  }

  void ZmqConnect(const Node& node) {
    // use zmq to boostrap libfabric
//    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    int id = node.id;
    std::string key = node.hostname + ":" + std::to_string(node.port);
    mu_.lock();
    auto it = zmq_sockets_.find(key);
    if (it != zmq_sockets_.end()) {
      zmq_close(it->second);
    }
    mu_.unlock();
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) /*&& (node.id != my_node_.id)*/) {
      LOG(INFO) << "skip connecting to my peers";
      return;
    }
    void* sender = zmq_socket(context_, ZMQ_DEALER);
    CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
    if (!key.empty()) {
			std::string my_id = "ps" + std::to_string(my_node_.id);
			zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
			std::lock_guard<std::mutex> lk(mu_);
			if (is_worker_ && (zmq_sockets_.find(key)==zmq_sockets_.end())) {
				auto t = new std::thread(&FabricRMAVan::CallZmqRecvThread, this, (void*) sender);
				thread_list_.push_back(t);
			}
    }
    // connect
    std::string addr =
        "tcp://" + node.hostname + ":" + std::to_string(node.port);

    if (GetEnv("DMLC_LOCAL", 0)) {
      addr = "ipc:///tmp/" + std::to_string(node.port);
    }
    if (zmq_connect(sender, addr.c_str()) != 0) {
      LOG(FATAL) << "connect to " + addr + " failed: " + zmq_strerror(errno);
    }
    std::lock_guard<std::mutex> lk(mu_);
    zmq_sockets_[key] = sender;
    if (my_node_.role != Node::SCHEDULER) {
      CHECK_EQ(senders_.size(), 1) << "Unexpected number of senders";
    }
    LOG(INFO) << "ZMQ sender " << id << " connected to " + addr;
  }

  void OnConnectionRequest() {
	  // prepare av_name and av_name_len;
//	  OfiCreateEndpoint();
//	  ret = fi_getname(&(my_endpoint_->ep->fid), (void *)&av_name_, &av_name_len_);
//	  check_err(ret, "Call to fi_getname() failed");

      // key
	  if (Node::SERVER == my_node_.role || Node::WORKER == my_node_.role){
		  Node key;
		  key.role = my_node_.role;
		  key.hostname = my_node_.hostname; // My IP Address
		  key.port = my_node_.port; // My Port
		  // value
		  Node value;
		  value.hostname = av_name_; // My av_name

		  std::string addr = key.hostname + ":" + std::to_string(key.port);
		  auto iter = zmq_sockets_.find(addr);
		  if (iter == zmq_sockets_.end()) {
		  	ZmqConnect(key);
		  }

		  Message req;
//		  req.meta.recver = Node::kEmpty;
		  req.meta.control.cmd = Control::ADDR_REQUEST;
		  req.meta.control.node.push_back(key);
		  req.meta.control.node.push_back(value);

		  int bytes = ZmqSendMsg(req);
	  }
  }

  int ZmqSendMsg(Message& msg) {
    //if (!is_worker_) return NonWorkerSendMsg(msg);

    std::lock_guard<std::mutex> lk(mu_);

//    int id = msg.meta.recver;
//    CHECK_NE(id, Meta::kEmpty);

    // find the socket
    std::string key = msg.meta.control.node[0].hostname + ":" + std::to_string(msg.meta.control.node[0].port);
    auto it = zmq_sockets_.find(key);
    if (it == zmq_sockets_.end()) {
      LOG(WARNING) << "there is no socket to node " << key;
      return -1;
    }

    void* socket = it->second;

    return ZmqSendMsg(socket, msg);
  }

  int ZmqSendMsg(void* socket, Message& msg) {
    // send meta
    int meta_size;
    char* meta_buf = nullptr;
    PackMeta(msg.meta, &meta_buf, &meta_size);
    int tag = ZMQ_SNDMORE;
    int n = msg.data.size();
    if (n == 0) tag = 0;
    zmq_msg_t meta_msg;
    zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData2, NULL);
    while (true) {
      if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
      if (errno == EINTR) continue;
      CHECK(0) << zmq_strerror(errno);
    }
    zmq_msg_close(&meta_msg);
    int send_bytes = meta_size;
    return send_bytes;
  }

/*
  void OfiCreateEndpoint() {
    // TODO: use smart pointer
    my_endpoint_ = new FabricEndpoint();
    CHECK(my_endpoint_ != nullptr) << "Failed to allocate Endpoint";

    // Initialize tag and num_cqes
    my_endpoint_->tag = 1;
    my_endpoint_->num_cqes = DMLC_PS_OFI_MAX_REQUESTS;
    my_endpoint_->prov_name = ofi_provider_->fabric_attr->prov_name;

    // Determine if any tag bits are used by provider
    int ofi_tag_leading_zeroes = 0, ofi_tag_bits_for_ring_id = 64;
    while (!((ofi_provider_->ep_attr->mem_tag_format << ofi_tag_leading_zeroes++) &
      (uint64_t) OFI_HIGHEST_TAG_BIT) &&
      (ofi_tag_bits_for_ring_id >= MIN_TAG_BITS_FOR_RING_ID)) {
      ofi_tag_bits_for_ring_id--;
    }

    CHECK_GT(ofi_tag_bits_for_ring_id, MIN_TAG_BITS_FOR_RING_ID)
      << "Provider " << ofi_provider_->fabric_attr->prov_name
      << " does not provide enough tag bits " << ofi_tag_bits_for_ring_id
      << " for ring ID. Minimum required is " << MIN_TAG_BITS_FOR_RING_ID;

    // Set maximum tag information; Reserving 1 bit for control information
    my_endpoint_->max_tag = (uint64_t)((1ULL << (ofi_tag_bits_for_ring_id - 1)) - 1);

    // Create fabric
//    int ret = fi_fabric(ofi_provider_->fabric_attr, &(my_endpoint_->fabric), nullptr);
//    check_err(ret, "Couldn't open a fabric provider");

    // Create domain
//    ret = fi_domain(my_endpoint_->fabric, ofi_provider_,
//        &(my_endpoint_->domain), nullptr);
//    check_err(ret, "Couldn't open a fabric access domain");

//    // Create transport level communication endpoint(s)
    ret = fi_endpoint(my_endpoint_->domain, ofi_provider_, &(my_endpoint_->ep), nullptr);
//    check_err(ret, "Couldn't allocate endpoint");


    // DO WE NEED CQ attribute in FabricEndpoint ??

    struct fi_cq_attr cq_attr = {};
    cq_attr.format = FI_CQ_FORMAT_TAGGED;
    ret = fi_cq_open(my_endpoint_->domain, &cq_attr, &my_endpoint_->cq, nullptr);
//    check_err(ret, "Couldn't open CQ");



    struct fi_av_attr av_attr = {};
    av_attr.type = FI_AV_TABLE;
    ret = fi_av_open(my_endpoint_->domain, &av_attr, &my_endpoint_->av, nullptr);
//    check_err(ret, "Couldn't open AV");
//
    // Bind CQ and AV to endpoint
    ret = fi_ep_bind(my_endpoint_->ep, (fid_t)my_endpoint_->cq, FI_SEND | FI_RECV);
//    check_err(ret, "Couldn't bind EP-CQ");
    ret = fi_ep_bind(my_endpoint_->ep, (fid_t)my_endpoint_->av, 0);
//    check_err(ret, "Couldn't bind EP-CQ");
//
    // Enable endpoint for communication
    ret = fi_enable(my_endpoint_->ep);
//    check_err(ret, "Couldn't enable endpoint");

    // Get endpoint name
    ret = fi_getname(&(my_endpoint_->ep->fid), (void *)&av_name_, &av_name_len_);
//    check_err(ret, "Call to fi_getname() failed");
  }
*/

  void OnConnected(Node node) {
    // process and insert av_name and av_name_len;
  	fi_addr_t remote_addr;
  	std::string remote_endpoint = node.hostname;
  	int ret = fi_av_insert(my_endpoint_->av, (void *) remote_endpoint.c_str(), 1,
	                           &remote_addr, 0, nullptr);
//    fi_av_insert(struct fid_av *av, void *addr, size_t count
//                 fi_addr_t *fi_addr, uint64_t flags, void *context);
    my_endpoint_->status = FabricEndpoint::CONNECTED;
    my_endpoint_->cv.notify_all();
    // see how RDMA sets cv in Endpoint()
    // endpoint->cv.notify_all();
  }

  int ZMQBind(const Node& node, int max_retry) /*override*/ {
    receiver_ = zmq_socket(context_, ZMQ_ROUTER);
    int option = 1;
    CHECK(!zmq_setsockopt(receiver_, ZMQ_ROUTER_MANDATORY, &option, sizeof(option)))
        << zmq_strerror(errno);
    CHECK(receiver_ != NULL)
        << "create receiver socket failed: " << zmq_strerror(errno);
    int local = GetEnv("DMLC_LOCAL", 0);
    std::string hostname = node.hostname.empty() ? "*" : node.hostname;
    int use_kubernetes = GetEnv("DMLC_USE_KUBERNETES", 0);
    if (use_kubernetes > 0 && node.role == Node::SCHEDULER) {
      hostname = "0.0.0.0";
    }
    std::string addr = local ? "ipc:///tmp/" : "tcp://" + hostname + ":";
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL) + port);
    for (int i = 0; i < max_retry + 1; ++i) {
      auto address = addr + std::to_string(port);
      if (zmq_bind(receiver_, address.c_str()) == 0) break;
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    std::lock_guard<std::mutex> lk(mu_);
    is_worker_ = (node.role == Node::WORKER ? true : false);
    auto t = new std::thread(&FabricRMAVan::CallZmqRecvThread, this, (void*) receiver_);
    thread_list_.push_back(t);

    return port;
  }

  void Connect(const Node &node) override {
    ZmqConnect(node);

    Message req;
    Node key;
	  key.role = node.role;
	  key.hostname = node.hostname; // My IP Address
	  key.port = node.port; // My Port
	  req.meta.control.cmd = Control::ADDR_REQUEST;
	  req.meta.control.node.push_back(key);

	  ZmqSendMsg(req);
    PS_VLOG(1) << "Connecting to " << my_node_.ShortDebugString();
//    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }

    std::string node_host_ip = node.hostname + ":" + std::to_string(node.port);
    if (!node_host_ip.empty()) {
      auto it = endpoints_.find(node_host_ip);

      // if there is an endpoint with pending connection
      if (it != endpoints_.end()) {
        endpoints_.erase(it);
      }

      FabricEndpoint *endpoint;
      endpoints_[node_host_ip] = std::make_unique<FabricEndpoint>();
      endpoint = endpoints_[node_host_ip].get();

//      endpoint->SetNodeID(node.id);

      struct addrinfo *remote_addr;
      CHECK_EQ(
          getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(),
                      nullptr, &remote_addr),
          0);

      while (endpoint->status != FabricEndpoint::CONNECTED) {
        std::unique_lock<std::mutex> lk(endpoint->connect_mu);
        endpoint->status = FabricEndpoint::CONNECTING;

//        if (endpoint->cm_id != nullptr) {
//          rdma_destroy_qp(endpoint->cm_id);
//          CHECK_EQ(rdma_destroy_id(endpoint->cm_id), 0) << strerror(errno);
//          endpoint->cm_id = nullptr;
//        }

//        CHECK_EQ(rdma_create_id(event_channel_, &endpoint->cm_id, nullptr,
//                                RDMA_PS_TCP),
//                 0)
//            << "Create RDMA connection identifier failed";
//        endpoint->cm_id->context = endpoint;

//        int max_retry = kMaxResolveRetry;
        int port = kBasePort;
        unsigned seed = static_cast<unsigned>(time(NULL) + port);
        auto val = Environment::Get()->find("DMLC_NODE_HOST");
//        if (val) {
//          struct sockaddr_in addr;
//          memset(&addr, 0, sizeof(addr));
//          addr.sin_addr.s_addr = inet_addr(val);
//          addr.sin_family = AF_INET;
//          for (int i = 0; i < max_retry + 1; ++i) {
//            addr.sin_port = htons(port);
//            if (rdma_resolve_addr(endpoint->cm_id,
//                                  reinterpret_cast<struct sockaddr *>(&addr),
//                                  remote_addr->ai_addr, kTimeoutms) == 0) {
//              break;
//            }
//            if (i == max_retry) {
//              port = -1;
//            } else {
//              port = 10000 + rand_r(&seed) % 40000;
//            }
//          }
//        } else {
//          CHECK_EQ(rdma_resolve_addr(endpoint->cm_id, nullptr,
//                                     remote_addr->ai_addr, kTimeoutms),
//                   0)
//              << "Resolve RDMA address failed with errno: " << strerror(errno);
//        }

        endpoint->cv.wait(lk, [endpoint] {
          return endpoint->status != FabricEndpoint::CONNECTING;
        });

        if (endpoint->status == FabricEndpoint::CONNECTED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }

      freeaddrinfo(remote_addr);
    }

    std::thread(PollCQ());
  }

  bool IsValidPushpull(const Message &msg) {
    if (!msg.meta.control.empty()) return false;
    if (msg.meta.simple_app) return false;
    return true;
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



  int SendMsg(Message &msg) override {
  	return 0;
  }

  int RecvMsg(Message *msg) override {
     return 0;
  }

 private:
  void InitContext(struct ibv_context *context) {
//    context_ = context;
//    CHECK(context_) << "ibv_context* empty";
//
//    pd_ = ibv_alloc_pd(context_);
//    CHECK(pd_) << "Failed to allocate protection domain";
//
//    mempool_.reset(new SimpleMempool(pd_));
//
//    comp_event_channel_ = ibv_create_comp_channel(context_);
//
//    // TODO(clan): Replace the rough estimate here
//    cq_ = ibv_create_cq(context_, kMaxConcurrentWorkRequest * 2, NULL,
//                        comp_event_channel_, 0);
//
//    CHECK(cq_) << "Failed to create completion queue";
//    CHECK(!ibv_req_notify_cq(cq_, 0)) << "Failed to request CQ notification";
  }

  void ReleaseWorkRequestContext(WRContext *context, Endpoint *endpoint) {
    switch (context->type) {
      case kRendezvousStartContext:
        endpoint->free_start_ctx.Push(context);
        break;
      case kRendezvousReplyContext:
        endpoint->free_reply_ctx.Push(context);
        break;
      case kWriteContext:
        endpoint->free_write_ctx.Push(context);
        break;
      case kReceiveContext:
        endpoint->PostRecv(context);
        break;
      default:
        CHECK(0);
    }
  }

  void PollCQ() {
    // Pre-allocated work completions array used for polling
//    struct ibv_wc wc[kMaxConcurrentWorkRequest];
//    while (!should_stop_.load()) {
//      int ne = ibv_poll_cq(cq_, kMaxConcurrentWorkRequest, wc);
//      CHECK_GE(ne, 0);
//      for (int i = 0; i < ne; ++i) {
//        CHECK(wc[i].status == IBV_WC_SUCCESS)
//            << "Failed status \n"
//            << ibv_wc_status_str(wc[i].status) << " " << wc[i].status << " "
//            << static_cast<uint64_t>(wc[i].wr_id) << " " << wc[i].vendor_err;
//
//        WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);
//        Endpoint *endpoint =
//            reinterpret_cast<Endpoint *>(context->private_data);
//
//        CHECK(endpoint);
//
//        switch (wc[i].opcode) {
//          case IBV_WC_SEND:
//            // LOG(INFO) << "opcode: IBV_WC_SEND";
//            ReleaseWorkRequestContext(context, endpoint);
//            break;
//          case IBV_WC_RDMA_WRITE: {
//            // LOG(INFO) << "opcode: IBV_WC_RDMA_WRITE";
//            // Note: This is not a struct ibv_mr*
//            MessageBuffer *msg_buf =
//                *reinterpret_cast<MessageBuffer **>(context->buffer->addr);
//            mempool_->Free(msg_buf->inline_buf);
//            delete msg_buf;
//            ReleaseWorkRequestContext(context, endpoint);
//          } break;
//          case IBV_WC_RECV_RDMA_WITH_IMM: {
//            // LOG(INFO) << "opcode: IBV_WC_RECV_RDMA_WITH_IMM";
//            uint32_t addr_idx = wc[i].imm_data;
//            BufferContext *buf_ctx = addr_pool_.GetAddressAndRelease(addr_idx);
//            recv_buffers_.Push(std::make_tuple(endpoint, buf_ctx));
//            ReleaseWorkRequestContext(context, endpoint);
//          } break;
//          case IBV_WC_RECV: {
//            CHECK(wc[i].wc_flags & IBV_WC_WITH_IMM);
//            uint32_t imm = wc[i].imm_data;
//            struct ibv_mr *mr = context->buffer;
//
//            if (imm == kRendezvousStart) {
//              // LOG(INFO) << "opcode: IBV_WC_RECV kRendezvousStart";
//              RendezvousStart *req =
//                  reinterpret_cast<RendezvousStart *>(mr->addr);
//              BufferContext *buf_ctx = new BufferContext();
//
//              uint64_t len = req->meta_len;
//              buf_ctx->meta_len = req->meta_len;
//              buf_ctx->data_num = req->data_num;
//              for (size_t i = 0; i < req->data_num; ++i) {
//                buf_ctx->data_len[i] = req->data_len[i];
//                len += req->data_len[i];
//              }
//
//              char *buffer = mempool_->Alloc(is_server ? len : req->meta_len);
//              CHECK(buffer) << "Alloc for " << len
//                            << " bytes, data_num: " << req->data_num;
//
//              buf_ctx->buffer = buffer;
//
//              uint64_t origin_addr = req->origin_addr;
//
//              WRContext *reply_ctx = nullptr;
//              endpoint->free_reply_ctx.WaitAndPop(&reply_ctx);
//              RendezvousReply *resp =
//                  reinterpret_cast<RendezvousReply *>(reply_ctx->buffer->addr);
//
//              resp->addr = reinterpret_cast<uint64_t>(buffer);
//              resp->rkey = mempool_->RemoteKey(buffer);
//              resp->origin_addr = origin_addr;
//              resp->idx = addr_pool_.StoreAddress(buf_ctx);
//
//              struct ibv_sge sge;
//              sge.addr = reinterpret_cast<uint64_t>(resp);
//              sge.length = sizeof(RendezvousReply);
//              sge.lkey = reply_ctx->buffer->lkey;
//
//              struct ibv_send_wr wr, *bad_wr = nullptr;
//              memset(&wr, 0, sizeof(wr));
//
//              wr.wr_id = reinterpret_cast<uint64_t>(reply_ctx);
//              wr.opcode = IBV_WR_SEND_WITH_IMM;
//              wr.next = nullptr;
//
//              wr.imm_data = kRendezvousReply;
//
//              wr.send_flags = IBV_SEND_SIGNALED;
//              wr.sg_list = &sge;
//              wr.num_sge = 1;
//
//              CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
//                  << "ibv_post_send failed.";
//
//            } else if (imm == kRendezvousReply) {
//              // LOG(INFO) << "opcode: IBV_WC_RECV kRendezvousReply";
//              RendezvousReply *resp =
//                  reinterpret_cast<RendezvousReply *>(mr->addr);
//              uint64_t remote_addr = resp->addr;
//              uint64_t origin_addr = resp->origin_addr;
//              uint32_t rkey = resp->rkey;
//              uint32_t idx = resp->idx;
//
//              MessageBuffer *msg_buf =
//                  reinterpret_cast<MessageBuffer *>(origin_addr);
//
//              struct ibv_sge sge[1 + msg_buf->mrs.size()];
//
//              sge[0].addr = reinterpret_cast<uint64_t>(msg_buf->inline_buf);
//              sge[0].length = msg_buf->inline_len;
//              sge[0].lkey = mempool_->LocalKey(msg_buf->inline_buf);
//
//              size_t num_sge = 1;
//              for (auto &pair : msg_buf->mrs) {
//                size_t length = pair.second;
//                CHECK(length);
//                sge[num_sge].addr =
//                    reinterpret_cast<uint64_t>(pair.first->addr);
//                sge[num_sge].length = length;
//                sge[num_sge].lkey = pair.first->lkey;
//                ++num_sge;
//              }
//              if (is_server) CHECK_EQ(num_sge, 1) << num_sge;
//
//              WRContext *write_ctx = msg_buf->reserved_context;
//
//              MessageBuffer **tmp =
//                  reinterpret_cast<MessageBuffer **>(write_ctx->buffer->addr);
//              *tmp = msg_buf;  // write the addr of msg_buf into the mr buffer
//
//              struct ibv_send_wr wr, *bad_wr = nullptr;
//              memset(&wr, 0, sizeof(wr));
//
//              wr.wr_id = reinterpret_cast<uint64_t>(write_ctx);
//              wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
//              wr.next = nullptr;
//
//              wr.imm_data = idx;
//
//              wr.send_flags = IBV_SEND_SIGNALED;
//              wr.sg_list = sge;
//              wr.num_sge = num_sge;
//
//              wr.wr.rdma.remote_addr = remote_addr;
//              wr.wr.rdma.rkey = rkey;
//
//              CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
//                  << "ibv_post_send failed.";
//
//            } else {
//              CHECK(0);
//            }
//            ReleaseWorkRequestContext(context, endpoint);
//          } break;
//          default:
//            CHECK(0) << "Unexpected opcode: " << wc[i].opcode;
//        }
//      }
//    }
  }

  void PollEvents() {
//    int flags = fcntl(event_channel_->fd, F_GETFL);
//    int rc = fcntl(event_channel_->fd, F_SETFL, flags | O_NONBLOCK);
//    CHECK_GE(rc, 0);
//    int error_flags = POLLERR | POLLHUP | POLLNVAL;
//
//    while (!should_stop_.load()) {
//      struct pollfd pfd = {
//          .fd = event_channel_->fd, .events = POLLIN, .revents = 0};
//      int ret = poll(&pfd, 1, 10);
//
//      CHECK_GE(ret, 0) << strerror(errno);
//      CHECK_EQ(pfd.revents & error_flags, 0);
//
//      if (!(pfd.revents & POLLIN)) {
//        continue;
//      }
//
//      struct rdma_cm_event *event;
//      CHECK_EQ(rdma_get_cm_event(event_channel_, &event), 0);
//      // TODO(clan): Reorder the list according to the event frequency
//      switch (event->event) {
//        case RDMA_CM_EVENT_CONNECT_REQUEST:
//          OnConnectRequest(event);
//          break;
//        case RDMA_CM_EVENT_ADDR_RESOLVED:
//          OnAddrResolved(event);
//          break;
//        case RDMA_CM_EVENT_ROUTE_RESOLVED:
//          OnRouteResolved(event);
//          break;
//        case RDMA_CM_EVENT_ESTABLISHED:
//          OnConnected(event);
//          break;
//        case RDMA_CM_EVENT_DISCONNECTED:
//          OnDisconnected(event);
//          break;
//        case RDMA_CM_EVENT_REJECTED:
//          OnRejected(event);
//          break;
//        default:
//          CHECK(0) << "OnEvent: unknown event " << event->event << " ("
//                   << rdma_event_str(event->event) << ")";
//      }
//      rdma_ack_cm_event(event);
//    }
  }

  void OnRejected(struct rdma_cm_event *event) {
//    struct rdma_cm_id *id = event->id;
//    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
//
//    auto it = endpoints_.find(endpoint->node_id);
//    CHECK(it != endpoints_.end()) << "Connection not ready.";
//    CHECK_EQ(endpoint->status, Endpoint::CONNECTING);
//    CHECK_EQ(endpoint->cm_id, id);
//
//    PS_VLOG(1) << "Connection rejected, retrying...";
//    {
//      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
//      endpoint->status = Endpoint::REJECTED;
//    }
//    endpoint->cv.notify_all();
//  }
//
//  void OnConnectRequest(struct rdma_cm_event *event) {
//    struct rdma_cm_id *id = event->id;
//    CHECK_NOTNULL(id);
//
//    CHECK_LE(sizeof(RequestContext), event->param.conn.private_data_len)
//        << "RequestContext size mismatch. Actual: "
//        << (size_t)event->param.conn.private_data_len
//        << ", Expected: " << sizeof(RequestContext);
//    CHECK_NOTNULL(event->param.conn.private_data);
//
//    const RequestContext *remote_ctx = reinterpret_cast<const RequestContext *>(
//        event->param.conn.private_data);
//
//    const auto r = incoming_.emplace(std::make_unique<Endpoint>());
//    Endpoint *endpoint = r.first->get();
//    endpoint->SetNodeID(remote_ctx->node);
//    endpoint->cm_id = id;
//    id->context = endpoint;
//
//    if (context_ == nullptr) {
//      InitContext(id->verbs);
//    }
//
//    endpoint->Init(cq_, pd_);
//
//    RequestContext ctx;
//    ctx.node = static_cast<uint32_t>(my_node_.id);
//    ctx.port = static_cast<uint16_t>(my_node_.port);
//    snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());
//
//    struct rdma_conn_param cm_params;
//    memset(&cm_params, 0, sizeof(cm_params));
//    cm_params.retry_count = 7;
//    cm_params.rnr_retry_count = 7;
//    cm_params.private_data = &ctx;
//    cm_params.private_data_len = sizeof(RequestContext);
//
//    CHECK_EQ(rdma_accept(id, &cm_params), 0)
//        << "Accept RDMA connection failed: " << strerror(errno);
  }

  // Resolve a route after address is resolved
  void OnAddrResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK_EQ(rdma_resolve_route(id, kTimeoutms), 0)
        << "Resolve RDMA route failed";
  }

  // Make a connection after route is resolved
  void OnRouteResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);

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

    CHECK_EQ(rdma_connect(id, &cm_params), 0)
        << "RDMA connect failed" << strerror(errno);
  }

  void OnConnected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK(id) << "rdma_cm_id not found.";
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    CHECK(endpoint) << "Endpoint not found.";

    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&FabricRMAVan::PollCQ, this));
    }

    CHECK_EQ(endpoint->cm_id, id);
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::CONNECTED;
    }
    endpoint->cv.notify_all();
  }


  void OnDisconnected(struct rdma_cm_event *event) {
    LOG(INFO) << "OnDisconnected from Node " << my_node_.id;
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::IDLE;
    }
    endpoint->cv.notify_all();
  }

  AddressPool<BufferContext> addr_pool_;
  std::unique_ptr<SimpleMempool> mempool_;

  struct rdma_cm_id *listener_ = nullptr;
  std::atomic<bool> should_stop_;

//  std::unordered_map<int, std::unique_ptr<Endpoint>> endpoints_;
  std::unordered_set<std::unique_ptr<Endpoint>> incoming_;

  struct rdma_event_channel *event_channel_ = nullptr;
//  struct ibv_context *context_ = nullptr;

  std::unordered_map<char *, struct ibv_mr *> memory_mr_map;

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
//  ThreadsafeQueue<std::tuple<Endpoint *, BufferContext *>> recv_buffers_;

  // JYM: the following are for push/pull buffer reuse

  // whether my role is server or not
  bool is_server;
  // RDMA logging info
  bool enable_rdma_log_;

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


//  FabricEndpoint* my_endpoint_ = nullptr;

};  // namespace ps
};  // namespace ps

//#endif  // DMLC_USE_RDMA
#endif  // PS_FABRIC_RMA_VAN_H_
