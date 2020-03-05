// Copyright 2019 Bytedance Inc. or its affiliates. All Rights Reserved.
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

#ifndef PS_RDMA_UTILS_H_
#define PS_RDMA_UTILS_H_

#ifdef DMLC_USE_RDMA


#include <errno.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <netdb.h>
#include <poll.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

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


namespace ps {


#define DIVUP(x, y) (((x)+(y)-1)/(y))
#define ROUNDUP(x, y) (DIVUP((x), (y))*(y))

static const int kStartDepth = 128;
static const int kRxDepth = 2048; // should be larger than kStartDepth
static const int kReplyDepth = kRxDepth;

static const int kSGEntry = 1;
static const int kTimeoutms = 1000;
static const int kRdmaListenBacklog = 128;
static const int kMaxConcurrentWorkRequest =
    kRxDepth + kStartDepth + kReplyDepth;
static const int kMaxHostnameLength = 16;
static const int kMaxDataFields = 4;

static const int kMaxResolveRetry = 50000;
static const int kBasePort = 9010;

// should have the same prefix with BytePS shared memory
static const std::string kShmPrefix("BytePS_ShM_");
static const std::string kShmPciePrefix("BytePS_Pcie");

template <typename T>
static inline T align_floor(T v, T align) {
  return v - (v % align);
}

template <typename T>
static inline T align_ceil(T v, T align) {
  return align_floor(v + align - 1, align);
}

static inline void ib_malloc(void** ptr, size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  void* p;
  int size_aligned = ROUNDUP(size, page_size);
  int ret = posix_memalign(&p, page_size, size_aligned);
  CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
  CHECK(p);
  memset(p, 0, size);
  *ptr = p;
}

class MemoryAllocator {
 public:
  explicit MemoryAllocator(struct ibv_pd *pd) {
    std::lock_guard<std::mutex> lk(mu_);
    pd_ = pd;
  }

  ~MemoryAllocator() {
    std::lock_guard<std::mutex> lk(mu_);
    for(auto &it : mr_) {
      CHECK_EQ(ibv_dereg_mr(it.second), 0);
      free(it.first);
    }
  }

  char *Alloc(size_t size) {
    if (size == 0) {
      return nullptr;
    }

    // align to page size (usually 4KB)
    size = align_ceil(size, pagesize_);

    char *p;
    ib_malloc((void**) &p, size);
    CHECK(p);

    struct ibv_mr *mr;
    CHECK(mr = ibv_reg_mr(pd_, p, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
    
    std::lock_guard<std::mutex> lk(mu_);
    mr_[p] = mr;
    used_list.emplace(p, size);

    return p;
  }

  uint32_t LocalKey(char *addr) {
    return Addr2MR(addr)->lkey;
  }

  uint32_t RemoteKey(char *addr) {
    return Addr2MR(addr)->rkey;
  }

  struct ibv_pd* GetPD() {
    return pd_;
  }

 private:
  // convert the memory address to its associated RDMA memory region
  inline struct ibv_mr* Addr2MR(char *addr) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = mr_.find(addr);
    CHECK_NE(it, mr_.end()) 
        << "cannot find the associated memory region";

    return it->second;
  }

  std::mutex mu_;
  struct ibv_pd *pd_;
  size_t pagesize_ = sysconf(_SC_PAGESIZE);
  std::unordered_map<char *, size_t> used_list;
  std::unordered_map<char *, struct ibv_mr *> mr_;
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

// <remote_addr, rkey, idx, local_addr> 
typedef std::tuple<uint64_t, uint32_t, uint32_t, MessageBuffer*> RemoteTuple;  

// recver, <remote_addr, rkey, idx> 
typedef std::unordered_map<int, RemoteTuple> RemoteAndLocalAddress;  

static_assert(std::is_pod<RendezvousStart>::value,
              "RendezvousStart must be a POD type.");
static_assert(std::is_pod<RendezvousReply>::value,
              "RendezvousReply must be a POD type.");
static_assert(std::is_pod<RequestContext>::value,
              "RequestContext must be a POD type.");

static const size_t kMempoolChunkSize =
    std::max({sizeof(RendezvousStart), sizeof(RendezvousReply)});

template <typename T>
class AddressPool {
 public:
  AddressPool() {
    auto addrpool_size = Environment::Get()->find("BYTEPS_ADDRESS_POOL_SIZE");
    kMaxEntries = addrpool_size ? atoi(addrpool_size) : kMaxEntries;
    std::lock_guard<std::mutex> lk(mu_);
    table_ = new T*[kMaxEntries];
    // init the queue
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
  
  // TODO: make the address pool size dynamic
  T *GetAddress(uint32_t index) {
    std::lock_guard<std::mutex> lk(mu_);
    return CHECK_NOTNULL(table_[index]);
  }

  uint32_t StoreAddress(T *ptr) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(ptr);
    CHECK(!indices_.empty())
      << "Address pool size is too small, "
      << "current size is " << kMaxEntries
      << ", consider increasing BYTEPS_ADDRESS_POOL_SIZE";
    uint32_t idx = indices_.front();
    indices_.pop();
    CHECK_EQ(table_[idx], nullptr) << idx;
    table_[idx] = ptr;
    return idx;
  }

 private:
  int kMaxEntries = 10240;

  std::mutex mu_;
  std::queue<uint32_t> indices_;
  T **table_;
};

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

uint64_t DecodeWorkerKey(uint64_t key) {
  auto kr = ps::Postoffice::Get()->GetServerKeyRanges()[ps::Postoffice::Get()->my_rank()];
  return key - kr.begin();
}

int AlignTo(int input, int alignment) { return input / alignment * alignment; }
int DivUp(int x, int y) { return (x + y - 1) / y; }
int RoundUp(int x, int y) { return DivUp(x, y) * y; }

};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_

