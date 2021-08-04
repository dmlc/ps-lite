// Copyright 2019 Amazon Web Services Inc. or its affiliates. All Rights
// Reserved.
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
#ifndef PS_RDMA_COMMON_H_
#define PS_RDMA_COMMON_H_

#if defined(DMLC_USE_RDMA) || defined(DMLC_USE_FABRIC)
#include <unistd.h>

#define DIVUP(x, y) (((x) + (y)-1) / (y))
#define ROUNDUP(x, y) (DIVUP((x), (y)) * (y))

namespace ps {

static const int kMaxDataFields = 4;
static const int kMaxAddressEntries = 10240;

template <typename T>
static inline T align_floor(T v, T align) {
  return v - (v % align);
}

template <typename T>
static inline T align_ceil(T v, T align) {
  return align_floor(v + align - 1, align);
}

enum MessageTypes : uint32_t {
  kRendezvousStart,
  kRendezvousReply,
};

static inline void aligned_malloc(void **ptr, size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  void *p;
  int size_aligned = ROUNDUP(size, page_size);
  int ret = posix_memalign(&p, page_size, size_aligned);
  CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
  CHECK(p);
  memset(p, 0, size);
  *ptr = p;
}

bool IsValidPushpull(const Message &msg) {
  if (!msg.meta.control.empty()) return false;
  if (msg.meta.simple_app) return false;
  return true;
}

// just a translation, the decoded key might not be
// readable when we have multiple servers
uint64_t DecodeKey(SArray<char> keys) {
  ps::Key key = 0;
  uint64_t coef = 1;
  for (unsigned int i = 0; i < keys.size(); ++i) {
    key += coef * (uint8_t)keys.data()[i];
    coef *= 256;  // 256=2^8 (uint8_t)
  }
  return key;
}

template <typename T>
class AddressPool {
 public:
  AddressPool() {
    auto addrpool_size = Environment::Get()->find("BYTEPS_ADDRESS_POOL_SIZE");
    kMaxEntries = addrpool_size ? atoi(addrpool_size) : kMaxEntries;
    std::lock_guard<std::mutex> lk(mu_);
    table_ = new T *[kMaxEntries];
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
  int kMaxEntries = kMaxAddressEntries;

  std::mutex mu_;
  std::queue<uint32_t> indices_;
  T **table_;
};

};  // namespace ps

#endif  // DMLC_USE_RDMA || DMLC_USE_FABRIC
#endif  // PS_RDMA_COMMON_H_
