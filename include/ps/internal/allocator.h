/**
 *  Copyright (c) 2018 by Junxue Zhang, Jingrong Chen
 */
#ifndef PS_INTERNAL_ALLOCATOR_H_
#define PS_INTERNAL_ALLOCATOR_H_
#ifdef MXNET_USE_RDMA

#include <rdma/rdma_cma.h>
#include <memory>
#include <unordered_map>
#include <utility>
#include "ps/internal/bfc_allocator.h"

static const uint64_t kDefaultSize = 1ll * (1024 * 1024 * 1024);

class Allocator {
 public:
  virtual void *Allocate(size_t size) { return nullptr; }
  virtual void Deallocate(void *data) {}
  virtual bool in_range(void *addr, size_t size) { return false; }
  virtual void Register(void *addr, size_t size) {}
};

class DefaultAllocator : public Allocator {
 public:
  void *Allocate(size_t size) override { return reinterpret_cast<void *>(new char[size]); }
  void Deallocate(void *data) override { delete[] reinterpret_cast<char *>(data); }
};

/**
 * \breif NIC Allocator
 */
class NICAllocator : public Allocator {
 public:
  static NICAllocator *GetNICAllocator() {
    static NICAllocator region_manager_;
    return &region_manager_;
  }

  NICAllocator() {
    ptr_ = malloc(kDefaultSize);
    allocator_ = BFCAllocator::Create(1, kDefaultSize);
  }

  ~NICAllocator() {
    free(ptr_);
    allocator_ = nullptr;
    for (const auto &e : addr_mr_) ibv_dereg_mr(e.second.first);
  }

  void *Allocate(size_t size) override {
    if (size < 256) size = 256;
    int64_t offset = allocator_->Allocate(size);
    CHECK(offset != BFCAllocator::k_invalid_offset) << "apply for size: " << size;
    return static_cast<void *>(static_cast<char *>(ptr_) + offset);
  }

  void Deallocate(void *data) override { allocator_->Deallocate(addr2offset(data)); }

  inline bool registered(void *addr, size_t size) {
    auto it = addr_mr_.find(addr);
    return it != addr_mr_.end() && it->second.second >= size;
  }

  void Register(void *addr, size_t size) override {
    addr_mr_[addr] = std::make_pair(ibv_reg_mr(pd_, addr, size, access_flag_), size);
    CHECK(addr_mr_[addr].first) << "register region failed";
  }

  bool in_range(void *addr, size_t size) override {
    uintptr_t a = (uintptr_t)addr;
    uintptr_t l = (uintptr_t)ptr_;
    return (l <= a && a + size <= l + kDefaultSize) || registered(addr, size);
  }

  void set_pd(struct ibv_pd *pd) { pd_ = pd; }

  inline struct ibv_mr *mr(void *addr) { return addr_mr_[addr].first; }
  inline void *ptr() const { return ptr_; }

  void *ptr_;
  std::shared_ptr<BFCAllocator> allocator_;
  std::unordered_map<void *, std::pair<struct ibv_mr *, size_t>> addr_mr_;

 private:
  int64_t addr2offset(void *addr) {
    CHECK(in_range(addr, 0)) << "address not in memory region";
    return static_cast<int64_t>(static_cast<char *>(addr) - static_cast<char *>(ptr_));
  }

  struct ibv_pd *pd_;
  static const int access_flag_ = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
};

#endif  // MXNET_USE_RDMA
#endif  // PS_INTERNAL_ALLOCATOR_H_
