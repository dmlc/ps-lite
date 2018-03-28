/**
 *  Copyright (c) 2018 by Jingrong Chen
 */
#ifndef PS_SRMEM_H_
#define PS_SRMEM_H_
#ifdef MXNET_USE_RDMA
#include <string.h>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "ps/internal/allocator.h"
#include "ps/internal/bfc_allocator.h"
#include "ps/internal/utils.h"
#include "ps/range.h"
namespace ps {

template <typename V>
class SArray;

inline Allocator* GetAllocator() { return NICAllocator::GetNICAllocator(); }

/**
 * \breif Shared RDMA Memory region
 *
 * A memory region registered by RDMA use that managed by shared pointer.
 * It is designed to help implement zero-copy RDMA operation.
 * It retains the element type and provides basic functionalities to access
 * these elements. SRMem can be constructed from SArray and c-array,
 * for instances
 *
 * \code
 * SArray<float> sa(10); SRMem<float> sr(sa); // copying
 * \endcode
 *
 * \code
 * char *c = malloc(10);
 * SRMem<int> sr(c, 10);
 * \endcode
 *
 * \tparam V the value type
 */
template <typename V>
class SRMem {
 public:
  /** \brief empty constructor */
  SRMem() {}

  /** \brief empty deconstrcutor */
  ~SRMem() {}

  /**
   * \brief Create an array with length n with initialized value
   * \param size the length
   * \param val the initial length (0 in default)
   */
  explicit SRMem(size_t size, V val = 0) { resize(size, val); }

  /**
   * \brief Constructor from a SArray
   *
   * Simply copy the data
   *
   * \param arr the source array
   */
  explicit SRMem(const SArray<V>& arr) {
    /* when the copy cost is less than doing ibv_reg_mr, copy it */
    if (arr.size() >= 16384 && !GetAllocator()->in_range(arr.data(), arr.size())) {
      GetAllocator()->Register(arr.data(), arr.size());
    }
    /* TODO(cjr) this code will cause problem when the newed memory is same as previous deleted one */
    if (arr.size() >= 16384) {
      reset(arr.data(), arr.size(), [](V* data) {});
    } else {
      CopyFrom(arr.data(), arr.size());
    }
  }

  /**
   * \brief Requests that the capacity be at least enough to contain n elements.
   *
   * Zero-copy constructor, namely just copy the pointer
   *
   * \tparam W the value type of the source array
   * \param arr the source array
   */
  template <typename W>
  explicit SRMem(const SRMem<W>& arr) {
    *this = arr;
  }

  /**
   * \brief construct from another SRMem.
   *
   * Zero-copy constructor, namely just copy the pointer
   *
   * \tparam W the value type of the source array
   * \param arr the source array
   */
  template <typename W>
  void operator=(const SRMem<W>& arr) {
    size_ = arr.size() * sizeof(W) / sizeof(V);
    CHECK_EQ(size_ * sizeof(V), arr.size() * sizeof(W)) << "cannot be divided";
    capacity_ = arr.capacity() * sizeof(W) / sizeof(V);

    CHECK(GetAllocator()->in_range(arr.data(), arr.size())) << "SRMem not in range";
    ptr_ = std::shared_ptr<V>(arr.ptr(), reinterpret_cast<V*>(arr.data()));
  }

  /**
   * \brief construct from a c-array
   *
   * Just copy the pointer if the buffer is registered by RDMA
   * Copy the data otherwise
   *
   * \param data the source data
   * \param size the length
   * \param deletable whether or not can call `NICAllocator::GetNICAllocator()->Deallocate(data)`
   * when the reference count goes 0
   */

  SRMem(V* data, size_t size, bool deletable = false) {
    if (deletable) {
      /* TODO(cjr) here we may do one more copy */
      if (GetAllocator()->in_range(data, size)) {
        reset(data, size, [this](V* data) { GetAllocator()->Deallocate(data); });
      } else {
        V* new_data = reinterpret_cast<V*>(GetAllocator()->Allocate(size * sizeof(V)));
        memcpy(new_data, data, size * sizeof(V));
        reset(new_data, size, [this](V* data) { GetAllocator()->Deallocate(data); });
      }
    } else {
      if (GetAllocator()->in_range(data, size)) {
        reset(data, size, [this](V* data) {});
      } else {
        V* new_data = reinterpret_cast<V*>(GetAllocator()->Allocate(size * sizeof(V)));
        memcpy(new_data, data, size * sizeof(V));
        reset(new_data, size, [](V* data) {});
      }
    }
  }

  template <typename Deleter>
  SRMem(V* data, size_t size, Deleter del) {
    reset(data, size, del);
  }

  /**
   * \brief copy from a c-array
   *
   * \param data the source data
   * \param size the length
   */
  void CopyFrom(const V* data, size_t size) {
    resize(size);
    memcpy(this->data(), data, size * sizeof(V));
  }

  /**
   * \brief copy from another SRMem
   *
   * \param other the source data
   */
  void CopyFrom(const SRMem<V>& other) {
    if (this == &other) return;
    CopyFrom(other.data(), other.size());
  }

  /**
   * \brief copy from an iterator
   */
  template <typename ForwardIt>
  void CopyFrom(const ForwardIt& first, const ForwardIt& last) {
    int size = static_cast<int>(std::distance(first, last));
    V* data = reinterpret_cast<V*>(GetAllocator()->Allocate(size * sizeof(V)));
    reset(data, size, [this](V* data) { GetAllocator()->Deallocate(data); });
    for (auto it = first; it < last; ++it) *data++ = *it;
  }

  /**
   * @brief Reset the current data pointer with a deleter
   */
  template <typename Deleter>
  void reset(V* data, size_t size, Deleter del) {
    size_ = size;
    capacity_ = size;
    ptr_.reset(data, del);
  }

  /**
   * @brief Resizes the array to size elements
   *
   * If size <= capacity_, then only change the size. otherwise, append size -
   * current_size entries, and then set new value to val
   */
  void resize(size_t size, V val = 0) {
    size_t cur_n = size_;
    if (capacity_ >= size) {
      size_ = size;
    } else {
      /* TODO(cjr) FIXME(cjr) when size * sizeof(V) >= REGION_SIZE*/
      V* new_data = reinterpret_cast<V*>(GetAllocator()->Allocate((size + 5) * sizeof(V)));
      memcpy(new_data, data(), size_ * sizeof(V));
      reset(new_data, size, [this](V* data) { GetAllocator()->Deallocate(data); });
    }
    if (size <= cur_n) return;
    V* p = data() + cur_n;
    if (val == 0) {
      memset(p, 0, (size - cur_n) * sizeof(V));
    } else {
      for (; cur_n < size; cur_n++) *p++ = val;
    }
  }

  /**
   * @brief Requests that the capacity be at least enough to contain n elements.
   */
  void reserve(size_t size) {
    if (capacity_ >= size) {
      return;
    }
    size_t old_size = size_;
    resize(size);
    size_ = old_size;
  }

  /** @brief release the memory */
  void clear() {
    reset(nullptr, 0, [](V* data) {});
  }

  inline bool empty() const { return size() == 0; }
  inline size_t size() const { return size_; }
  inline size_t capacity() const { return capacity_; }
  inline size_t bytesize() const { return size_ * sizeof(V); }

  inline V* begin() { return data(); }
  inline const V* begin() const { return data(); }
  inline V* end() { return data() + size(); }
  inline const V* end() const { return data() + size(); }

  inline V* data() const { return ptr_.get(); }

  /** \brief get the shared pointer */
  inline std::shared_ptr<V>& ptr() { return ptr_; }
  /** \brief get the const shared pointer */
  inline const std::shared_ptr<V>& ptr() const { return ptr_; }

  inline V back() const {
    CHECK(!empty());
    return data()[size_ - 1];
  }
  inline V front() const {
    CHECK(!empty());
    return data()[0];
  }
  inline V& operator[](int i) { return data()[i]; }
  inline const V& operator[](int i) const { return data()[i]; }

  inline void push_back(const V& val) {
    if (size_ == capacity_) reserve(size_ * 2 + 5);
    data()[size_++] = val;
  }

  void pop_back() {
    if (size_) --size_;
  }

 private:
  size_t size_ = 0;
  size_t capacity_ = 0;
  std::shared_ptr<V> ptr_;
};

/**
 * \brief print a debug string
 */
template <typename V>
std::ostream& operator<<(std::ostream& os, const SRMem<V>& obj) {
  os << DebugStr(obj.data(), obj.size());
  return os;
}

}  // namespace ps
#endif  // MXNET_USE_RDMA
#endif  // PS_SRMEM_H_
