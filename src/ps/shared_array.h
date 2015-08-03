#pragma once
#include <memory>
#include "blob.h"
#include "base/range.h"
namespace ps {

/// \brief An empty deleter for \ref SArray
template<typename T> struct EmptyDel {
  void operator()(T* p) const { }
};

/// \brief An array deleter for \ref SArray
template<typename T> struct ArrayDel {
  void operator()(T* p) const { delete [] p; }
};

/**
 * @brief Shared array
 *
 * It's a wrapper of C array point with std::shared_ptr. The data pointed is
 * guaranteed to be deleted when the last SArray is deleted or reseted. It
 * memory efficient. Most operations are zero-copy, such as assign, slice a
 * segment, convert to Eigen3 vector/array. It shares the same semantic as a C
 * array pointer. For example,
 * \code{.cpp}
   SArray<int> A(10);
   SArray<int> B = A;
   SArray<int> C = A.Segment(1, 3);
   A[2] = 2;
   CHECK_EQ(B[2], 2);
   CHECK_EQ(C[1], 2);
   \endcode
 */
template<typename V> class SArray {
 public:
  SArray() { }
  ~SArray() { }

  /** @brief Create an array with length n. Values are not initialized. */
  explicit SArray(size_t n) { resize(n); }

  /** @brief Create an array with length n, and values are initialized to val */
  SArray(size_t n, V val) { resize(n, val); }

  /** @brief Zero-copy constructor, namely just copy the pointer */
  template <typename W> explicit SArray(const SArray<W>& arr) {
    *this = arr;
  }

  /** @brief Zero-copy constructor, namely just copy the pointer */
  template <typename W> void operator=(const SArray<W>& arr) {
    size_ = arr.size() * sizeof(W) / sizeof(V);
    capacity_ = arr.capacity() * sizeof(W) / sizeof(V);
    ptr_ = std::shared_ptr<V>(arr.ptr(), reinterpret_cast<V*>(arr.data()));
  }

  /** @brief Zero-copy constructor. */
  template<typename Deleter>
  SArray(V* data, size_t size, Deleter d) {
    reset(data, size, d);
  }

  /** @brief Zero-copy constructor. */
  explicit SArray(const std::shared_ptr<std::vector<V> >& vec) {
    size_ = vec.get()->size();
    capacity_ = size_;
    ptr_ = std::shared_ptr<V>(vec, vec.get()->data());
  }

  /** @brief Copy constructor */
  explicit SArray(const std::vector<V>& vec) {
    CopyFrom(vec.data(), vec.size());
  }

  /** @brief Copy constructor */
  void CopyFrom(const V* src, size_t size) {
    resize(size);
    memcpy(data(), src, size*sizeof(V));
  }

  /** @brief Copy constructor */
  void CopyFrom(const SArray<V>& arr) {
    CopyFrom(arr.data(), arr.size());
  }

  /** @brief A general but might slower version of copy constructor */
  template <typename ForwardIt>
  void CopyFrom(const ForwardIt first, const ForwardIt last) {
    size_t size = std::distance(first, last);
    V* data = new V[size + 5];
    for (size_t i = 0; i < size; ++i) {
      data[i] = *(first+i);
    }
    reset(data, size, ArrayDel<V>());
  }

  /** @brief Copy from a initializer_list */
  template <typename W> SArray(const std::initializer_list<W>& list) {
    CopyFrom(list.begin(), list.end());
  }

  /** @brief Copy from a initializer_list */
  template <typename W> void operator=(const std::initializer_list<W>& list) {
    CopyFrom(list.begin(), list.end());
  }

  /**
   * @brief Reset the current data pointer with a deleter
   */

  template<typename Deleter>
  void reset(V* data, size_t size, Deleter d) {
    size_ = size;
    capacity_ = size;
    ptr_.reset(data, d);
  }

  /**
   * @brief Resizes the array to n elements
   *
   * If n <= capacity_, then only change the size. otherwise, append n -
   * current_size entries (without value initialization)
   */
  void resize(size_t n) {
    if (capacity_ >= n) { size_ = n; return; }
    V* new_data = new V[n+5];
    memcpy(new_data, data(), size_*sizeof(V));
    reset(new_data, n, ArrayDel<V>());
  }

  /**
   * @brief Resizes the array to n elements
   *
   * If n <= capacity_, then only change the size. otherwise, append n -
   * current_size entries, and then set new value to val
   */
  void resize(size_t n, V val) {
    size_t cur_n = size_;
    resize(n);
    if (cur_n == 0) {
      SetValue(val);  // might be faster
    } else {
      for (size_t i = cur_n; i < n; ++i) {
        ptr_.get()[i] = val;
      }
    }
  }

  /**
   * @brief Requests that the capacity be at least enough to contain n elements.
   */
  void reserve(size_t n) {
    if (capacity_ >= n) { return; }
    size_t old_size = size_;
    resize(n);
    size_ = old_size;
  }

  /** @brief release the memory */
  void clear() { reset(nullptr, 0, EmptyDel<V>()); }

  Blob<V> blob() const { return Blob<V>(data(), size()); }

  /**
   * @brief Slice a segment, zero-copy
   *
   * @param range the index segment
   * @return the segment [range.begin(), range.end())
   */
  SArray<V> Segment(const Range<size_t>& range) const {
    CHECK(range.valid()); CHECK_LE(range.end(), size());
    SArray<V> ret;
    ret.ptr_ = std::shared_ptr<V>(ptr_, data() + range.begin());
    ret.size_ = range.size();
    ret.capacity_ = range.size();
    return ret;
  }
  SArray<V> Segment(size_t start, size_t end) const {
    return Segment(SizeR(start, end));
  }

  /**
   * @brief Performs set intersection between two sorted arrays.  Assume array
   * values are ordered.
   *
   * An example:
   \code{cpp}
   SArray<int> a{1,2,3,5,6,7,8}, b{3,4,7,10}, c{3,7};
   CHECK_EQ(a.SetIntersection(b), c);
   \endcode
   * @param other
   *
   * @return *this \f$\cap\f$ other
   */
  SArray<V> SetIntersection(const SArray<V>& other) const {
    SArray<V> result(std::min(other.size(), size())+1);
    V* last = std::set_intersection(
        begin(), end(), other.begin(), other.end(), result.begin());
    result.size_ = last - result.begin();
    result.capacity_ = result.size_;
    return result;
  }

  /**
   * @brief Perform set union between two sorted arrays. Assume array values are ordered.
   *
   * An example:
   \code{cpp}
   SArray<int> a{3,5,8,10}, b{5,9,10,11}, c{3,5,8,9,10,11};
   CHECK_EQ(a.SetUnion(b), c)
   \endcode
   * @param other
   *
   * @return *this \f$\cup\f$ other
   */
  SArray<V> SetUnion(const SArray<V>& other) const {
    SArray<V> result(other.size() + size());
    V* last = std::set_union(
        begin(), end(), other.begin(), other.end(), result.begin());
    result.size_ = last - result.begin();
    return result;
  }

  /**
   * @brief Find the index range of a segment of a sorted array such that the
   * entries in this segment is within [bound.begin(), bound.end()). Assume
   * array values are ordered.
   *
   * An example
   \code{cpp}
   SArray<int> a{1 3 5 7 9};
   CHECK_EQ(SizeR(1,3), a.FindRange(Range<int>(2,7)));
   \endcode
   * @param bound
   *
   * @return the index range
   */
  SizeR FindRange (const Range<V>& bound) const {
    if (empty()) return SizeR(0,0);
    CHECK(bound.valid());
    auto lb = std::lower_bound(begin(), end(), bound.begin());
    auto ub = std::lower_bound(begin(), end(), bound.end());
    return SizeR(lb - begin(), ub - begin());
  }

  inline bool empty() const { return size() == 0; }
  inline size_t size() const { return size_; }
  inline size_t capacity() const { return capacity_; }

  inline V* begin() { return data(); }
  inline const V* begin() const { return data(); }
  inline V* end() { return data() + size(); }
  inline const V* end() const { return data() + size(); }

  inline V* data() const { return ptr_.get(); }

  inline std::shared_ptr<V>& ptr() { return ptr_; }
  inline const std::shared_ptr<V>& ptr() const { return ptr_; }


  /** @brief Returns the memory size in bytes */
  size_t MemSize() const { return capacity_*sizeof(V); }

  inline V back() const { CHECK(!empty()); return data()[size_-1]; }
  inline V front() const { CHECK(!empty()); return data()[0]; }
  inline V& operator[] (int i) { return data()[i]; }
  inline const V& operator[] (int i) const { return data()[i]; }

  inline void push_back(const V& val) {
    if (size_ == capacity_) reserve(size_*2+5);
    data()[size_++] = val;
  }
  void pop_back() { if (size_) --size_; }
  void append(const SArray<V>& arr) {
    if (arr.empty()) return;
    auto orig_size = size_;
    resize(size_ + arr.size());
    memcpy(data()+orig_size, arr.data(), arr.size()*sizeof(V));
  }

  void SetValue(V value) {
    if (value == 0) {
      SetZero();
    } else {
      for (size_t i = 0; i < size_; ++i) data()[i] = value;
    }
  }
  /// @brief set all entries into 0
  void SetZero() { memset(data(), 0, size_*sizeof(V)); }

  /// @brief  Assume values are ordered, return the value range.
  Range<V> range() const {
    return (empty() ? Range<V>(0,0) : Range<V>(front(), back()+1));
  }


  /// @brief Compare values
  template <typename W> bool operator==(const SArray<W> &rhs) const {
    if (rhs.size() * sizeof(W) != size() * sizeof(V)) return false;
    if (size() == 0) return true;
    return (memcmp(data(), rhs.data(), size() * sizeof(V)) == 0);
  }


 private:
  size_t size_ = 0;
  size_t capacity_ = 0;
  std::shared_ptr<V> ptr_;
};

/// \brief for debug use
template <typename V>
std::ostream& operator<<(std::ostream& os, const SArray<V>& obj) {
  os << obj.blob();
  return os;
}

} // namespace ps
