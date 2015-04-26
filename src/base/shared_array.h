#pragma once
#include <atomic>
#include "base/blob.h"
#include "base/common.h"
#include "base/file.h"
#include "base/range.h"
#include "Eigen/Core"
#include "proto/data.pb.h"
#include "proto/param.pb.h"
namespace ps {

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
   SArray<int> C = A.Segment(SizeR(1,3));
   A[2] = 2;
   CHECK_EQ(B[2], 2);
   CHECK_EQ(C[1], 2);
   \endcode
 */
template<typename V> class SArray {
 public:
  SArray() { }
  ~SArray() { }
  /**
   * @brief Create an array with length n.
   *
   * Values are not initialized. To initialize them, call SetValue() or SetZero()
   */
  explicit SArray(size_t n) { resize(n); }
  SArray(size_t n, V val) { resize(n, val); }

  /**
   * @brief Zero-copy constructor, namely just copy the pointer
   */
  template <typename W> explicit SArray(const SArray<W>& arr);
  /**
   * @brief Zero-copy constructor, namely just copy the pointer
   */
  template <typename W> void operator=(const SArray<W>& arr);

  /**
   * @brief Zero-copy constructor.
   *
   * @param data
   * @param size
   * @param deletable if true, the "data" will be deleted in the copy last
   * SArray
   */
  SArray(V* data, size_t size, bool deletable = true) {
    reset(data, size, deletable);
  }

  explicit SArray(const SBlob<V>& blob) {
    size_ = blob.size();
    capacity_ = size_;
    data_ = blob.data();
    ptr_ = std::shared_ptr<char>(blob.shared_data(), (char*)blob.data());
  }

  /**
   * @brief Copy constructor
   *
   * @param src
   * @param size
   */
  void CopyFrom(const V* src, size_t size);
  /**
   * @brief Copy constructor
   *
   * @param arr
   */
  void CopyFrom(const SArray<V>& arr);
  /**
   * @brief A general but might slower version of copy constructor
   *
   * @param first
   * @param last
   */
  template <typename ForwardIt>
  void CopyFrom(const ForwardIt first, const ForwardIt last);

  /**
   * Copy from a initializer_list
   *
   * @param list
   */
  template <typename W> SArray(const std::initializer_list<W>& list);
  /**
   * Copy from a initializer_list
   *
   * @param list
   */
  template <typename W> void operator=(const std::initializer_list<W>& list);

  /**
   * @brief Slice a segment, zero-copy
   *
   * @param range the index segment
   * @return the segment [range.begin(), range.end())
   */
  SArray<V> Segment(const Range<size_t>& range) const;

  /**
   * @brief Performs set intersection between two sorted arrays
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
  SArray<V> SetIntersection(const SArray<V>& other) const;

  /**
   * @brief Perform set union between two sorted arrays
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
  SArray<V> SetUnion(const SArray<V>& other) const;


  // Assume array values are ordered. return the position range of the segment
  // whose entry values are within

  /**
   * @brief Find the index range of a segment of a sorted array such that the
   * entries in this segment is within [bound.begin(), bound.end())
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
  SizeR FindRange (const Range<V>& bound) const;

  size_t size() const { return size_; }
  size_t capacity() const { return capacity_; }

  /**
   * @brief Returns the memory size in bytes
   */
  size_t MemSize() const { return capacity_*sizeof(V); }


  bool empty() const { return size() == 0; }

  /**
   * @brief Reset the current data pointer
   *
   * @param data
   * @param size
   * @param deletable
   */
  void reset(V* data, size_t size, bool deletable = true);

  /**
   * @brief Resizes the array to n elements
   *
   * If n <= capacity_, then only change the size. otherwise, append n -
   * current_size entries (without value initialization)
   * @param n
   */
  void resize(size_t n);

  /**
   * @brief Resizes the array to n elements
   *
   * If n <= capacity_, then only change the size. otherwise, append n -
   * current_size entries, and then set all value to val (TODO doesn't make sense)
   * @param val
   * @param n
   */
  void resize(size_t n, V val) { resize(n); SetValue(val); }

  /**
   * @brief Requests that the capacity be at least enough to contain n elements.
   *
   * @param n
   */
  void reserve(size_t n);

  void clear() { reset(nullptr, 0); }

  V* begin() { return data(); }
  const V* begin() const { return data(); }
  V* end() { return data() + size(); }
  const V* end() const { return data() + size(); }

  V back() const { CHECK(!empty()); return data_[size_-1]; }
  V front() const { CHECK(!empty()); return data_[0]; }
  V& operator[] (int i) { return data_[i]; }
  const V& operator[] (int i) const { return data_[i]; }

  void append(const SArray<V>& tail);
  void push_back(const V& val);
  void pop_back() { if (size_) --size_; }
  void SetValue(V value);

  /// @brief set all entries into 0
  void SetZero() { memset(data_, 0, size_*sizeof(V)); }
  //
  /// @brief set values according to *cf*
  void SetValue(const ParamInitConfig& cf);

  /// @brief  Assume values are ordered, return the value range.
  Range<V> range() const {
    return (empty() ? Range<V>(0,0) : Range<V>(front(), back()+1));
  }
  V* data() const { return data_; }
  const std::shared_ptr<void>& pointer() const { return ptr_; }
  std::shared_ptr<void>& pointer() { return ptr_; }

  /// @brief the number of non-zero entries
  size_t nnz() const;

  /// @brief Compare values
  template <typename W> bool operator==(const SArray<W> &rhs) const;

  // /// @brief return an Eigen3 vector, zero-copy
  // typedef Eigen::Map<Eigen::Matrix<V, Eigen::Dynamic, 1> > EVecMap;
  // EVecMap EigenVector() const { return EVecMap(data(), size()); }
  // EVecMap vec() const { return EVecMap(data(), size()); }

  // /// @brief return an Eigen3 array, zero-copy
  // typedef Eigen::Map<Eigen::Array<V, Eigen::Dynamic, 1> > EArrayMap;
  // EArrayMap EigenArray() const { return EArrayMap(data(), size()); }
  // EArrayMap arr() const { return EArrayMap(data(), size()); }

  // /// @brief return an Eigen3 matrix, zero-copy
  // typedef Eigen::Map<Eigen::Array<V, Eigen::Dynamic, Eigen::Dynamic> > EMatMap;
  // EMatMap EigenMatrix(int k) const {
  //   CHECK_EQ(size()%k, 0); return EArrayMap(data(), size()/k, k);
  // }
  // EMatMap mat(int k) const {
  //   CHECK_EQ(size()%k, 0); return EArrayMap(data(), size()/k, k);
  // }


  // double Sum() const { return EigenArray().sum(); }
  // double Mean() const { return empty() ? 0 : Sum() / (double)size(); }
  // double Std() const {
  //   return empty() ? 0 :
  //       (EigenArray() - Mean()).matrix().norm() / sqrt((double)size());
  // }

  // /// @brief convert to a dense matrix, zero-copy
  // std::shared_ptr<Matrix<V>> SMatrix(size_t rows = -1, size_t cols = -1);

  /// @brief  Return the compressed array by snappy
  SArray<char> CompressTo() const;
  /// @brief Uncompress the values from src with size src_size. Before calling this
  /// function, you should allocate enough memory first (e.g. call resize(xx))
  void UncompressFrom(const char* src, size_t src_size);
  void UncompressFrom(const SArray<char>& src) { UncompressFrom(src.data(), src.size()); }

  /// @brief read the segment [range.begin(), range.end()) from the binary file
  bool ReadFromFile(SizeR range, const string& file_name);
  bool ReadFromFile(const string& file_name) {
    return ReadFromFile(SizeR::All(), file_name);
  }
  bool ReadFromFile(SizeR range, const DataConfig& file);

  /// @brief  write all values into a binary file
  bool WriteToFile(const string& file_name) const {
    return WriteToFile(SizeR(0, size_), file_name);
  }
  /// @brief write the segment [range.begin(), range.end()) into a binary file
  bool WriteToFile(SizeR range, const string& file_name) const;


 private:
  // TODO use single std::shared_ptr<V> data_;
  size_t size_ = 0;
  size_t capacity_ = 0;
  V* data_ = nullptr;
  std::shared_ptr<void> ptr_ = std::shared_ptr<void>(nullptr);

};

// for debug use
template <typename V>
std::ostream& operator<<(std::ostream& os, const SArray<V>& obj) {
  os << DBSTR(obj.data(), obj.size(), 10);
  return os;
}

} // namespace ps
