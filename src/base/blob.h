/*!
 * \file   blob.h
 * \brief  Blob is a simple structure that contains a length and a pointer to an
 * external array,
 */
#pragma once
#include <string>
#include <vector>
#include <memory>
#include <string>
#include <sstream>

#if DMLC_USE_EIGEN
#include "Eigen/src/Core/Map.h"
#include "Eigen/src/Core/Array.h"
#endif  // DMLC_USE_EIGEN

#include "glog/logging.h"
#include "base/base.h"
namespace ps {

/**
 * \brief Blob, Binary Large OBject, is a simple structure
 * containing a pointer into some external storage and a size. The user of a
 * Blob must ensure that the blob is not used after the corresponding external
 * storage has been deallocated.
 *
 * \tparam T the date type
 */
template <typename T>
struct Blob {
  T* data;
  size_t size;

  /*! \brief Create an empty blob */
  Blob() : data(NULL), size(0) { }

  /*! \brief Create a blob from a pointer */
  Blob(T* d, size_t s) : data(d), size(s) { }

  /*! \brief Create a blob from std::vector */
  Blob(std::vector<T>& v) : data(v.data()), size(v.size()) { }

  T& operator[] (size_t n) const {
    CHECK_LT(n, size);
    return data[n];
  }

#if DMLC_USE_EIGEN
  typedef Eigen::Map<
    Eigen::Array<T, Eigen::Dynamic, 1> > EigenArrayMap;
  /*! \brief Return a size() by 1 Eigen3 Array */
  EigenArrayMap EigenArray() const {
    return EigenArrayMap(data, size);
  }

  typedef Eigen::Map<
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> > EigenMatrixMap;
  /*! \brief Return a size()/k by k Eigen3 Matrix */
  EigenMatrixMap EigenMatrix(int k = 1) const {
    CHECK_EQ(size % k, 0);
    return EigenMatrixMap(data, size / k, k);
  }
#endif  // DMLC_USE_EIGEN

  std::string ShortDebugString() const {
    return DBSTR<T>(data, size);
  }
};

/*!
 * \brief Const Blob
 */
template <typename T>
class CBlob {
 public:
  const T* data;
  size_t size;

  /*! \brief Create an empty blob */
  CBlob() : data(NULL), size(0) { }

  /*! \brief Create a blob from a pointer */
  CBlob(T* d, size_t s) : data(d), size(s) { }

  /*! \brief Create a blob from std::vector */
  CBlob(const std::vector<T>& v) : data(v.data()), size(v.size()) { }

  T operator[] (size_t n) const {
    CHECK_LT(n, size);
    return data[n];
  }

  std::string ShortDebugString() const {
    return DBSTR<T>(data, size);
  }
#if DMLC_USE_EIGEN
  typedef Eigen::Map<
    Eigen::Array<T, Eigen::Dynamic, 1> > EigenArrayMap;
  /*! \brief Return a size() by 1 Eigen3 Array */
  EigenArrayMap EigenArray() const {
    return EigenArrayMap(data, size);
  }

  typedef Eigen::Map<
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> > EigenMatrixMap;
  /*! \brief Return a size()/k by k Eigen3 Matrix */
  EigenMatrixMap EigenMatrix(int k = 1) const {
    CHECK_EQ(size % k, 0);
    return EigenMatrixMap(data, size / k, k);
  }
#endif  // DMLC_USE_EIGEN

};


template<typename T> struct EmptyDeleter {
  void operator()(T* p) const { }
};

/**
 * @brief a shared blob
 *
 * SBlob a wrapper of an shared pointer and its size  The data pointed is
 * guaranteed to be deleted when the last SBlob is destroyed or reseted.
 */
template<class T>
class SBlob {
 public:
  SBlob() { }
  ~SBlob() { }


  /*! @brief Create a blob with length n, values are initialized to 0 */
  explicit SBlob(size_t n) { resize(n); }

  SBlob(size_t n, T init_val) { resize(n, init_val); }

  SBlob(T* data, size_t size) { reset(data, size); }

  template<typename Deleter>
  SBlob(T* data, size_t size, Deleter d) { reset(data, size, d); }

  void reset(T* data, size_t size) {
    reset(data, size,  ArrayDeleter());
  }

  template<typename Deleter>
  void reset(T* data, size_t size, Deleter d) {
    size_ = size;
    data_.reset(data, d);
  }

  void resize(size_t n) {
    T* new_data = new T[n+5];
    memcpy(new_data, data(), size() * sizeof(T));
    reset(new_data, n);
  }

  void resize(size_t n, T init_val) {
    resize(n);
    if (init_val == 0) {
      memset(data(), 0, n * sizeof(T));
    } else {
      for (size_t i = 0; i < size(); ++i) {
        data()[i] = init_val;
      }
    }
  }

  template <typename V>
  SBlob(const std::initializer_list<V>& list) {
    resize(list.size());
    size_t i = 0;
    for (V l : list) data()[i++] = l;
  }

  void CopyFrom(const T* data, size_t size) {
    resize(size);
    memcpy(data_.get(), data, size * sizeof(T));
  }

  // Blob<T> blob() const {
  //   return Blob<T>(data(), size());
  // }

  // CBlob<T> cblob() const {
  //   return CBlob<T>(data(), size());
  // }

  T& operator[](size_t i) const { return data_.get()[i]; }
  T* data() const { return data_.get(); }

  std::shared_ptr<T> shared_data() const { return data_; }

  size_t size() const { return size_; }
  std::string ShortDebugString() const {
    return CBlob<T>(data(), size()).ShortDebugString();
  }
 private:
  struct ArrayDeleter {
    void operator()(T* p) const { delete [] p; }
  };
  size_t size_ = 0;
  std::shared_ptr<T> data_;
};

// TODO support string? i.e. construct from string, and ToString
// TODO support == !=
// TODO slice itself Segment(int pos, int
// TODO a mutable slice?

}  // namespace ps
