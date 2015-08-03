/*!
 * \file   blob.h
 * \brief  Blob is a simple structure that contains a length and a pointer to an
 * external array,
 */
#pragma once
#include <cstring>
#include <type_traits>
#include "ps/base.h"
#if USE_EIGEN
#include "Eigen/src/Core/Map.h"
#include "Eigen/src/Core/Array.h"
#endif  // DMLC_USE_EIGEN

namespace ps {

/**
 * \brief Binary Large OBject
 *
 * Blob is a simple structure
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
  Blob(const std::vector<typename std::remove_cv<T>::type>& v)
      : data(v.data()), size(v.size()) { }
  Blob(std::vector<T>* v) : data(v->data()), size(v->size()) { }

  inline T& operator[] (size_t n) const {
    CHECK_LT(n, size);
    return data[n];
  }

  T* begin() { return data; }
  T* end() { return data+size; }

  /*! \brief Slics a segment [begin, end) */
  Blob Slice(size_t begin, size_t end) const {
    CHECK_LE(begin, end); CHECK_LE(end, size);
    return Blob(data+begin, end-begin);
  }

#if USE_EIGEN
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
#endif  // USE_EIGEN

  /**
   * \brief Returns the m head and m tail element as string
   */
  std::string ShortDebugString(size_t m = 5) const {
    return DebugStr(data, size);
  }
};

/// \brief for debug use
template <typename T>
std::ostream& operator<<(std::ostream& os, const Blob<T>& obj) {
  os << obj.ShortDebugString(); return os;
}

}  // namespace ps
