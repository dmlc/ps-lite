#pragma once
#include <limits>
#include "proto/range.pb.h"
#include "glog/logging.h"

namespace ps {

template<class T> class Range;
typedef Range<size_t> SizeR;

/**
 * \brief a range between [begin, end)
 */
template<class T>
class Range {
 public:
  Range() : begin_(0), end_(0) { }

  template<typename V>
  Range(const Range<V>& other) { Set(other.begin(), other.end()); }

  template<typename V, typename W>
  Range(V begin, W end) { Set(begin, end); }

  Range(const PbRange& pb) { CopyFrom(pb); }

  template <typename V>
  void operator=(const Range<V>& rhs) { Set(rhs.begin(), rhs.end()); }

  // construct from a protobuf range
  void CopyFrom(const PbRange& pb) { Set(pb.begin(), pb.end()); }

  // fill a protobuf range
  void To(PbRange* pb) const { pb->set_begin(begin_); pb->set_end(end_); }

  template <typename V, typename W>
  void Set(V start, W end) {
    begin_ = static_cast<T>(start);
    end_ = static_cast<T>(end);
  }

  T begin() const { return begin_; }
  T& begin() { return begin_; }
  T end() const { return end_; }
  T& end() { return end_; }

  size_t size() const { return (size_t)(end_ - begin_); }

  bool valid() const { return end_ >= begin_; }
  bool empty() const { return begin_ >= end_; }

  bool operator== (const Range& rhs) const {
    return (begin_ == rhs.begin_ && end_ == rhs.end_);
  }

  Range operator+ (const T v) const { return Range(begin_+v, end_+v); }
  Range operator- (const T v) const { return Range(begin_-v, end_-v); }
  Range operator* (const T v) const { return Range(begin_*v, end_*v); }

  template <typename V> bool contains(const V& v) const {
    return (begin_ <= static_cast<T>(v) && static_cast<T>(v) < end_);
  }

  bool InLeft(const Range& other) const {
    return (begin_ <= other.begin_) ||
        (begin_ == other.begin_ && end_ <= other.end_);
  }

  bool InRight(const Range& other) const {
    return !InLeft(other);
  }

  // project v into this range
  template <typename V> V Project(const V& v) const {
    return static_cast<V>(std::max(begin_, std::min(end_, static_cast<T>(v))));
  }

  Range SetIntersection(const Range& dest) const {
    return Range(std::max(begin_, dest.begin_), std::min(end_, dest.end_));
  }

  Range SetUnion(const Range& dest) const {
    return Range(std::min(begin_, dest.begin_), std::max(end_, dest.end_));
  }

  // divide this range evenly into n ones, and return the i-th
  Range EvenDivide(size_t n, size_t i) const;

  std::string ToString() const {
    return ("["+std::to_string(begin_)+","+std::to_string(end_)+")");
  }

  static Range All() {
    return Range(std::numeric_limits<T>::min(),
                 std::numeric_limits<T>::max());
  }
 private:
  T begin_;
  T end_;
};


template<class T>
Range<T> Range<T>::EvenDivide(size_t n, size_t i) const {
  CHECK(valid());
  CHECK_GT(n, (size_t)0);
  CHECK_LT(i, n);
  auto itv = static_cast<long double>(end_ - begin_) /
             static_cast<long double>(n);
  return Range(static_cast<T>(begin_+itv*i), static_cast<T>(begin_+itv*(i+1)));
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const Range<T>& obj) {
  return (os << obj.ToString());
}

} // namespace ps

namespace std {
template<typename T>
struct hash<ps::Range<T> > {
  std::size_t operator()(ps::Range<T> const& s) const {
    // return std::hash<std::pair<T,T> >()(std::make_pair(s.begin(), s.end()));
    // return (std::hash<T>(s.begin()) ^ (std::hash<T>(s.end()) << 1));
    return (size_t)(s.begin() ^ s.end() << 1);  // TODO why << 1?
  }
};

template<typename T>
struct hash<std::pair<int, ps::Range<T>>> {
  std::size_t operator()(std::pair<int, ps::Range<T>> const& s) const {
    return (s.first ^ s.second.begin() ^ s.second.end());
  }
};

} // namespace std
