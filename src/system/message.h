#pragma once
#include "base/common.h"
#include "ps/shared_array.h"
#include "proto/task.pb.h"
#include "proto/filter.pb.h"
namespace ps {

// The message communicated between nodes. It conntains all argument and data
// a request or a response needed.
struct Message {
 public:
  const static int kInvalidTime = -1;
  Message() { }
  Message(const Task& tk, const NodeID& dst) : task(tk), recver(dst) { }
  explicit Message(const Task& tk) : task(tk) { }

  Task task;  // argument

  // keys
  bool has_key() const { return !key.empty(); }
  template <typename T>
  void set_key(const SArray<T>& key);
  void clear_key() { task.clear_has_key(); key.clear(); }
  SArray<char> key;

  // values
  template <typename T>
  void add_value(const SArray<T>& value);
  void clear_value() { task.clear_value_type(); value.clear(); }
  std::vector<SArray<char> > value;  // values

  // clear both keys and values
  void clear_data() { clear_key(); clear_value(); }

  bool has_data() { return key.size() > 0 || value.size() > 0; }
  // memory size in bytes
  size_t mem_size();

  Filter* add_filter(Filter::Type type);

  // -- more local control signals --
  // they will not be sent to other nodes

  NodeID sender;  // sender node id
  NodeID recver;  // receiver node id

  bool replied   = false;  // true if this message has been replied
  bool finished  = true;   // true if the request associated with this message
                           // has been finished.
  bool valid     = true;   // an invalid message will not be sent, but be marked
                           // as finished
  bool terminate = false;  // used to stop the sending thread in Postoffice.

  typedef std::function<void()> Callback;
  Callback callback;       // the callback when the associated request is finished

  // debug
  std::string ShortDebugString() const;
  std::string DebugString() const;

 private:
  // helper
  template <typename V>
  static DataType EncodeType();

};


template <typename T> void Message::set_key(const SArray<T>& key) {
  task.set_key_type(EncodeType<T>());
  if (has_key()) clear_key();
  task.set_has_key(true);
  this->key = SArray<char>(key);
  if (!task.has_key_range()) Range<Key>::All().To(task.mutable_key_range());
}

template <typename T> void Message::add_value(const SArray<T>& value) {
  task.add_value_type(EncodeType<T>());
  this->value.push_back(SArray<char>(value));
}

template <typename V> DataType Message::EncodeType() {
  if (std::is_same<V, uint32>::value)
    return DataType::UINT32;
  else if (std::is_same<V, uint64>::value)
    return DataType::UINT64;
  else if (std::is_same<V, int32>::value)
    return DataType::INT32;
  else if (std::is_same<V, int64>::value)
    return DataType::INT64;
  else if (std::is_same<typename std::remove_cv<V>::type, float>::value)
    return DataType::FLOAT;
  else if (std::is_same<V, double>::value)
    return DataType::DOUBLE;
  else if (std::is_same<V, uint8>::value)
    return DataType::UINT8;
  else if (std::is_same<V, int8>::value)
    return DataType::INT8;
  else if (std::is_same<V, char>::value)
    return DataType::CHAR;
  return DataType::OTHER;
}

// Slice a "msg" according to key ranges "krs". "msg.key" must be ordered, and a
// each value entry must have the same length.
template <typename K> void SliceKOFVMessage(
    const Message& msg, const std::vector<Range<Key>>& krs,
    std::vector<Message*>* rets) {
  CHECK_EQ(krs.size(), rets->size());

  // find the positions in msg.key
  size_t n = krs.size();
  std::vector<size_t> pos(n+1);
  SArray<K> key(msg.key);
  Range<Key> msg_key_range(msg.task.key_range());
  for (size_t i = 0; i < n; ++i) {
    if (i == 0) {
      K k = (K)msg_key_range.Project(krs[0].begin());
      pos[0] = std::lower_bound(key.begin(), key.end(), k) - key.begin();
    } else {
      CHECK_EQ(krs[i-1].end(), krs[i].begin());
    }
    K k = (K)msg_key_range.Project(krs[i].end());
    pos[i+1] = std::lower_bound(key.begin(), key.end(), k) - key.begin();
  }

  // split the message according to *pos*
  for (size_t i = 0; i < n; ++i) {
    Message* ret = CHECK_NOTNULL((*rets)[i]);
    if (krs[i].SetIntersection(msg_key_range).empty()) {
      // the remote node does not maintain this key range. mark this message as
      // valid, which will not be sent
      ret->valid = false;
    } else {
      ret->valid = true;  // must set true, otherwise this piece might not be sent
      if (key.empty()) continue;  // to void be divided by 0
      SizeR lr(pos[i], pos[i+1]);
      ret->set_key(key.Segment(lr));
      for (auto& v : msg.value) {
        size_t k = v.size() / key.size();
        CHECK_EQ(key.size() * k, v.size());
        ret->value.push_back(v.Segment(lr*k));
      }
    }
  }
}

// similar to above, but the values can be arbitary length
template <typename K> void SliceDynValMessage(
    const Message& msg, const std::vector<Range<Key>>& krs,
    std::vector<Message*>* rets) {
}

} // namespace ps

// inline std::ostream& operator<<(std::ostream& os, const Message& msg) {
//   return (os << msg.ShortDebugString());
// }

// template <typename T>
// void add_value(const SArrayList<T>& value) {
//   for (const auto& v : value) add_value(v);
// }
// template <typename T>
// void add_value(const std::initializer_list<SArray<T>>& value) {
//   for (const auto& v : value) add_value(v);
// }
