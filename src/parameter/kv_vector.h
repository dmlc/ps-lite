#pragma once
#include "ps.h"
#include "parameter/parameter.h"
#include "base/parallel_ordered_match.h"
#include "filter/frequency_filter.h"
namespace ps {
// TODO doc, and filter
/**
 * @brief Key-value vectors.
 *
 * Keys are with type K, and a value is a fixed
 * length array with type V, such as int[10]. Storage format
   \verbatim
   key_0,   ...   key_n
   val_00,  ...   val_n0
   val_01,  ...   val_n1
    ...     ...    ...
   val_0k,  ...   val_nk
   \endverbatim
 *
 * Keys are ordered and unique. Both keys and values are stored in continous
 * arrays to accelerate read and overwrite, where values are in a column-major
 * order. However, it is efficient only when insert bulk (key,value) pairs each
 * time.
 *
 * It supports multiple channels. Communication is isolated between
 * channels. For example, we can sent a pull request on channel 1 and 2 at same
 * time, then the pulled results will be store at channel 1 and 2,
 * respectively.
 */
template <typename K, typename V>
class KVVector : public Parameter {
 public:
  /**
   * @brief Constructor
   *
   * @param buffer_value if true then the received data in push request or a
   * pull response is merged into my value directly. otherwise, they will be
   * aligned, stored, and can be retrieved by `received'' into
   * @param k the length of a value entry
   * @param id customer id
   */
  KVVector(bool buffer_value = false, int k = 1, int id = NextCustomerID()) :
      Parameter(id), k_(k), buffer_value_(buffer_value) {
    CHECK_GT(k, 0);
  }
  virtual ~KVVector() { }

  /// @brief n key-value pairs stored by arrays
  struct KVPairs {
    SArray<K> key;    // [key_0,  ..., key_n]
    SArray<V> value;  // [val_00, ..., val_0k, ..., val_n0, ..., val_nk]
  };

  /// @brief Returns the key-vale pairs in channel "chl"
  KVPairs& operator[] (int chl) { Lock l(mu_); return data_[chl]; }

  /// @brief Clears both key and value at channel "chl"
  void Clear(int chl) {
    Lock l(mu_); data_[chl].key.clear(); data_[chl].value.clear();
  }

  /// @brief buffer for received data
  struct Buffer {
    int channel;
    SizeR idx_range;
    std::vector<SArray<V>> values;
  };

  /// @brief Returns the buffered data on timestamp
  Buffer buffer(int timestamp) { Lock l(mu_); return buffer_[timestamp]; }

  void ClearBuffer(int timestamp) {
    for (auto& v : buffer_[timestamp].values) v.clear();
  }

  void ClearIFilter() { freq_filter_.clear(); }

  /**
   * @brief Push data into servers
   *
   * @param request
   * @param keys  n keys
   * @param values a list of n values
   * @param callback called when responses of this request is received
   *
   * @return the timestamp
   */
  int Push(const Task& request,
           const SArray<K>& keys,
           const std::initializer_list<SArray<V>>& values = {},
           const Message::Callback& callback = Message::Callback());

  /**
   * @brief Pull data from servers
   *
   * @param request
   * @param keys n keys
   * @param callback called when responses of this request is received
   *
   * @return the timestamp
   */
  int Pull(const Task& request, const SArray<K>& keys,
           const Message::Callback& callback = Message::Callback());


  virtual void Slice(const Message& request, const std::vector<Range<Key>>& krs,
                     std::vector<Message*>* msgs) {
    SliceMessage<K>(request, krs, msgs);
  }
  virtual void GetValue(Message* msg);
  virtual void SetValue(const Message* msg);
  using Parameter::Push;
  using Parameter::Pull;
 protected:
  int k_;  // value entry size
  std::unordered_map<int, KVPairs> data_;  // <channel, KVPairs>

  bool buffer_value_;
  std::unordered_map<int, Buffer> buffer_;  // <channel, Buffer>

  std::mutex mu_;  // protect the structure of data_ and buffer_

  // <channel, filter tail keys>
  std::unordered_map<int, FreqencyIFilter<Key, uint8>> freq_filter_;
};

template <typename K, typename V>
void KVVector<K,V>::SetValue(const Message* msg) {
  // do check
  SArray<K> recv_key(msg->key);
  VLOG(1) << "SetValue: received " << recv_key.size() << " keys from " << msg->sender;
  if (recv_key.empty()) return;
  int chl = msg->task.key_channel();

  // filter request
  if (msg->task.param().has_tail_filter() && msg->task.request()) {
    const auto& tail = msg->task.param().tail_filter();
    CHECK(tail.insert_count());
    CHECK_EQ(msg->value.size(), 1);
    SArray<uint8> count(msg->value[0]);
    CHECK_EQ(count.size(), recv_key.size());
    auto& filter = freq_filter_[chl];
    if (filter.Empty()) {
      double w = (double)std::max(sys_.manager().num_workers(), 1);
      int n = w / log(w+1) * tail.countmin_n();
      int k = tail.countmin_k();
      filter.Resize(n, k);
      VLOG(1) << "resize " << chl << " into n=" << n << ", k=" << k;
    }
    filter.InsertKeys(recv_key, count);
    return;
  }

  mu_.lock();
  auto& kv = data_[chl];
  mu_.unlock();

  if (msg->value.size() == 0) {
    // only has keys. merge these keys
    kv.key = kv.key.SetUnion(recv_key);
    // clear the values, because they are not matched any more
    kv.value.clear();
    VLOG(1) << "merge keys, now the key size is " << kv.key.size();
    return;
  } else if (kv.key.empty()) {
    LOG(ERROR) << "empty keys at channel " << msg->task.key_channel();
    return;
  }

  for (int i = 0; i < msg->value.size(); ++i) {
    SArray<V> recv_data(msg->value[i]);
    if (!buffer_value_) {
      // write the received value into kv.value directly
      CHECK_EQ(i, 0) << " can only receive one value";
      CHECK_EQ(recv_data.size(), recv_key.size() * k_);
      if (kv.value.empty()) {
        kv.value = SArray<V>(kv.key.size() * k_, 0);
      }
      CHECK_EQ(kv.key.size() * k_, kv.value.size());

      size_t n = ParallelOrderedMatch(
          recv_key, recv_data, kv.key, &kv.value, k_, AsOp::PLUS);
      CHECK_EQ(n, recv_key.size() * k_);
      VLOG(1) << "matched " << n << " keys";
    } else {
      // match the received value, then save it
      mu_.lock();
      auto& buf = buffer_[msg->task.time()];
      mu_.unlock();

      if (i == 0) {
        SizeR idx_range = kv.key.FindRange(Range<K>(msg->task.key_range()));
        if (buf.values.size() == 0) {
          // "msg" comes from the first nodes in this channel, allocate memory first
          buf.values.resize(msg->value.size());
          buf.idx_range = idx_range;
          buf.channel = chl;
        } else {
          CHECK_EQ(buf.idx_range, idx_range);
          CHECK_EQ(buf.channel, chl);
        }
      }
      size_t k = recv_data.size() / recv_key.size();  // not necessary == k_
      size_t n = ParallelOrderedMatch(
          recv_key, recv_data, kv.key.Segment(buf.idx_range), &buf.values[i], k,
          AsOp::PLUS);
      CHECK_LE(n, recv_key.size() * k);
      VLOG(1) << "matched " << n << " keys";
    }
  }
}

template <typename K, typename V>
void KVVector<K,V>::GetValue(Message* msg) {
  // do check
  SArray<K> recv_key(msg->key);
  VLOG(1) << "GetValue: received " << recv_key.size() << " keys from " << msg->sender;
  if (recv_key.empty()) return;
  int chl = msg->task.key_channel();

  // filter request
  if (msg->task.param().has_tail_filter()) {
    const auto& tail = msg->task.param().tail_filter();
    CHECK(tail.has_freq_threshold());
    auto& filter = freq_filter_[chl];
    msg->key = filter.QueryKeys(recv_key, tail.freq_threshold());
    return;
  }

  Lock l(mu_);
  auto& kv = data_[chl];
  CHECK_EQ(kv.key.size() * k_, kv.value.size());

  // get the data
  SArray<V> val;
  size_t n = ParallelOrderedMatch(kv.key, kv.value, recv_key, &val, k_);
  CHECK_LE(n, recv_key.size() * k_);
  VLOG(1) << "matched " << n << " keys";
  msg->clear_value();
  msg->add_value(val);
}

template <typename K, typename V>
int KVVector<K,V>::Push(const Task& request, const SArray<K>& keys,
                        const std::initializer_list<SArray<V>>& values,
                        const Message::Callback& callback) {
  Message push(request, kServerGroup);
  push.set_key(keys);
  for (const auto& v : values) if (!v.empty()) push.add_value(v);
  if (callback) push.callback = callback;
  return Push(&push);
}

template <typename K, typename V>
int KVVector<K,V>::Pull(const Task& request, const SArray<K>& keys,
                        const Message::Callback& callback) {
  Message pull(request, kServerGroup);
  pull.set_key(keys);
  if (callback) pull.callback = callback;
  return Pull(&pull);
}

}  // namespace ps
