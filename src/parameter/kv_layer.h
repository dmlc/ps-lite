#pragma once
#include "ps.h"
#include "parameter/parameter.h"
namespace ps {

/**
 * @brief the default updater for KVLayer
 */
template <typename V>
class KVLayerUpdater {
 public:
  /// @brief initialize the data
  void Init(int id, size_t size, V* data) { }

  /// @brief update the model by using received data
  void Update(int id, size_t size, const V* recv_data, V* data) { }
};

/**
 * @brief multiple layers with various size
 *
 * @tparam V value type
 * @tparam Updater the updater class.
 */
template <typename V, class Updater = KVLayerUpdater<V>>
class  KVLayer : public Parameter {
 public:
  /**
   * @brief constructor
   * @param id customer id
   */
  KVLayer(size_t partition_thr = 1000, int id = NextCustomerID()) :
      Parameter(id), partition_thr_(partition_thr) { }
  virtual ~KVLayer() { }

  /// @brief set the updater,
  void set_updater(Updater* updt) { updater_ = updt; }

  /// @brief get the layer by the key
  SArray<V> operator[] (int key) { Lock l(mu_); return layer_[key]; }

  /// @brief get the layer by the key
  SArray<V> layer(int key) { Lock l(mu_); return layer_[key]; }

  // void set_layer(int key, V* data, size_t size) {
  //   layer_[key] = SArray<V>(data, size, false);
  // }

  /**
   * @brief Push data into servers
   *
   * @param task the request task
   * @param data layer pointer
   * @param size layer size
   * @param zero_copy if true, the system will not copy "data", which means
   * the content of "data" should not be modified or delete utill the push has
   * been successed. namely Wait() returns.
   *
   * @return the timestamp of the push request
   */
  int Push(const Task& task, V* data, size_t size, bool zero_copy = false);

  /**
   * @brief Sent a pull request to servers.
   *
   * @param task the request task
   * @param data if not NULL, then pulled back will be written into "data";
   * otherwise the received data will be saved in layer_
   * @param size layer size
   * @param callback the callback function will be called once pull is finished.
   *
   * @return the timestamp of the pull request
   */
  int Pull(const Task& task, V* data, size_t size,
           Message::Callback callback = Message::Callback());

  virtual void Slice(const Message& request, const std::vector<Range<Key>>& krs,
                     std::vector<Message*>* msgs);
  virtual void GetValue(Message* msg);
  virtual void SetValue(const Message* msg);
 protected:
  std::mutex mu_;
  std::unordered_map<int, SArray<V>> layer_;
  size_t partition_thr_;
  Updater* updater_ = nullptr;

  int call_ = 0;
};

template <typename V, class Updater>
int KVLayer<V, Updater>::Push(const Task& task, V* data, size_t size, bool zero_copy) {
  // LOG_FIRST_N(INFO, 100) << size;
  SArray<V> val;
  if (zero_copy) {
    val.reset(data, size, EmptyDel<V>());
  } else {
    val.CopyFrom(data, size);
  }
  Message push(task, kServerGroup);
  Range<Key>(0, size).To(push.task.mutable_key_range());
  push.add_value(val);
  return Parameter::Push(&push);
}

template <typename V, class Updater>
int KVLayer<V, Updater>::Pull(
    const Task& task, V* data, size_t size, std::function<void()> callback) {
  int id = task.key_channel();
  if (data == NULL) {
    if (layer_[id].size() != size) layer_[id].resize(size, 0);
  } else {
    layer_[id].reset(data, size, EmptyDel<V>());
  }
  Message pull(task, kServerGroup);
  Range<Key>(0, size).To(pull.task.mutable_key_range());
  if (callback) pull.callback = callback;
  return Parameter::Pull(&pull);
}

template <typename V, class Updater>
void KVLayer<V, Updater>::Slice(
    const Message& request, const std::vector<Range<Key>>& krs,
    std::vector<Message*>* msgs) {
  // divide the key range
  size_t n = krs.size();
  int key = request.task.key_channel();
  Range<Key> kr(request.task.key_range());
  for (size_t i = 0; i < n; ++i) {
    Message* msg = (*msgs)[i];
    msg->clear_value();
    auto mut_kr = msg->task.mutable_key_range();
    if (kr.size() < partition_thr_) {
      // a tiny layer, sent it to server k
      int k = (key * 991) % n;
      if ((int)i == k) {
        kr.To(mut_kr);
      } else {
        Range<Key>(0,0).To(mut_kr);
        msg->valid = false;  // invalid msg will not be sent
      }
    } else {
      // evenly parititon the data into all server nodes
      kr.EvenDivide(n, i).To(mut_kr);
    }
  }

  // divide the data
  for (size_t i = 0; i < request.value.size(); ++i) {
    SArray<V> data(request.value[i]);
    CHECK_EQ(data.size(), kr.size());
    for (size_t j = 0; j < n; ++j) {
      Message* msg = (*msgs)[j];
      if (msg->valid) {
        Range<Key> kr(msg->task.key_range());
        msg->add_value(data.Segment(kr));
      }
    }
  }
}

template <typename V, class Updater>
void KVLayer<V, Updater>::GetValue(Message* msg) {

  // Lock l(mu_);
  mu_.lock();
  auto& my_val = layer_[msg->task.key_channel()];
  mu_.unlock();
  Range<Key> kr(msg->task.key_range());
  if (my_val.empty()) {
    // initialize weight
    my_val.resize(kr.size(), 0);
    CHECK_NOTNULL(updater_)->Init(
        msg->task.key_channel(), my_val.size(), my_val.data());
  }

  CHECK_EQ(my_val.size(), kr.size());
  SArray<V> send_data(kr.size());
  send_data.CopyFrom(my_val);  // TODO, needs memcpy?
  msg->add_value(send_data);
}

template <typename V, class Updater>
void KVLayer<V, Updater>::SetValue(const Message* msg) {
  // Lock l(mu_);
  CHECK_EQ(msg->value.size(), 1);
  SArray<V> recv_data(msg->value[0]);
  Range<Key> kr(msg->task.key_range());
  CHECK_EQ(kr.size(), recv_data.size());
  int key = msg->task.key_channel();
  mu_.lock();
  auto& my_val = layer_[key];
  mu_.unlock();

  if (IsWorkerNode()) {
    if (my_val.empty()) my_val.resize(kr.size(), 0);
    CHECK_GE(my_val.size(), kr.end());
    my_val.Segment(kr).CopyFrom(recv_data);
  } else if (IsServerNode()) {
    // TODO this server can do flexible consistency control here

    if (my_val.empty()) {
      // initialize weight
      my_val.resize(kr.size(), 0);
      CHECK_NOTNULL(updater_)->Init(key, kr.size(), my_val.data());
    }

    // update weight
    CHECK_GE(my_val.size(), kr.size());
    CHECK_NOTNULL(updater_)->Update(key, kr.size(), recv_data.data(), my_val.data());
  }
}

}  // namespace ps
