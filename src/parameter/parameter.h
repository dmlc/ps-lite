#pragma once
#include "ps/app.h"
#include "proto/param.pb.h"
namespace ps {

/// The base class of shared parameters
class Parameter : public Customer {
 public:
  Parameter(int id) : Customer(id)  { }
  virtual ~Parameter() { }

  typedef std::initializer_list<int> Timestamps;
  typedef ::google::protobuf::RepeatedPtrField<Filter> IFilters;
  /**
   * @brief Creats a request task
   *
   * @param channel communication channel
   * @param ts the timestamp of this request
   * @param wait a list of timestamp this request should wait
   * @param filters a list of filters to compress the request message
   * @param key_range the key range of this request
   *
   * @return A Task
   */
  static Task Request(int channel,
                      int ts = Message::kInvalidTime,
                      const Timestamps& wait = {},
                      const IFilters& filters = IFilters(),
                      const Range<Key>& key_range = Range<Key>::All()) {
    Task req; req.set_request(true);
    req.set_key_channel(channel);
    if (ts > Message::kInvalidTime) req.set_time(ts);
    for (int t : wait) req.add_wait_time(t);
    for (const auto& f : filters) req.add_filter()->CopyFrom(f);
    key_range.To(req.mutable_key_range());
    return req;
  }

  /// @brief Submit a push message to msg->recver
  inline int Push(Message* msg) {
    msg->task.mutable_param()->set_push(true);
    return Submit(msg);
  }

  /// @brief Submit a pull message to msg->recver
  inline int Pull(Message* msg) {
    msg->task.mutable_param()->set_push(false);
    return Submit(msg);
  }

  virtual void WriteToFile(std::string file) { }

  virtual void ProcessRequest(Message* request);
  virtual void ProcessResponse(Message* response);
 protected:

  /// @brief Fill "msg" with the values it requests, e.g.,
  ///   msg->value(0)[0] = my_val_[msg->key[0]];
  virtual void GetValue(Message* msg) = 0;

  /// @brief Set the values in "msg" into into my data strcuture, e.g..
  ///  my_val_[msg->key[0]] = msg->value(0)[0];
  virtual void SetValue(const Message* msg) = 0;

  /// @brief the message contains the backup KV pairs sent by the master node of the key
  /// segment to its replica node. merge these pairs into my replica, say
  /// replica_[msg->sender] = ...
  virtual void SetReplica(const Message* msg) { }

  /// @brief retrieve the replica. a new server node replacing a dead server will first
  /// ask for the dead's replica node for the data
  virtual void GetReplica(Message* msg) { }

  /// @brief a new server node fill its own datastructure via the the replica data from
  /// the dead's replica node
  virtual void Recover(Message* msg) { }
};

}  // namespace ps
