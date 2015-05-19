#pragma once
#include "ps/app.h"
#include "proto/param.pb.h"
namespace ps {

class KVStore : public Customer {
 public:
  KVStore(int id) : Customer(id) { }
  virtual ~KVStore() { }

  void ProcessRequest(Message* request) {
    const auto& call = request->task.param();
    Message* response = nullptr;
    bool push = call.push();
    if (!push) {
      // a pull request, need to reply with the value
      response = new Message(*request);
    }

    if (call.replica()) {
      // a replication request
      // if (push) {
      //   SetReplica(request);
      // } else {
      //   GetReplica(response);
      // }
    } else {
      // a normal request
      if (push) {
        SetValue(request);
      } else {
        GetValue(response);
      }
    }

    if (response) Reply(request, response);
  }

  virtual void SaveModel(const std::string& file) { }

 protected:
  /// User-defineded functions ////

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
