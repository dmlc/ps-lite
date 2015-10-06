#pragma once
#include "base/common.h"
#include "system/message.h"
#include "system/postoffice.h"
#include "system/executor.h"

namespace ps {
/**
 * \brief The base class of a shared object, such as an application or shared parameters.
 *
 * Customer implements an asynchronous RPC-like interface. A
 * customer can send a request to another customer with the same ID in a remote
 * machine.
 *
 * How it works:
 *
 * The customer A at node NA can submit a request to the customer B at node NB if both A
 * and B have the same Customer::id. The request Message, contains
 * arguments specified by the protobuf class Task and additional data such as
 * (key,value) pairs. The customer B first processes this request by the
 * user-defined function ProcessRequest() if the dependency constraints in this
 * request are satisfied; and then sent back the response to A by `Reply'. Once
 * A received the process, the user-defined function `ProcessResponse' will be
 * called.
 *
 * It's an asynchronous interface. `Submit' returns immediately after the
 * message is queued in the system. There is a timestamp assigned to each
 * request for synchronization. For example, B can use it to check whether this
 * request has been processed via `WaitReceivedRequest', while A can use `Wait'
 * to wait the response from B. A can also set a callback function when the
 * response is arriving.
 *
 * Furthermore, customer A can submit a request to a node group, such as the
 * server group. There is a user-defined function `Slice' which will partition
 * the request message, mainly the (key,value) pairs, according to the key range
 * of the remote node. Each node in this group will get a part of the original
 * message.
 *
 * There are user-defined filters to encode and decode the messages
 * communicated between nodes, mainly to reduce the network overhead.
 *
 */
class Customer {
 public:
  /**
   * \brief Constructor
   *
   * @param id an integer id to identify the shared object across machines.
   */
  Customer(int id) : id_(id), sys_(Postoffice::instance()), exec_(*this) {
    sys_.manager().AddCustomer(this);
  }
  virtual ~Customer() {
    sys_.manager().RemoveCustomer(id_);
  }

  ///////// As a sender /////////

  /**
   * @brief Submits a request to the customer with the same id in node #recver
   *
   * Sample usage: send a request to all worker nodes and wait until finished:
   *   Task task; task.mutable_sgd()->set_cmd(SGDCall::UPDATE_MODEL);
   *   int ts = Submit(task, kWorkerGroup);
   *   Wait(ts);
   *   Foo();
   *
   * @param request the request arguments
   * @param recver a string ID of the receiver node, which can be
   * a particular node "W0" or a node group such as "kServerGroup".
   *
   * @return the timestamp of this request.
   */
  inline int Submit(const Task& request, const NodeID& recver) {
    Message msg(request, recver);
    return Submit(&msg);
  }

  /**
   * @brief Submits a request message into a remote node
   *
   * Sample usage: the same functionality as above:
   *   Message req(task, kWorkerGroup);
   *   reg.callback = [this](){ Foo(); }
   *   Wait(Submit(&req));
   *
   * @param request contains a request task and other entries such as data
   * arrays, callbacks when replies received, and other flags.
   *
   * @return the timestamp of this request.
   */
  inline int Submit(Message* request) {
    return exec_.Submit(request);
  }

  /**
   * @brief Waits until a submitted request is finished
   *
   * If the receiver is this request is a single node, this function is blocked
   * until a reply message with the same #timestamp has been received from this
   * receiver, or it is considered as deed.  Otherwise, this function will wait
   * for the response from each alive node in the receiver node group.
   *
   * @param timestamp the timestamp of this request
   */
  inline void Wait(int timestamp) {
    exec_.WaitSentReq(timestamp);
  }

  /**
   * @brief Slices a message into multiple parts.
   *
   * It will be called by Submit.
   *
   * @param request the request message
   * @param krs a list of key ranges. It has length 1 if the receiver is single
   * node, otherwise, it will contain all the key ranges in this node group, and
   * they are ordered.
   * @param msgs output a list of n messages such as the msgs[i] is sent to the i-th
   * node. Each msgs[i] is already initilized by msgs[i]->task = request.task
   */
  virtual void Slice(const Message& request, const std::vector<Range<Key>>& krs,
                     std::vector<Message*>* msgs) {}

  /**
   * @brief A user-defined function, which processes a request message received
   * from "request->sender"
   *
   * It which will be called by the executor's processing thread once the time
   * dependencies specified in "request->task" have been satisfied.
   *
   * @param request the received request
   */
  virtual void ProcessRequest(Message* request) { }

  /**
   * @brief Returns the last received request.
   */
  inline std::shared_ptr<Message> LastRequest() {
    return exec_.last_request();
  }

  ///////// As a receiver /////////

  /**
   * @brief A user-defined function, which processes a response message received
   * from "response->sender"
   *
   * It be called by the executor's processing thread.
   *
   * @param response the received response
   */
  virtual void ProcessResponse(Message* response) { }

  /// Returns the last received response.
  inline std::shared_ptr<Message> LastResponse() {
    return exec_.last_response();
  }

  /**
   * @brief Wait until the a received request is processed at this node or the
   * sender is dead. If the sender is a node group, then wait for each alive
   * node in this node group.
   *
   * @param timestamp
   * @param sender
   */
  inline void WaitReceivedRequest(int timestamp, const NodeID& sender) {
    exec_.WaitRecvReq(timestamp, sender);
  }

  /**
   * @brief Set the request with "timestamp" sent from "sender" as been finished
   * (processed)
   *
   * Set the request with "timestamp" sent from "sender" as been finished
   * (processed). Typically the DAG engine in `executor_` will do it
   * automatically. But this function can mark a virtual request as finished to
   * achieve certain synchronization.
   *
   * Sample usage: data aggregation at server nodes. It has three steps: 1)
   * workers push data into servers; 2) each server aggregates the pushed data
   * from workers; 3) workers pull the aggregated data from servers.
   *
   * Implementation:
   *
   * Worker: submit a push request with timestamp t via `push_task.set_time(t)`;
   * then submit a pull request with timestamp t+2 which depends timestamp t+1,
   * namely `pull_task.set_time(t+2); pull_task.add_wait_time(t+1)`.
   *
   * Server; wait for pushed data via `WaitRecvReq(t, kWorkerGroup)`; aggregate
   * data; mark the virtual request t+1 as finished via
   * `FinishRecvReq(t+1)`. Then all blocked pull requests will be executed by
   * the DAG engine.
   *
   * @param timestamp
   * @param sender
   */
  inline void FinishReceivedRequest(int timestamp, const NodeID& sender) {
    exec_.FinishRecvReq(timestamp, sender);
  }

  /**
   * @brief Returns the number of finished requests on a particular timestamp this node received.
   *
   * @param timestamp
   * @param sender
   *
   * @return
   */
  inline int NumDoneReceivedRequest(int timestamp, const NodeID& sender) {
    return exec_.QueryRecvReq(timestamp, sender);
  }

  /**
   * @brief Replies the request message with a response. In default, response is
   * an empty ack message.
   *
   * @param request
   * @param response
   */
  void Reply(Message* request, Task response = Task()) {
    Message* msg = new Message(response);
    Reply(request, msg);
  }


  /**
   * @brief Reply the request message with a response message.
   *
   * @param request
   * @param response the system will DELETE response, so do not DELETE it.
   */
  void Reply(Message* request, Message* response) {
    exec_.Reply(request, response);
  }

  /**
   * @brief  Returns the unique ID of this customer
   */
  int id() const { return id_; }

  /**
   * @brief Returns the executor of this customer. DEPRECATED
   */
  Executor* executor() { return &exec_; }

 protected:
  int id_;
  Postoffice& sys_;
  Executor exec_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Customer);
};

/// The base class of an application.
class App : public Customer {
 public:
  App(int id = Postoffice::instance().manager().NextCustomerID()) : Customer(id) { }
  virtual ~App() { }

  /**
   * @brief The factory method an application must implement.
   *
   * @param conf a string configuration contained in the file -app_file and
   * string -app_conf
   *
   * @return The created instance
   */
  static App* Create(int argc, char *argv[]);

  /// @brief It is executed by the main thread immediately after this app has been
  /// created.
  virtual bool Run() {
    return true;
  }
};


}  // namespace ps
