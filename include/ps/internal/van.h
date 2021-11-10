/**
 *  Copyright (c) 2015 by Contributors
 *  Modifications Copyright (C) Mellanox Technologies Ltd. 2020.
 */
#ifndef PS_INTERNAL_VAN_H_
#define PS_INTERNAL_VAN_H_
#include <atomic>
#include <ctime>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ps/base.h"
#include "ps/internal/message.h"
namespace ps {
class Resender;
class Postoffice;

typedef enum ErrorCode {
  PS_OK,
  PS_ERR_TIMED_OUT,
  PS_ERR_NOT_CONNECTED,
  PS_ERR_CONNECTION_RESET,
  PS_ERR_OTHER
} ErrorCode;

/**
 * \brief Van sends messages to remote nodes
 *
 * If environment variable PS_RESEND is set to be 1, then van will resend a
 * message if it no ACK messsage is received within PS_RESEND_TIMEOUT
 * millisecond
 */
class Van {
 public:
  /**
   * \brief create Van
   * \param type zmq, socket, ...
   */
  static Van *Create(const std::string &type, Postoffice *postoffice);

  /** \brief constructer, do nothing. use \ref Start for real start */
  Van(Postoffice *postoffice) : postoffice_(postoffice) {}

  /**\brief deconstructer, do nothing. use \ref Stop for real stop */
  virtual ~Van() {}

  /**
   * \brief start van
   *
   * must call it before calling Send
   *
   * it initalizes all connections to other nodes. start the receiving
   * threads, which keeps receiving messages. if it is a system
   * control message, give it to postoffice::manager, otherwise, give it to the
   * accoding app.
   *
   * if standalone is set, the van will not contact the scheduler at start.
   */
  virtual void Start(int customer_id, bool standalone);

  /**
   * \brief send a message, It is thread-safe
   * \return the number of bytes sent. -1 if failed
   */
  int Send(Message &msg);

  /**
   * \brief return my node
   */
  inline const Node &my_node() const {
    CHECK(ready_) << "call Start() first";
    return my_node_;
  }

  /**
   * \brief stop van
   * stop receiving threads
   */
  virtual void Stop();

  /**
   * \brief get next available timestamp. thread safe
   */
  inline int GetTimestamp() { return timestamp_++; }

  /**
   * \brief whether it is ready for sending. thread safe
   */
  inline bool IsReady() { return ready_; }

  /**
   * \brief connect to a node
   */
  virtual void Connect(const Node &node) = 0;

  /**
   * \brief bind to my node
   * do multiple retries on binding the port. since it's possible that
   * different nodes on the same machine picked the same port
   * when there are multiple ports, the method may also update `node`
   * with the ports and device info used by the van.
   * \return return the first port bound, -1 if failed.
   */
  virtual int Bind(Node &node, int max_retry) = 0;

  /**
   * \brief block until received a message
   * \return the number of bytes received. -1 if failed or timeout
   */
  virtual int RecvMsg(Message *msg) = 0;

  /**
   * \brief send a mesage
   * \return the number of bytes sent
   */
  virtual int SendMsg(Message &msg) = 0;

  virtual void RegisterRecvBuffer(Message &msg) {
    CHECK(false) << "recv buffer registration is not supported";
  }

  /**
   * \brief the handle to process an error
   * \param data custom data
   * \param status error status
   * \param reason error description
   */
  using ErrHandle =
      std::function<void(void *data, ErrorCode status, std::string reason)>;

  static void set_err_handle(const ErrHandle &err_handle) {
    CHECK(err_handle) << "invalid error handle";
    err_handle_ = err_handle;
  }

  /**
   * \brief pin a memory address for RDMA. This can be used to
            avoid memory registration overhead during ZPush/ZPull.
   * \param addr the memory address
   * \param length the size of the memory buffer
   * \param gpu whether the address is on the GPU device
   */
  virtual void PinMemory(void *addr, size_t length, bool is_gpu,
                         int numa_or_gpu_index = 0) {
    CHECK(false) << "RDMA memory registration is not supported";
  }

  /**
   * \brief set the identity of the node
   */
  virtual void SetNode(const Node &node) { my_node_ = node; }

  /**
   * \brief get the node type {'fabric', 'zeromq', 'rdma'}
   */
  virtual std::string GetType() const = 0;

  static ErrHandle err_handle_;

 protected:
  /**
   * \brief get the length of pack meta
   */
  int GetPackMetaLen(const Meta &meta);

  /**
   * \brief pack meta into a string
   */
  void PackMeta(const Meta &meta, char **meta_buf, int *buf_size);

  /**
   * \brief unpack meta from a string
   */
  void UnpackMeta(const char *meta_buf, int buf_size, Meta *meta);

  bool IsValidPushpull(const Message &msg);

  Node scheduler_;
  Node my_node_;
  bool is_scheduler_;
  std::mutex start_mu_;

  Postoffice *postoffice_;

 private:
  /** thread function for receving */
  void Receiving();

  /** thread function for heartbeat */
  void Heartbeat();

  // node's address string (i.e. ip:port) -> node id
  // this map is updated when ip:port is received for the first time
  std::unordered_map<std::string, int> connected_nodes_;
  // maps the id of node which is added later to the id of node
  // which is with the same ip:port and added first
  std::unordered_map<int, int> shared_node_mapping_;

  /** whether it is ready for sending */
  std::atomic<bool> ready_{false};
  std::atomic<size_t> send_bytes_{0};
  size_t recv_bytes_ = 0;
  // number of server instances
  int num_servers_ = 0;
  // number of worker instances
  int num_workers_ = 0;
  /** the thread for receiving messages */
  std::unique_ptr<std::thread> receiver_thread_;
  /** the thread for sending heartbeat */
  std::unique_ptr<std::thread> heartbeat_thread_;
  // the count of instance barrier requests, used for instance-level barrier
  std::vector<int> barrier_count_;
  // the id of (group) barrier request senders, used for group-level barrier
  std::unordered_map<int, std::vector<int>> group_barrier_requests_;

  /** msg resender */
  Resender *resender_ = nullptr;
  int drop_rate_ = 0;
  std::atomic<int> timestamp_{0};
  int init_stage = 0;

  /**
   * \brief processing logic of AddNode message for scheduler
   */
  void ProcessAddNodeCommandAtScheduler(Message *msg, Meta *nodes,
                                        Meta *recovery_nodes);

  /**
   * \brief processing logic of Terminate message
   */
  void ProcessTerminateCommand();

  /**
   * \brief processing logic of AddNode message (run on each node)
   */
  void ProcessAddNodeCommand(Message *msg, Meta *nodes, Meta *recovery_nodes);

  /**
   * \brief processing logic of group-level Barrier message (run on each
   * postoffice instance group)
   */
  void ProcessBarrierCommand(Message *msg);

  /**
   * \brief processing logic of instance-level Barrier message (run on each
   * postoffice instance)
   */
  void ProcessInstanceBarrierCommand(Message *msg);

  /**
   * \brief processing logic of AddNode message (run on each node)
   */
  void ProcessHearbeat(Message *msg);

  /**
   * \brief processing logic of Data message
   */
  void ProcessDataMsg(Message *msg);

  /**
   * \brief called by ProcessAddNodeCommand, in scheduler it assigns an id to
   * the newly added node; in other nodes, it updates the node id with what is
   * received from scheduler
   */
  void UpdateLocalID(Message *msg, std::unordered_set<int> *deadnodes_set,
                     Meta *nodes, Meta *recovery_nodes);

  const char *heartbeat_timeout_val =
      Environment::Get()->find("PS_HEARTBEAT_TIMEOUT");
  int heartbeat_timeout_ =
      heartbeat_timeout_val ? atoi(heartbeat_timeout_val) : 0;

  DISALLOW_COPY_AND_ASSIGN(Van);
};
}  // namespace ps
#endif  // PS_INTERNAL_VAN_H_
