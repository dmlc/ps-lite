/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_VAN_H_
#define PS_INTERNAL_VAN_H_
#include <atomic>
#include <ctime>
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
class PBMeta;
/**
 * \brief Van sends messages to remote nodes
 *
 * If environment variable PS_RESEND is set to be 1, then van will resend a
 * message if it no ACK messsage is received within PS_RESEND_TIMEOUT
 * millisecond
 * 通信模块，负责与其他节点的网络通信和Message的实际收发工作。PostOffice持有一个Van成员；相当于邮局里有了地址簿，就需要有货车来负责拉送物件，Van 就是整个Parameter Server的通信模块
 * Van 负责具体的节点间通信。具体来说就是负责建立起节点之间的互相连接（例如Worker与Scheduler之间的连接），并且开启本地的receiving thread用来监听收到的message。
 */
class Van {
 public:
  /**
   * \brief create Van
   * \param type zmq, socket, ...
   */
  static Van *Create(const std::string &type);

  /** \brief constructer, do nothing. use \ref Start for real start */
  Van() {}

  /**\brief deconstructer, do nothing. use \ref Stop for real stop */
  virtual ~Van() {}

  /**
   * \brief start van
   *
   * must call it before calling Send
   *
   * it initializes all connections to other nodes.  start the receiving
   * thread, which keeps receiving messages. if it is a system
   * control message, give it to postoffice::manager, otherwise, give it to the
   * corresponding app.
   */
  virtual void Start(int customer_id); //建立通信初始化；

  /**
   * \brief send a message, It is thread-safe
   * \return the number of bytes sent. -1 if failed
   */
  int Send(const Message &msg);

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

 protected:
  /**
   * \brief connect to a node
   * 连接节点
   */
  virtual void Connect(const Node &node) = 0;

  /**
   * \brief bind to my node
   * do multiple retries on binding the port. since it's possible that
   * different nodes on the same machine picked the same port
   * \return return the port binded, -1 if failed.
   * 绑定到自己节点之上 
   */
  virtual int Bind(const Node &node, int max_retry) = 0;

  /**
   * \brief block until received a message
   * \return the number of bytes received. -1 if failed or timeout
   * 接收消息，用阻塞方式  
   */
  virtual int RecvMsg(Message *msg) = 0;

  /**
   * \brief send a mesage
   * \return the number of bytes sent
   * 发送消息
   */
  virtual int SendMsg(const Message &msg) = 0;

  /**
   * \brief pack meta into a string
   */
  void PackMeta(const Meta &meta, char **meta_buf, int *buf_size);

  /**
   * \brief pack meta into protobuf
   */
  void PackMetaPB(const Meta &meta, PBMeta *pb);

  /**
   * \brief unpack meta from a string
   */
  void UnpackMeta(const char *meta_buf, int buf_size, Meta *meta);

  Node scheduler_; //Scheduler 节点参数，每一个node都会记录Scheduler 节点的信息；
  Node my_node_; //本节点参数。如果本节点是Scheduler，则 my_node_ 会指向上面的 scheduler_ ；
  bool is_scheduler_; //本节点是否是 scheduler;
  std::mutex start_mu_;

 private:
  /** thread function for receving */
  void Receiving(); //接收消息线程的处理函数；

  /** thread function for heartbeat */
  void Heartbeat(); //发送心跳线程的处理函数；

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
  int num_servers_ = 0;
  int num_workers_ = 0;
  /** the thread for receiving messages */
  std::unique_ptr<std::thread> receiver_thread_; //接收消息线程指针；
  /** the thread for sending heartbeat */
  std::unique_ptr<std::thread> heartbeat_thread_; //发送心跳线程指针；
  std::vector<int> barrier_count_; //barrier 计数，用来记录登记节点数目，只有所有节点都登记之后，系统才到了 ready 状态，scheduler 才会给所有节点发送 ready 消息，系统才正式启动。
  /** msg resender */
  Resender *resender_ = nullptr; //重新发送消息指针;
  int drop_rate_ = 0;
  std::atomic<int> timestamp_{0}; //message 自增 id，原子变量;
  int init_stage = 0; //记录了目前连接到哪些 nodes;

  /**
   * \brief processing logic of AddNode message for scheduler
   * scheduler 的 AddNode 消息处理函数；
   */
  void ProcessAddNodeCommandAtScheduler(Message *msg, Meta *nodes, 
                                        Meta *recovery_nodes);

  /**
   * \brief processing logic of Terminate message
   */
  void ProcessTerminateCommand();

  /**
   * \brief processing logic of AddNode message (run on each node)
   * worker 和 server 的 AddNode 消息处理函数；
   */
  void ProcessAddNodeCommand(Message *msg, Meta *nodes, Meta *recovery_nodes);

  /**
   * \brief processing logic of Barrier message (run on each node)
   * Barrier 消息处理函数；
   */
  void ProcessBarrierCommand(Message *msg);

  /**
   * \brief processing logic of AddNode message (run on each node)
   * 心跳包处理函数；
   */
  void ProcessHearbeat(Message *msg);

  /**
   * \brief processing logic of Data message
   * 数据消息（push & pull）处理函数；
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
