/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_CUSTOMER_H_
#define PS_INTERNAL_CUSTOMER_H_
#include <mutex>
#include <vector>
#include <utility>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <thread>
#include <memory>
#include "ps/internal/message.h"
#include "ps/internal/threadsafe_pqueue.h"
namespace ps {

/**
 * \brief The object for communication.
 *
 * As a sender, a customer tracks the responses for each request sent.
 *
 * It has its own receiving thread which is able to process any message received
 * from a remote node with `msg.meta.customer_id` equal to this customer's id
 * 每个SimpleApp对象持有一个Customer类的成员，且Customer需要在PostOffice进行注册，该类主要负责：
 * 1.跟踪由SimpleApp发送出去的消息的回复情况；
 * 2.维护一个Node的消息队列，为Node接收消息；
 * 一个 app 实例可以对应多个 Customer；
 * Customer 需要注册到 Postoffice 之中；
 */
/**
 * Customer 其实有两个功能：
 *    作为一个发送方，用于追踪SimpleApp发送出去每个Request对应的Response情况；
 *    作为接收方，因为有自己的接受线程和接受消息队列，所以Customer实际上是作为一个接受消息处理引擎（或者说是引擎的一部分）存在；
 * 具体特点如下：
 *    每个SimpleApp对象持有一个Customer类的成员，且Customer需要在PostOffice进行注册。
 *    因为 Customer 同时又要处理Message 但是其本身并没有接管网络，因此实际的Response和Message需要外部调用者告诉它，所以功能和职责上有点分裂。
 *    每一个连接对应一个Customer实例，每个Customer都与某个node id相绑定，代表当前节点发送到对应node id节点。连接对方的id和Customer实例的id相同。
 *    新建一次request，会返回一个timestamp，这个timestamp会作为这次request的id，每次请求会自增1，相应的res也会自增1，调用wait时会保证 后续比如做Wait以此为ID识别。
*/
/**
 * Van::ProcessDataMsg ---> Customer::Accept ---> Customer::recv_queue_ ---> Customer::recv_thread_ ---> Customer::recv_handle_ 
*/
/**
 * 为什么 Server 端，app_id 与 customer_id 相等？猜测是在 ps 代码中，Server 端也是有多个 cusomer，但是出于精简目的，在 ps-lite 之中删除了这部分功能，因此在 ps-lite 之中，app_id 与 customer_id 相等。
*/
class Customer {
 public:
  /**
   * \brief the handle for a received message
   * \param recved the received message
   */
  using RecvHandle = std::function<void(const Message& recved)>;

  /**
   * \brief constructor
   * \param app_id the globally unique id indicating the application the postoffice
   *               serving for
   * \param customer_id the locally unique id indicating the customer of a postoffice
   * \param recv_handle the functino for processing a received message
   */
  Customer(int app_id, int customer_id, const RecvHandle& recv_handle);

  /**
   * \brief desconstructor
   */
  ~Customer();

  /**
   * \brief return the globally unique application id
   */
  inline int app_id() { return app_id_; }


  /**
   * \brief return the locally unique customer id
   */
  inline int customer_id() { return customer_id_; }

  /**
   * \brief get a timestamp for a new request. threadsafe
   * \param recver the receive node id of this request
   * \return the timestamp of this request
   */
  int NewRequest(int recver);


  /**
   * \brief wait until the request is finished. threadsafe
   * \param timestamp the timestamp of the request
   */
  void WaitRequest(int timestamp);

  /**
   * \brief return the number of responses received for the request. threadsafe
   * \param timestamp the timestamp of the request
   */
  int NumResponse(int timestamp);

  /**
   * \brief add a number of responses to timestamp
   */
  void AddResponse(int timestamp, int num = 1);

  /**
   * \brief accept a received message from \ref Van. threadsafe
   * \param recved the received the message
   */
  inline void Accept(const Message& recved) {
    recv_queue_.Push(recved);
  }

 private:
  /**
   * \brief the thread function
   */
  void Receiving();

  int app_id_;

  int customer_id_;

/**
 * @brief 
 * 绑定Customer接收到request后的处理函数(SimpleApp::Process)；
 * Customer会新拉起一个线程，用于在customer生命周期内，使用recv_handle_来处理接受的请求，这里是使用了一个线程安全队列，Accept()用于往队列中一直发送消息，
 * 接受到的消息来自于Van的receiving thread，即每个节点的Van对象收到message后，根据message的不同，推送到不同的customer对象中。
 * 对于Worker，比如KVWorker，recv_handle_保存拉取的msg中的数据，
 * 对于Server,需要使用set_request_handle来设置对应的处理函数，如KVServerDefaultHandle，
 */
  RecvHandle recv_handle_; //worker 或者 server 的消息处理函数。
  ThreadsafePQueue recv_queue_; //线程安全的消息队列；
  std::unique_ptr<std::thread> recv_thread_;  //不断从 recv_queue 读取message并调用 recv_handle_；

  std::mutex tracker_mu_;
  std::condition_variable tracker_cond_;
  /**
   * @brief 
   * tracker_是Customer内用来记录request和response的状态的map。记录了每个 request（使用request id）可能发送了多少节点 以及 从多少个节点返回的 response的次数，
   * tracker_下标为每个request 的timestamp，即Request编号。
   * tracker_[i] . first 表示该请求发送给了多少节点，即本节点应收到的Response数量。
   * tracker_[i] . second 表示目前为止实际收到的Response数量。
   */
  std::vector<std::pair<int, int>> tracker_; //request & response 的同步变量。
  

  DISALLOW_COPY_AND_ASSIGN(Customer);
};

}  // namespace ps
#endif  // PS_INTERNAL_CUSTOMER_H_
