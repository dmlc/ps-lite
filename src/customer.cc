/**
 *  Copyright (c) 2015 by Contributors
 */
#include "ps/internal/customer.h"
#include "ps/internal/postoffice.h"
namespace ps {

const int Node::kEmpty = std::numeric_limits<int>::max();
const int Meta::kEmpty = std::numeric_limits<int>::max();

/**
 * @brief Construct a new Customer:: 
 * Customer object分别用传入构造函数的参数初始化 app_id_, custom_id_ , recv_handle成员
 * 调用PostOffice::AddCustomer将当前Customer注册到PostOffice；
 *    PostOffice的customers_成员: 在对应的app_id的元素上添加custom_id;
 *    PostOffice的barrier_done_成员将该custom_id的同步状态设为false
 * 新起一个Receiving线程recv_thread_；
 */
Customer::Customer(int app_id, int customer_id, const Customer::RecvHandle& recv_handle)
    : app_id_(app_id), customer_id_(customer_id), recv_handle_(recv_handle) {
  Postoffice::Get()->AddCustomer(this);
  recv_thread_ = std::unique_ptr<std::thread>(new std::thread(&Customer::Receiving, this));
}

Customer::~Customer() {
  Postoffice::Get()->RemoveCustomer(this);
  Message msg;
  msg.meta.control.cmd = Control::TERMINATE;
  recv_queue_.Push(msg);
  recv_thread_->join();
}

int Customer::NewRequest(int recver) {
  std::lock_guard<std::mutex> lk(tracker_mu_);
  int num = Postoffice::Get()->GetNodeIDs(recver).size(); // recver 可能会代表一个group
  tracker_.push_back(std::make_pair(num, 0));
  return tracker_.size() - 1; // 代表此次请求的时间戳timestamp，后续customer使用这个值代表这个request
}

//Worker pull 是异步操作，如果需要等待 pull 完成，则可以调用Wait来保证customer里面的request和response两者相等，即保证Pull完成后再做其他操作；
void Customer::WaitRequest(int timestamp) {
  std::unique_lock<std::mutex> lk(tracker_mu_);
  tracker_cond_.wait(lk, [this, timestamp]{
      return tracker_[timestamp].first == tracker_[timestamp].second;
    });
}

int Customer::NumResponse(int timestamp) {
  std::lock_guard<std::mutex> lk(tracker_mu_);
  return tracker_[timestamp].second;
}

void Customer::AddResponse(int timestamp, int num) {
  std::lock_guard<std::mutex> lk(tracker_mu_);
  tracker_[timestamp].second += num;
}
/**
 * @brief 
 * worker节点 或者 server节点 在程序的最开始会执行Postoffice::start()。
 * Postoffice::start()会初始化节点信息，并且调用Van::start()。
 * Van::start() 启动一个本地线程，使用Van::Receiving()来持续监听收到的message。
 * Van::Receiving()接收后消息之后，根据不同命令执行不同动作。针对数据消息，如果需要下一步处理，会调用 ProcessDataMsg：
 * 依据消息中的app id找到 Customer。
 * 将消息传递给Customer::Accept函数。
 * Customer::Accept() 函数将消息添加到一个队列recv_queue_；
 * Customer 对象本身也会启动一个接受线程 recv_thread_，使用 Customer::Receiving()
 * 从recv_queue_队列取消息。
 * 调用注册的recv_handle_函数对消息进行处理。
 */
void Customer::Receiving() {
  while (true) {
    Message recv;
    recv_queue_.WaitAndPop(&recv);
    if (!recv.meta.control.empty() &&
        recv.meta.control.cmd == Control::TERMINATE) {
      break;
    }
    recv_handle_(recv);
    if (!recv.meta.request) {
      std::lock_guard<std::mutex> lk(tracker_mu_);
      tracker_[recv.meta.timestamp].second++;
      tracker_cond_.notify_all();
    }
  }
}

}  // namespace ps
