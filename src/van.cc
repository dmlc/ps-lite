/**
 *  Copyright (c) 2015 by Contributors
 */

#include <chrono>
#include <thread>

#include "ps/base.h"
#include "ps/internal/customer.h"
#include "ps/internal/postoffice.h"
#include "ps/internal/van.h"
#include "ps/sarray.h"

#include "./meta.pb.h"
#include "./network_utils.h"
#include "./ibverbs_van.h"
#include "./resender.h"
#include "./zmq_van.h"
#include "./p3_van.h"

namespace ps {

// interval in second between to heartbeast signals. 0 means no heartbeat.
// don't send heartbeast in default. because if the scheduler received a
// heartbeart signal from a node before connected to that node, then it could be
// problem.
static const int kDefaultHeartbeatInterval = 0;

Van* Van::Create(const std::string& type) {
  if (type == "zmq") {
    return new ZMQVan();
  } else if (type == "p3") {
    return new P3Van();
#ifdef DMLC_USE_IBVERBS
} else if (type == "ibverbs") {
    return new IBVerbsVan();
#endif
  } else {
    LOG(FATAL) << "Unsupported van type: " << type;
    return nullptr;
  }
}

void Van::ProcessTerminateCommand() {
  PS_VLOG(1) << my_node().ShortDebugString() << " is stopped";
  ready_ = false;
}

/**
 * ProcessAddNodeCommandAtScheduler 是在 Scheduler 之内运行，是对控制类型消息的处理。
 * 对于Scheduler节点来说，scheduler收到所有worker和server的ADD_NODE的消息后进行节点id分配并应答，即，需要设定 最新的所有node的 全局rank 并发送给所有Worker和Server。
 *  当接受到所有 worker & server 的注册消息之后（nodes->control.node.size() == num_nodes）：
 *    将节点按照 ip + port 组合排序。
 *    Scheduler 与所有注册的节点建立连接、更新心跳时间戳，给 scheduler所有连接的节点分配全局 rank。
 *    向所有的worker和server发送ADD_NODE消息（携带scheduler之中的所有node信息）。
 *    会把 ready_ = true; 即 scheduler 是一个 ready 状态了，不管 worker 和 server 是否确认收到ADD_NODE消息。
 *    而在接收端（worker & server）的，每一个本地Node的全局rank等信息是由接收端 receiver_thread_（其他函数）获取，就是得到了 scheduler 返回的这些 nodes 信息。
 * 如果 !recovery_nodes->control.node.empty()，这就表明是处理某些重启节点的注册行为：
 *    查出心跳包超时的id，转存到dead_set之中。
 *    与重启节点建立连接（因为接收到了一个ADD_NODE），所以只与这个新重启节点建立连接即可（在代码中有 CHECK_EQ(recovery_nodes->control.node.size(), 1) 来确认重启节点为 1 个）。
 *    更新重启节点的心跳。
 *    因为新加入了重启节点，所以用一个发送达到两个目的：
 *        向所有 recovery 的worker和server发送ADD_NODE消息（携带scheduler之中的目前所有node信息）。
 *        向 alive 节点发送 recovery 节点信息。
 *        这样，收到消息的节点会则分别与新节点相互建立连接；
 */
void Van::ProcessAddNodeCommandAtScheduler(Message* msg, Meta* nodes,
                                           Meta* recovery_nodes) {
  recovery_nodes->control.cmd = Control::ADD_NODE;
  time_t t = time(NULL);
  size_t num_nodes =
      Postoffice::Get()->num_servers() + Postoffice::Get()->num_workers();
  if (nodes->control.node.size() == num_nodes) { // scheduler收到所有worker和server的ADD_NODE的消息后进行节点id分配并应答
    // sort the nodes according their ip and port,
    std::sort(nodes->control.node.begin(), nodes->control.node.end(),
              [](const Node& a, const Node& b) {
                return (a.hostname.compare(b.hostname) | (a.port < b.port)) > 0;  //根据IP和port给worker，server排个序
              });
    // assign node rank
    for (auto& node : nodes->control.node) {
      // 建立连接、更新心跳时间戳，给 scheduler所有连接的节点分配全局 rank。
      std::string node_host_ip =
          node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(node_host_ip) == connected_nodes_.end()) { //如果ip:port不存在van_中的话
        CHECK_EQ(node.id, Node::kEmpty); //判断是不是初始化节点
        int id = node.role == Node::SERVER
                     ? Postoffice::ServerRankToID(num_servers_)
                     : Postoffice::WorkerRankToID(num_workers_); //如果是sever的话，就id产生一个id号，num_servers_初始化为0
        PS_VLOG(1) << "assign rank=" << id << " to node " << node.DebugString();
        node.id = id; //将这个新节点的id赋值为id
        Connect(node); //连接这个新节点， 即建立一个socket, 然后senders_[id] = sender; 就是将目标id的socket存放起来后面使用
        Postoffice::Get()->UpdateHeartbeat(node.id, t);
        connected_nodes_[node_host_ip] = id; //既然 worker, server 已经发message来了，scheduler要把这个节点作为已经链接的节
      } else {
        int id = node.role == Node::SERVER
                     ? Postoffice::ServerRankToID(num_servers_)
                     : Postoffice::WorkerRankToID(num_workers_);
        shared_node_mapping_[id] = connected_nodes_[node_host_ip];
        node.id = connected_nodes_[node_host_ip];
      }
      if (node.role == Node::SERVER) num_servers_++;
      if (node.role == Node::WORKER) num_workers_++;
    }
    nodes->control.node.push_back(my_node_);
    nodes->control.cmd = Control::ADD_NODE;
    Message back;
    back.meta = *nodes;
    // 向所有已经和schedular建立连接的worker节点/server节点 广播此 "节点的加入信息“，并把 节点 2 请求连接的信息放入meta信息中。
    for (int r : Postoffice::Get()->GetNodeIDs(kWorkerGroup + kServerGroup)) {
      int recver_id = r;
      if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
        back.meta.recver = recver_id;
        back.meta.timestamp = timestamp_++;
        Send(back);
      }
    }
    PS_VLOG(1) << "the scheduler is connected to " << num_workers_
               << " workers and " << num_servers_ << " servers";
    ready_ = true; //scheduler已经准备好了
  } else if (!recovery_nodes->control.node.empty()) { // 节点没有收集完全
    auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout_);
    std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
    // send back the recovery node
    CHECK_EQ(recovery_nodes->control.node.size(), 1);
    Connect(recovery_nodes->control.node[0]);
    Postoffice::Get()->UpdateHeartbeat(recovery_nodes->control.node[0].id, t);
    Message back;
    for (int r : Postoffice::Get()->GetNodeIDs(kWorkerGroup + kServerGroup)) {
      if (r != recovery_nodes->control.node[0].id &&
          dead_set.find(r) != dead_set.end()) {
        // do not try to send anything to dead node
        continue;
      }
      // only send recovery_node to nodes already exist
      // but send all nodes to the recovery_node
      back.meta =
          (r == recovery_nodes->control.node[0].id) ? *nodes : *recovery_nodes;
      back.meta.recver = r;
      back.meta.timestamp = timestamp_++;
      Send(back);
    }
  }
}

/**
 * @brief 
 * 此函数作用是更新节点内部的node id 信息，也是分为两种情况，函数逻辑如下：
 * 如果msg->meta.sender是Meta::kEmpty，即未设定，则处理此message的一定是Scheduler，会进入 if 分支。
 *    如果目前 nodes 的control.node数目小于 "配置的server数目 + 配置的worker数目"，则说明是系统启动阶段，将当前消息的node信息加入到 control.node 之中。
 *    否则说明是系统运行阶段，应该是有些节点死掉重启后再次连接。那么，就从 nodes 的control.node 之中找到一个已经死掉的且节点role 与当前消息一致（同类型）的 node id，把这个 node id 赋给这个重启的节点。并且更新 nodes->control.node 和 recovery_nodes。
 * 下面就是普通节点处理的逻辑：
      即在 scheduler 传回来的所有节点信息中查找，目的是找到与自己的ip，port一致的节点。
      如果找到，就更新本地节点信息（因为在本节点启动时候，并没有设置 node_id 这个信息，这个需要scheduler统一设置，从注释看，目的是为了使重新注册成为可能）。包括全局 rank 信息。
 */
void Van::UpdateLocalID(Message* msg, std::unordered_set<int>* deadnodes_set,
                        Meta* nodes, Meta* recovery_nodes) {
  auto& ctrl = msg->meta.control;
  size_t num_nodes =
      Postoffice::Get()->num_servers() + Postoffice::Get()->num_workers();
  // assign an id
  if (msg->meta.sender == Meta::kEmpty) { //如果sender未设定，则处理此message的一定是Scheduler
    CHECK(is_scheduler_);
    CHECK_EQ(ctrl.node.size(), 1); //msg中的control命令中的节点集合就是worker自己，所以就是1个节点
    if (nodes->control.node.size() < num_nodes) {
      nodes->control.node.push_back(ctrl.node[0]);
    } else {  //如果所有work和server到齐了，就进入else
      // some node dies and restarts
      CHECK(ready_.load());
      for (size_t i = 0; i < nodes->control.node.size() - 1; ++i) {
        const auto& node = nodes->control.node[i];
        if (deadnodes_set->find(node.id) != deadnodes_set->end() &&
            node.role == ctrl.node[0].role) {
          auto& recovery_node = ctrl.node[0];
          // assign previous node id
          recovery_node.id = node.id;
          recovery_node.is_recovery = true;
          PS_VLOG(1) << "replace dead node " << node.DebugString()
                     << " by node " << recovery_node.DebugString();
          nodes->control.node[i] = recovery_node;
          recovery_nodes->control.node.push_back(recovery_node);
          break;
        }
      }
    }
  }

  // update my id / 对普通的node，更新其rank，scheduler 节点不会起作用（因为找不到）
  // schedule发给此work节点的消息，如果发现本地的ip和port和消息中的某个一点重合，那么就把本地节点的ID（初始化时候没有ID,只是等于Empty）改为schedule发过来的 node id。
  for (size_t i = 0; i < ctrl.node.size(); ++i) {
    const auto& node = ctrl.node[i];
    if (my_node_.hostname == node.hostname && my_node_.port == node.port) {
      if (getenv("DMLC_RANK") == nullptr || my_node_.id == Meta::kEmpty) {
        my_node_ = node;
        std::string rank = std::to_string(Postoffice::IDtoRank(node.id));
#ifdef _MSC_VER
        _putenv_s("DMLC_RANK", rank.c_str());
#else
        setenv("DMLC_RANK", rank.c_str(), true);
#endif
      }
    }
  }
}

void Van::ProcessHearbeat(Message* msg) {
  auto& ctrl = msg->meta.control;
  time_t t = time(NULL);
  for (auto& node : ctrl.node) {
    Postoffice::Get()->UpdateHeartbeat(node.id, t);
    if (is_scheduler_) {
      Message heartbeat_ack;
      heartbeat_ack.meta.recver = node.id;
      heartbeat_ack.meta.control.cmd = Control::HEARTBEAT;
      heartbeat_ack.meta.control.node.push_back(my_node_);
      heartbeat_ack.meta.timestamp = timestamp_++;
      // send back heartbeat
      Send(heartbeat_ack);
    }
  }
}

void Van::ProcessBarrierCommand(Message* msg) {
  auto& ctrl = msg->meta.control;
  if (msg->meta.request) { // 如果 msg->meta.request 为true，说明是 scheduler 收到消息进行处理，scheduler收到了消息，因为Postoffice::Barrier函数 会在发送时候做设置为true
    if (barrier_count_.empty()) {
      barrier_count_.resize(8, 0);
    }
    int group = ctrl.barrier_group;
    ++barrier_count_[group]; // Scheduler会对Barrier请求进行计数
    PS_VLOG(1) << "Barrier count for " << group << " : "
               << barrier_count_[group];
    if (barrier_count_[group] ==
        static_cast<int>(Postoffice::Get()->GetNodeIDs(group).size())) { // 如果相等，说明已经收到了最后一个请求，所以发送解除 barrier 消息
      barrier_count_[group] = 0;
      Message res;
      res.meta.request = false; //回复时候，这里就是false
      res.meta.app_id = msg->meta.app_id;
      res.meta.customer_id = msg->meta.customer_id;
      res.meta.control.cmd = Control::BARRIER;
      for (int r : Postoffice::Get()->GetNodeIDs(group)) {
        int recver_id = r;
        if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
          res.meta.recver = recver_id;
          res.meta.timestamp = timestamp_++;
          Send(res);
        }
      }
    }
  } else {  //说明这里收到了 barrier respones，可以解除 barrier了。具体见上面的设置为false处。
    Postoffice::Get()->Manage(*msg);
  }
}

/**
 * @brief 
 * 在 Van 中，我们可以看到，当处理数据消息时候，会：
 * 依据消息中的 app_id 从Postoffice 之中得到 customer_id；
 * 依据 customer_id 从 Postoffice 之中得到 Customer；
 * 调用 Customer 的 Accept 方法来处理消息；
 */

void Van::ProcessDataMsg(Message* msg) {
  // data msg
  CHECK_NE(msg->meta.sender, Meta::kEmpty);
  CHECK_NE(msg->meta.recver, Meta::kEmpty);
  CHECK_NE(msg->meta.app_id, Meta::kEmpty);
  int app_id = msg->meta.app_id;
  int customer_id =
      Postoffice::Get()->is_worker() ? msg->meta.customer_id : app_id;
  auto* obj = Postoffice::Get()->GetCustomer(app_id, customer_id, 5);
  CHECK(obj) << "timeout (5 sec) to wait App " << app_id << " customer "
             << customer_id << " ready at " << my_node_.role;
  obj->Accept(*msg);
}

/**
 * @brief 
 * 查出心跳包超时的id，转存到dead_set之中。
 * 拿到收到消息里面的control信息。
 * 调用 UpdateLocalID，在其中：
 *    如果是新node，Scheduler记录这个新的node。
 *    如果这个node是重启产生的，则将旧node的信息更新。
 * 如果是 scheduler，则：
 *    调用 ProcessAddNodeCommandAtScheduler 收到所有worker和server的ADD_NODE 的消息后进行节点id分配并应答，即 设定最新的所有node的rank并发送给所有Worker和Server。
 * 如果不是 scheduler，说明 work & server 收到了 scheduler 回答的 ADD_NODE 消息，则：
 *    如果自身是现有节点，则在 connected_nodes_ 之中不会找到这个新节点，则先有节点会调用 Connect 与新节点建立连接。
 *    如果自身是新节点，则会连接所有现有节点（非同类型）。
 *    在 connected_nodes_ 之中更新 全局节点信息，包括 global rank（本地Node的全局rank等信息是由receiver_thread_在这里获取）；
 *    最后设置 ready_ = true，即本节点也可以运行了，因为主线程会阻塞在其上。
 */
void Van::ProcessAddNodeCommand(Message* msg, Meta* nodes,
                                Meta* recovery_nodes) {
  auto dead_nodes = Postoffice::Get()->GetDeadNodes(heartbeat_timeout_); //查出心跳包超时的id
  std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end()); //转存到dead_set之中
  auto& ctrl = msg->meta.control; //拿到收到消息里面的control信息

  UpdateLocalID(msg, &dead_set, nodes, recovery_nodes);

  if (is_scheduler_) {
    ProcessAddNodeCommandAtScheduler(msg, nodes, recovery_nodes);
  } else {
    for (const auto& node : ctrl.node) {
      std::string addr_str = node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(addr_str) == connected_nodes_.end()) { // 现有连接中没有这个新节点
        //现有节点会在自己连接之中查找这个新节点，发现现有连接中没有这个新节点
        Connect(node);  // 与新节点进行连接
        connected_nodes_[addr_str] = node.id; // 加入已经连接的节点
      }
      if (!node.is_recovery && node.role == Node::SERVER) ++num_servers_;
      if (!node.is_recovery && node.role == Node::WORKER) ++num_workers_;
    }
    PS_VLOG(1) << my_node_.ShortDebugString() << " is connected to others";
    ready_ = true;
  }
}

/**
 * @brief 
 * Van对象的初始化函数作用就是依据本地节点类型的不同，做不同设置，从而启动端口，建立到scheduler的连结，启动接收消息线程，心跳线程等，这样就可以进行通信了。具体如下：
 * 1.首先从环境变量中得到相关信息，比如scheduler 的 "ip，port"（这两个是预先设置的），本节点的角色（Worker/Server/Scheduler）等等，然后 初始化scheduler_这个成员变量；
 * 2.如果本节点是 scheduler，则把 scheduler_ 赋值给 my_node_；
 * 3.如果本节点不是 scheduler，则：
 *  （1）从系统中获取本节点的ip信息；
 *  （2）使用 GetAvailablePort 获取一个port；
 * 4.使用 Bind 绑定一个端口；
 * 5.调用 Connect 建立到 Scheduler 的连接（scheduler也连接到自己的那个预先设置的固定端口）；
 * 6.启动本地Node的接收消息线程receiver_thread_，执行Van::Receiving ；
 * 7.如果本节点不是 scheduler，给 Scheduler 发送一个 ADD_NODE 消息，这样可以将本地Node的信息告知Scheduler，即注册到 scheduler;
 * 8.然后进入等待状态，等待Scheduler通知 Ready（scheduler 会等待所有节点都完成注册后，统一发送 ready）; 注意，这里 scheduler 节点也会等，但是不影响 scheduler 节点 的 recevie 线程接受处理消息；
 * 9.Ready后启动心跳线程，建立到Scheduler的Heartbeat 连接;
 * 关于7，8两点的进一步说明就是：
 *   当worker和server节点绑定ip和port后，便向scheduler节点发送ADD_NODE message。
 *   当 scheduler收到所有worker和server的ADD_NODE message后，则依次应答ADD_NODE message，
 *   各个节点在此过程中通过原子变量ready_等待上述过程完成。
 * 
 */
void Van::Start(int customer_id) {
  // get scheduler info
  start_mu_.lock();

  if (init_stage == 0) {
    scheduler_.hostname = std::string(
        CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_URI")));
    scheduler_.port =
        atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_PORT")));
    scheduler_.role = Node::SCHEDULER;
    scheduler_.id = kScheduler;
    // 确认本节点是scheduler节点
    is_scheduler_ = Postoffice::Get()->is_scheduler();

    // get my node info
    if (is_scheduler_) {
      my_node_ = scheduler_;
    } else {
      auto role = Postoffice::Get()->is_worker() ? Node::WORKER : Node::SERVER;
      const char* nhost = Environment::Get()->find("DMLC_NODE_HOST");
      std::string ip;
      if (nhost) ip = std::string(nhost);
      if (ip.empty()) {
        const char* itf = Environment::Get()->find("DMLC_INTERFACE");
        std::string interface;
        if (itf) interface = std::string(itf);
        if (interface.size()) {
          GetIP(interface, &ip);
        } else {
          GetAvailableInterfaceAndIP(&interface, &ip);
        }
        CHECK(!interface.empty()) << "failed to get the interface";
      }
      int port = GetAvailablePort();
      const char* pstr = Environment::Get()->find("PORT");
      if (pstr) port = atoi(pstr);
      CHECK(!ip.empty()) << "failed to get ip";
      CHECK(port) << "failed to get a port";
      my_node_.hostname = ip;
      my_node_.role = role;
      my_node_.port = port;
      // cannot determine my id now, the scheduler will assign it later
      // set it explicitly to make re-register within a same process possible
      my_node_.id = Node::kEmpty;
      my_node_.customer_id = customer_id;
    }

    // bind.
    // 绑定接口,把本节点绑定到ip:port这个socket上，理论来说这个函数就是初始化了receiver_
    my_node_.port = Bind(my_node_, is_scheduler_ ? 0 : 40);
    PS_VLOG(1) << "Bind to " << my_node_.DebugString();
    CHECK_NE(my_node_.port, -1) << "bind failed";

    // connect to the scheduler
    // 连接上scheduler_,由于本节点就是scheduler_,其实就是初始化senders_,由于发送的节点很多，所以这里是一个map<int,void*>
	  // 在这里就是senders_[1] = socket_1, socket_1中的body设置一点字符“ps1***”, 注意链接不是sendMsg
    Connect(scheduler_);

    // for debug use
    if (Environment::Get()->find("PS_DROP_MSG")) {
      drop_rate_ = atoi(Environment::Get()->find("PS_DROP_MSG"));
    }
    // start receiver
    // 开启一个接收消息的线程，这里就是处理消息
    receiver_thread_ =
        std::unique_ptr<std::thread>(new std::thread(&Van::Receiving, this));
    init_stage++;
  }
  start_mu_.unlock();

  if (!is_scheduler_) {
    // let the scheduler know myself
    // worker和server节点会通过 ADD_NODE 消息把本地节点的信息告诉scheduler，比如角色，ip，port...
    Message msg;
    Node customer_specific_node = my_node_;
    customer_specific_node.customer_id = customer_id;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::ADD_NODE;
    msg.meta.control.node.push_back(customer_specific_node);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }

  // wait until ready
  // 等待 ready_ 从false变成true，当是scheduler的时候，必须要有等worker和server节点过来，不然一直都是阻塞在这，如果是 worker/server，则是等待 scheduler 发送系统allready消息。
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  start_mu_.lock();
  if (init_stage == 1) {
    // resender
    if (Environment::Get()->find("PS_RESEND") &&
        atoi(Environment::Get()->find("PS_RESEND")) != 0) {
      int timeout = 1000;
      if (Environment::Get()->find("PS_RESEND_TIMEOUT")) {
        timeout = atoi(Environment::Get()->find("PS_RESEND_TIMEOUT"));
      }
      // 如果设置了超时重传，就初始化resender_这个变量
      resender_ = new Resender(timeout, 10, this);
    }

    //启动了一个线程，每一个 Worker/Server 节点，每隔 PS_HEARTBEAT_INTERVAL 秒向 Scheduler 发送一条 HEARTBEAT 消息：
    if (!is_scheduler_) {
      // start heartbeat thread
      // 初始化心跳线程
      heartbeat_thread_ =
          std::unique_ptr<std::thread>(new std::thread(&Van::Heartbeat, this));
    }
    init_stage++;
  }
  start_mu_.unlock();
}

void Van::Stop() {
  // stop threads
  Message exit;
  exit.meta.control.cmd = Control::TERMINATE;
  exit.meta.recver = my_node_.id;
  // only customer 0 would call this method
  exit.meta.customer_id = 0;
  int ret = SendMsg(exit);
  CHECK_NE(ret, -1);
  receiver_thread_->join();
  init_stage = 0;
  if (!is_scheduler_) heartbeat_thread_->join();
  if (resender_) delete resender_;
  ready_ = false;
  connected_nodes_.clear();
  shared_node_mapping_.clear();
  send_bytes_ = 0;
  timestamp_ = 0;
  my_node_.id = Meta::kEmpty;
  barrier_count_.clear();
}

int Van::Send(const Message& msg) {
  int send_bytes = SendMsg(msg);
  CHECK_NE(send_bytes, -1);
  send_bytes_ += send_bytes;
  if (resender_) resender_->AddOutgoing(msg);
  if (Postoffice::Get()->verbose() >= 2) {
    PS_VLOG(2) << msg.DebugString();
  }
  return send_bytes;
}

void Van::Receiving() {
  Meta nodes; //只有 scheduler 在处理 ADD_NODE 时候会用到，存储目前 scheduler 内部拥有的所有 nodes；
  Meta recovery_nodes;  // store recovery nodes，只有 scheduler 在处理 ADD_NODE 时候会用到，存储目前 scheduler 内部拥有的所有 recovery nodes（康复重启的节点）；
  recovery_nodes.control.cmd = Control::ADD_NODE;

  while (true) {
    Message msg;
    int recv_bytes = RecvMsg(&msg);
    // For debug, drop received message
    if (ready_.load() && drop_rate_ > 0) {
      unsigned seed = time(NULL) + my_node_.id;
      if (rand_r(&seed) % 100 < drop_rate_) {
        LOG(WARNING) << "Drop message " << msg.DebugString();
        continue;
      }
    }

    CHECK_NE(recv_bytes, -1);
    recv_bytes_ += recv_bytes;
    if (Postoffice::Get()->verbose() >= 2) {
      PS_VLOG(2) << msg.DebugString();
    }
    // duplicated message
    if (resender_ && resender_->AddIncomming(msg)) continue; //重传确认机制

    if (!msg.meta.control.empty()) { //如果是控制类型的消息
      // control msg
      auto& ctrl = msg.meta.control;
      if (ctrl.cmd == Control::TERMINATE) {
        ProcessTerminateCommand();
        break;
      } else if (ctrl.cmd == Control::ADD_NODE) {
        ProcessAddNodeCommand(&msg, &nodes, &recovery_nodes); //当执行到这个位置的时候继续跳转
      } else if (ctrl.cmd == Control::BARRIER) {
        ProcessBarrierCommand(&msg);
      } else if (ctrl.cmd == Control::HEARTBEAT) {
        ProcessHearbeat(&msg); // 发回Heartbeat的ACK
      } else {
        LOG(WARNING) << "Drop unknown typed message " << msg.DebugString();
      }
    } else { //非控制类型的消息处理方式
      ProcessDataMsg(&msg);
    }
  }
}

void Van::PackMetaPB(const Meta& meta, PBMeta* pb) {
  pb->set_head(meta.head);
  if (meta.app_id != Meta::kEmpty) pb->set_app_id(meta.app_id);
  if (meta.timestamp != Meta::kEmpty) pb->set_timestamp(meta.timestamp);
  if (meta.body.size()) pb->set_body(meta.body);
  pb->set_push(meta.push);
  pb->set_request(meta.request);
  pb->set_simple_app(meta.simple_app);
  pb->set_priority(meta.priority);
  pb->set_customer_id(meta.customer_id);
  for (auto d : meta.data_type) pb->add_data_type(d);
  if (!meta.control.empty()) {
    auto ctrl = pb->mutable_control();
    ctrl->set_cmd(meta.control.cmd);
    if (meta.control.cmd == Control::BARRIER) {
      ctrl->set_barrier_group(meta.control.barrier_group);
    } else if (meta.control.cmd == Control::ACK) {
      ctrl->set_msg_sig(meta.control.msg_sig);
    }
    for (const auto& n : meta.control.node) {
      auto p = ctrl->add_node();
      p->set_id(n.id);
      p->set_role(n.role);
      p->set_port(n.port);
      p->set_hostname(n.hostname);
      p->set_is_recovery(n.is_recovery);
      p->set_customer_id(n.customer_id);
    }
  }
  pb->set_data_size(meta.data_size);
}

void Van::PackMeta(const Meta& meta, char** meta_buf, int* buf_size) {
  // convert into protobuf
  PBMeta pb;
  pb.set_head(meta.head);
  if (meta.app_id != Meta::kEmpty) pb.set_app_id(meta.app_id);
  if (meta.timestamp != Meta::kEmpty) pb.set_timestamp(meta.timestamp);
  if (meta.body.size()) pb.set_body(meta.body);
  pb.set_push(meta.push);
  pb.set_pull(meta.pull);
  pb.set_request(meta.request);
  pb.set_simple_app(meta.simple_app);
  pb.set_priority(meta.priority);
  pb.set_customer_id(meta.customer_id);
  for (auto d : meta.data_type) pb.add_data_type(d);
  if (!meta.control.empty()) {
    auto ctrl = pb.mutable_control();
    ctrl->set_cmd(meta.control.cmd);
    if (meta.control.cmd == Control::BARRIER) {
      ctrl->set_barrier_group(meta.control.barrier_group);
    } else if (meta.control.cmd == Control::ACK) {
      ctrl->set_msg_sig(meta.control.msg_sig);
    }
    for (const auto& n : meta.control.node) {
      auto p = ctrl->add_node();
      p->set_id(n.id);
      p->set_role(n.role);
      p->set_port(n.port);
      p->set_hostname(n.hostname);
      p->set_is_recovery(n.is_recovery);
      p->set_customer_id(n.customer_id);
    }
  }

  // to string
  *buf_size = pb.ByteSize();
  *meta_buf = new char[*buf_size + 1];
  CHECK(pb.SerializeToArray(*meta_buf, *buf_size))
      << "failed to serialize protobuf";
}

void Van::UnpackMeta(const char* meta_buf, int buf_size, Meta* meta) {
  // to protobuf
  PBMeta pb;
  CHECK(pb.ParseFromArray(meta_buf, buf_size))
      << "failed to parse string into protobuf";

  // to meta
  meta->head = pb.head();
  meta->app_id = pb.has_app_id() ? pb.app_id() : Meta::kEmpty;
  meta->timestamp = pb.has_timestamp() ? pb.timestamp() : Meta::kEmpty;
  meta->request = pb.request();
  meta->push = pb.push();
  meta->pull = pb.pull();
  meta->simple_app = pb.simple_app();
  meta->priority = pb.priority();
  meta->body = pb.body();
  meta->customer_id = pb.customer_id();
  meta->data_type.resize(pb.data_type_size());
  for (int i = 0; i < pb.data_type_size(); ++i) {
    meta->data_type[i] = static_cast<DataType>(pb.data_type(i));
  }
  if (pb.has_control()) {
    const auto& ctrl = pb.control();
    meta->control.cmd = static_cast<Control::Command>(ctrl.cmd());
    meta->control.barrier_group = ctrl.barrier_group();
    meta->control.msg_sig = ctrl.msg_sig();
    for (int i = 0; i < ctrl.node_size(); ++i) {
      const auto& p = ctrl.node(i);
      Node n;
      n.role = static_cast<Node::Role>(p.role());
      n.port = p.port();
      n.hostname = p.hostname();
      n.id = p.has_id() ? p.id() : Node::kEmpty;
      n.is_recovery = p.is_recovery();
      n.customer_id = p.customer_id();
      meta->control.node.push_back(n);
    }
  } else {
    meta->control.cmd = Control::EMPTY;
  }
}

void Van::Heartbeat() {
  const char* val = Environment::Get()->find("PS_HEARTBEAT_INTERVAL");
  const int interval = val ? atoi(val) : kDefaultHeartbeatInterval;
  while (interval > 0 && ready_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    Message msg;
    msg.meta.recver = kScheduler;
    msg.meta.control.cmd = Control::HEARTBEAT;
    msg.meta.control.node.push_back(my_node_);
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }
}
}  // namespace ps
