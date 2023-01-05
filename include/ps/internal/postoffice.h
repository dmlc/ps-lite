/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_POSTOFFICE_H_
#define PS_INTERNAL_POSTOFFICE_H_
#include <mutex>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <memory>
#include "ps/range.h"
#include "ps/internal/env.h"
#include "ps/internal/customer.h"
#include "ps/internal/van.h"
namespace ps {
/**
 * \brief the center of the system
 * 一个单例模式的全局管理类，一个 node 在生命期内具有一个PostOffice，依赖它的类成员对Node进行管理；
 * 具有如下特点：
 * 三种Node角色都依赖 Postoffice 进行管理，每一个 node 在生命期内具有一个单例 PostOffice。
 * 如我们之前所说，ps-lite的特点是 worker, server, scheduler 都使用同一套代码，Postoffice也是如此，所以我们最好分开描述。
 * 在 Scheduler侧，顾名思义，Postoffice 是邮局，可以认为是一个地址簿，一个调控中心，其记录了系统（由scheduler，server， worker 集体构成的这个系统）中所有节点的信息。具体功能如下：
 *    维护了一个Van对象，负责整个网络的拉起、通信、命令管理如增加节点、移除节点、恢复节点等等；
 *    负责整个集群基本信息的管理，比如worker、server数的获取，管理所有节点的地址，server 端 feature分布的获取，worker/server Rank与node id的互转，节点角色身份等等；
 *    负责 Barrier 功能；
 * 在 Server / Worker 端，负责：
 *    配置当前node的一些信息，例如当前node是哪种类型(server，worker)，nodeid是啥，以及worker/server 的rank 到 node id的转换。
 *    路由功能：负责 key 与 server 的对应关系。
 *    Barrier 功能；
 */
class Postoffice {
 public:
  /**
   * \brief return the singleton object
   */
  static Postoffice* Get() {
    static Postoffice e; return &e;
  }
  /** \brief get the van */
  Van* van() { return van_; }
  /**
   * \brief start the system
   *
   * This function will block until every nodes are started.
   * \param argv0 the program name, used for logging.
   * \param do_barrier whether to block until every nodes are started.
   */
  void Start(int customer_id, const char* argv0, const bool do_barrier); //建立通信初始化
  /**
   * \brief terminate the system
   *
   * All nodes should call this function before existing.
   * \param do_barrier whether to do block until every node is finalized, default true.
   */
  void Finalize(const int customer_id, const bool do_barrier = true); //节点阻塞退出
  /**
   * \brief add an customer to the system. threadsafe
   */
  void AddCustomer(Customer* customer);
  /**
   * \brief remove a customer by given it's id. threasafe
   */
  void RemoveCustomer(Customer* customer);
  /**
   * \brief get the customer by id, threadsafe
   * \param app_id the application id
   * \param customer_id the customer id
   * \param timeout timeout in sec
   * \return return nullptr if doesn't exist and timeout
   */
  Customer* GetCustomer(int app_id, int customer_id, int timeout = 0) const;
  /**
   * \brief get the id of a node (group), threadsafe
   *
   * if it is a  node group, return the list of node ids in this
   * group. otherwise, return {node_id}
   */
  const std::vector<int>& GetNodeIDs(int node_id) const {
    const auto it = node_ids_.find(node_id);
    CHECK(it != node_ids_.cend()) << "node " << node_id << " doesn't exist";
    return it->second;
  }
  /**
   * \brief return the key ranges of all server nodes
   */
  /**
   * 将int范围按照server个数均分
  */
  const std::vector<Range>& GetServerKeyRanges();
  /**
   * \brief the template of a callback
   */
  using Callback = std::function<void()>;
  /**
   * \brief Register a callback to the system which is called after Finalize()
   *
   * The following codes are equal
   * \code {cpp}
   * RegisterExitCallback(cb);
   * Finalize();
   * \endcode
   *
   * \code {cpp}
   * Finalize();
   * cb();
   * \endcode
   * \param cb the callback function
   */
  void RegisterExitCallback(const Callback& cb) {
    exit_callback_ = cb;
  }
  /**
   * \brief convert from a worker rank into a node id
   * \param rank the worker rank
   */
  /**
   * 逻辑rankid映射到物理node id，Node id 是物理节点的唯一标识，可以和一个 host + port 的二元组唯一对应，1-7 的id表示的是node group，单个节点的id 就从 8 开始
   * 而且这个算法保证server id为偶数，worker id为奇数
  */
  static inline int WorkerRankToID(int rank) {
    return rank * 2 + 9;
  }
  /**
   * \brief convert from a server rank into a node id
   * \param rank the server rank
   */
  static inline int ServerRankToID(int rank) {
    return rank * 2 + 8;
  }
  /**
   * \brief convert from a node id into a server or worker rank
   * \param id the node id
   */
  static inline int IDtoRank(int id) {
#ifdef _MSC_VER
#undef max
#endif
    return std::max((id - 8) / 2, 0);
  }
  /** \brief Returns the number of worker nodes */
  int num_workers() const { return num_workers_; }
  /** \brief Returns the number of server nodes */
  int num_servers() const { return num_servers_; }
  /** \brief Returns the rank of this node in its group
   *
   * Each worker will have a unique rank within [0, NumWorkers()). So are
   * servers. This function is available only after \ref Start has been called.
   */
  int my_rank() const { return IDtoRank(van_->my_node().id); }
  /** \brief Returns true if this node is a worker node */
  int is_worker() const { return is_worker_; }
  /** \brief Returns true if this node is a server node. */
  int is_server() const { return is_server_; }
  /** \brief Returns true if this node is a scheduler node. */
  int is_scheduler() const { return is_scheduler_; }
  /** \brief Returns the verbose level. */
  int verbose() const { return verbose_; }
  /** \brief Return whether this node is a recovery node */
  bool is_recovery() const { return van_->my_node().is_recovery; }
  /**
   * \brief barrier
   * \param node_id the barrier group id
   */
  void Barrier(int customer_id, int node_group); //进入 barrier 阻塞状态
  /**
   * \brief process a control message, called by van
   * \param the received message
   */
  void Manage(const Message& recv); //退出 barrier 阻塞状态
  /**
   * \brief update the heartbeat record map
   * \param node_id the \ref Node id
   * \param t the last received heartbeat time
   * 存储了心跳关联的节点的活跃信息。键为节点编号，值为上次收到其 HEARTBEAT 消息的时间戳。
   */
  void UpdateHeartbeat(int node_id, time_t t) {
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    heartbeats_[node_id] = t;
  }
  /**
   * \brief get node ids that haven't reported heartbeats for over t seconds
   * \param t timeout in sec
   */
  std::vector<int> GetDeadNodes(int t = 60); //根据 heartbeats_ 获取已经 dead 的节点；

 private:
  Postoffice();
  ~Postoffice() { delete van_; }

  void InitEnvironment(); //初始化环境变量，创建 van 对象；
  Van* van_; //底层通信对象
  mutable std::mutex mu_;
  // app_id -> (customer_id -> customer pointer)
  std::unordered_map<int, std::unordered_map<int, Customer*>> customers_; //本节点目前有哪些customer
  std::unordered_map<int, std::vector<int>> node_ids_;  //node id映射表
  std::mutex server_key_ranges_mu_;
  std::vector<Range> server_key_ranges_; //Server key 区间范围对象
  bool is_worker_, is_server_, is_scheduler_; //标注了本节点类型
  int num_servers_, num_workers_; //节点心跳对象
  std::unordered_map<int, std::unordered_map<int, bool> > barrier_done_; //Barrier 同步变量
  int verbose_;
  std::mutex barrier_mu_;
  std::condition_variable barrier_cond_;
  std::mutex heartbeat_mu_;
  std::mutex start_mu_;
  int init_stage_ = 0;
  std::unordered_map<int, time_t> heartbeats_;
  Callback exit_callback_;
  /** \brief Holding a shared_ptr to prevent it from being destructed too early */
  std::shared_ptr<Environment> env_ref_;
  time_t start_time_;
  DISALLOW_COPY_AND_ASSIGN(Postoffice);
};

/** \brief verbose log */
#define PS_VLOG(x) LOG_IF(INFO, x <= Postoffice::Get()->verbose())
}  // namespace ps
#endif  // PS_INTERNAL_POSTOFFICE_H_
