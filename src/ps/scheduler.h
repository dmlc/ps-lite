/**
 * @file   scheduler.h
 * @brief  Scheduler API
 */
#include "base/common.h"
#include "base/assign_op.h"
#include "ps/app.h"
#include "ps/node_info.h"
#include "proto/assign_op.pb.h"
namespace ps {

/**
 * \brief Slave allows a worker/server to communicate with the scheduler. See
 * \ref Root for the according class for the scheduler
 *
 * \tparam Val the value type
 */
template <typename Val>
class Slave : public Customer {
 public:
  explicit Slave(int id = NextID())
      : Customer(id), root_(NodeInfo::SchedulerID()) { }
  virtual ~Slave() { }

  /**
   * \brief Push a vector to the scheduler
   *
   * \tparam Val the value type
   * @param vals the value vector
   * @param reduce the reduce operator, can be PLUS, TIMES, OR, ...
   * @param opts push options
   *
   * @return the timestamp of this request
   */
  int Push(const std::vector<Val>& vals,
           const AsOp& reduce = AsOp::PLUS,
           const SyncOpts& opts = SyncOpts()) {
    Message msg(opts.GetTask(), root_);
    msg.task.set_op(reduce);
    msg.task.set_cmd(kPush);
    msg.add_value(SArray<Val>(vals));
    return Submit(&msg);
  }

  /**
   * \brief Pull a vector from the scheduler
   *
   * @param vals the value vector
   * @param opts push options
   *
   * @return the timestamp of this request
   */
  int Pull(std::vector<Val>* vals, const SyncOpts& opts = SyncOpts()) {
    Task task = opts.GetTask();
    task.set_cmd(kPull);
    int ts = Submit(task, root_);
    recv_buf_[ts] = vals;
    return ts;
  }

  // implementation

  static const int kPush = 1;
  static const int kPull = 2;

  virtual void ProcessResponse(Message* response) {
    const auto& task = response->task;
    if (task.cmd() == kPull) {
      int t = task.time();
      auto it = recv_buf_.find(t);
      CHECK(it != recv_buf_.end()) << "no message sent at time " << t;
      CHECK_EQ(response->value.size(), 1);
      SArray<Val> recv(response->value[0]);
      it->second->resize(recv.size());
      memcpy(it->second->data(), recv.data(), recv.size()*sizeof(Val));
      recv_buf_.erase(it);
    }
  }
 private:
  NodeID root_;
  std::unordered_map<int, std::vector<Val>*> recv_buf_;
};

/**
 * \brief APIs for the scheduler. See \ref Slave for worker/server APIs
 */
template <typename Val>
class Root : public Customer {
 public:
  explicit Root(int id = NextID()) : Customer(id) { }
  ~Root() { }

  /**
   * \brief Get the received (and reduced) vector from servers/workers
   *
   * System will clear the received data
   * \param vals the value vector.
   */
  void Get(std::vector<Val>* vals) {
    Lock l(mu_);
    *vals = recv_; recv_.clear();
  }


  // implementation

  virtual void ProcessRequest(Message* request) {
    Lock l(mu_);
    const Task& task = request->task;
    int cmd = request->task.cmd();
    if (cmd == Slave<Val>::kPush) {
      CHECK_EQ(request->value.size(), 1);
      SArray<Val> recv(request->value[0]);
      if (recv_.size() == 0) recv_.resize(recv.size());
      CHECK_EQ(recv.size(), recv_.size());
      CHECK(task.has_op());
      for (size_t i = 0; i < recv.size(); ++i) {
        AssignOp(recv_[i], recv[i], task.op());
      }
    } else if (cmd == Slave<Val>::kPull) {
      Message* response = new Message();
      response->add_value(SArray<Val>(recv_));
      Reply(request, response);
    }
  }
 private:
  std::mutex mu_;
  std::vector<Val> recv_;
};

}  // namespace ps
