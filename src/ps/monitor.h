/**
 * @file   monitor.h
 * @brief  A distributed monitor
 */
#pragma once
#include "ps.h"
#include "ps/app.h"
namespace ps {

/**
 * @brief The master of the monitor, which collects reports from slavers and
 *
 * @tparam Progress which should implement
 * \code
   void Merge(const Progress&);
   void Parse(const std::string&);
   void Clear();
   \endcode
 */
template <typename Progress>
class MonitorMaster : public Customer {
 public:
  MonitorMaster(int id = NextCustomerID()) : Customer(id) {}

  // Get the newest progress
  void Get(Progress *prog) { Lock lk(mu_); prog->Merge(progress_); }

  void Clear() { Lock lk(mu_); progress_.Clear(); }

  // implement system required functions
  virtual void ProcessRequest(Message* request) {
    Lock lk(mu_);
    progress_.Merge(request->task.msg());
  }
 private:
  std::mutex mu_;
  Progress progress_;
};

/**
 * @brief A slave monitor, which report to the master monitor
 *
 * @tparam Progress which should implement
 \code
 void Serialize(std::string *);
 \endcode
 */
template <typename Progress>
class MonitorSlaver : public Customer {
 public:
  MonitorSlaver(const NodeID& master = SchedulerID(),
                int id = NextCustomerID())
      : Customer(id), master_(master) { }
  virtual ~MonitorSlaver() { }

  /**
   * @brief Sends a report to the master
   */
  void Report(const Progress& prog) {
    string str; prog.Serialize(&str);
    Task report; report.set_msg(str);
    Submit(report, master_);
  }
 protected:
  NodeID master_;
};
}
