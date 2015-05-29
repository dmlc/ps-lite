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

  // Get the merged progress on channel chl, return the number of unique senders
  int Get(int chl, Progress *prog) {
    Lock lk(mu_); prog->Merge(prog_[chl]); return nodes_[chl].size();
  }

  void Clear(int chl) { Lock lk(mu_); prog_[chl].Clear(); nodes_[chl].clear(); }

  // implement system required functions
  virtual void ProcessRequest(Message* request) {
    Lock lk(mu_);
    Progress p; p.Parse(request->task.msg());
    int chl = request->task.key_channel();
    prog_[chl].Merge(p);
    nodes_[chl].insert(request->sender);
  }
 private:
  std::mutex mu_;
  std::unordered_map<int, Progress> prog_;
  std::unordered_map<int, std::unordered_set<std::string>> nodes_;
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
   * @brief Sends a report to the master, reports on the same channel are merged
  on the master
   */
  void Report(int chl, const Progress& prog) {
    string str; prog.Serialize(&str);
    Task report; report.set_msg(str);
    report.set_key_channel(chl);
    Submit(report, master_);
  }
 protected:
  NodeID master_;
};
}
