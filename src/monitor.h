/**
 * @file   monitor.h
 * @brief  A distributed monitor
 *
 */
#pragma once
#include "ps.h"
#include "system/customer.h"
namespace ps {

/**
 * @brief The master of the monitor, which collects reports from slavers and
 *
 * @tparam Progress which should implement
 * \code
   void Merge(const Progress&);
   void Parse(const std::string&);
   \endcode
 */
template <typename Progress>
class MonitorMaster : public Customer {
 public:
  MonitorMaster(int id = NextCustomerID()) : Customer(id) {}

  // Get the newest progress
  const Progress& GetProgress() {

  }

  double GetElapsedTime() {

  }

  // implement system required functions
  virtual void ProcessRequest(Message* request) {
    // NodeID sender = request->sender;
    // Progress prog;
    // CHECK(prog.ParseFromString(request->task.msg()));
    // if (merger_) {
    //   merger_(prog, &progress_[sender]);
    // } else {
    //   progress_[sender] = prog;
    // }

    // double time = timer_.stop();
    // if (time > interval_ && printer_) {
    //   total_time_ += time;
    //   printer_(total_time_, &progress_);
    //   timer_.restart();
    // } else {
    //   timer_.start();
    // }
  }
 private:
  std::unordered_map<NodeID, Progress> progress_;
  double interval_;
  Timer timer_;
  double total_time_ = 0;
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
