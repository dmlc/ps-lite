/**
 * @file   monitor.h
 * @brief  A distributed monitor
 *
 */
#pragma once
#include "ps/app.h"
namespace ps {

/**
 * @brief The master of the monitor, which collects reports from slavers and
 * display the progress
 *
 * @tparam Progress A proto buffer class
 */
template <typename Progress>
class MonitorMaster : public Customer {
 public:
  MonitorMaster(int id = NextCustomerID()) : Customer(id) {}

  typedef std::function<void(
      double time, std::unordered_map<NodeID, Progress>*)> Printer;
  /**
   * @brief set the printer
   *
   * @param time_interval in sec
   * @param printer
   */
  void set_printer(double time_interval, Printer printer) {
    timer_.start();
    printer_ = printer;
    interval_ = time_interval;
  }

  typedef std::function<void(const Progress& src, Progress* dst)> Merger;
  /**
   * @brief set the merger
   *
   * @param merger merges two reports
   */
  void set_merger(Merger merger) {
    merger_ = merger;
  }

  virtual void ProcessRequest(Message* request) {
    NodeID sender = request->sender;
    Progress prog;
    CHECK(prog.ParseFromString(request->task.msg()));
    if (merger_) {
      merger_(prog, &progress_[sender]);
    } else {
      progress_[sender] = prog;
    }

    double time = timer_.stop();
    if (time > interval_ && printer_) {
      total_time_ += time;
      printer_(total_time_, &progress_);
      timer_.restart();
    } else {
      timer_.start();
    }
  }
 private:
  std::unordered_map<NodeID, Progress> progress_;
  double interval_;
  Timer timer_;
  double total_time_ = 0;
  Merger merger_;
  Printer printer_;
};

/**
 * @brief A slave monitor, which report to the master monitor
 *
 * @tparam Progress a proto class
 */
template <typename Progress>
class MonitorSlaver : public Customer {
 public:
  MonitorSlaver(const NodeID& master, int id = NextCustomerID())
      : Customer(id), master_(master) { }
  virtual ~MonitorSlaver() { }

  /**
   * @brief Sends a report to the master
   *
   * @param prog
   */
  void Report(const Progress& prog) {
    string str; CHECK(prog.SerializeToString(&str));
    Task report; report.set_msg(str);
    Submit(report, master_);
  }
 protected:
  NodeID master_;
};

} // namespace ps
