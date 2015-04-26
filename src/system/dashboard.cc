#include "system/dashboard.h"
#include <iomanip>

namespace ps {

bool NodeIDCmp::operator()(const NodeID& a, const NodeID& b) {
  string a_primary, a_secondary;
  splitNodeID(a, a_primary, a_secondary);
  string b_primary, b_secondary;
  splitNodeID(b, b_primary, b_secondary);

  // compare
  if (a_primary != b_primary) {
    return a_primary < b_primary;
  } else {
    return std::stoul(a_secondary) < std::stoul(b_secondary);
  }
}

void NodeIDCmp::splitNodeID(const NodeID& in, string& primary, string& secondary) {
  size_t tailing_alpha_idx = in.find_last_not_of("0123456789");
  if (std::string::npos == tailing_alpha_idx) {
    primary = in;
    secondary = "";
  } else {
    primary = in.substr(0, tailing_alpha_idx + 1);
    secondary = in.substr(tailing_alpha_idx + 1);
  }
  return;
}

void Dashboard::addTask(const NodeID& node, int task_id) {
  Lock l(mu_);
  data_[node].set_task_id(task_id);
}

void Dashboard::addReport(const NodeID& node, const string& report) {
  HeartbeatReport hb; CHECK(hb.ParseFromString(report));
  {
    Lock l(mu_);
    auto& rp = data_[node];
    hb.set_task_id(rp.task_id());
    rp = hb;
  }
}

string Dashboard::title() {
  const size_t WIDTH = 10;
  std::time_t now_time = std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::now());
  string time_str = ctime(&now_time);
  time_str.resize(time_str.size() - 1);
  std::stringstream ss;
  ss << std::setiosflags(std::ios::left) <<
      std::setw(WIDTH * 2) << std::setfill('=') << "" << " Dashboard " <<
      time_str + " " << std::setw(WIDTH * 2) << std::setfill('=') << "" << "\n";
  ss << std::setfill(' ') <<
      std::setw(WIDTH) << "Node" <<
      std::setw(WIDTH) << "Task" <<
      std::setw(WIDTH) << "MyCPU(%)" <<
      std::setw(WIDTH) << "MyRSS(M)" <<
      std::setw(WIDTH) << "MyVir(M)" <<
      std::setw(WIDTH) << "BusyTime" <<
      std::setw(WIDTH) << "InMB" <<
      std::setw(WIDTH) << "OutMB" <<
      std::setw(WIDTH) << "HostCPU" <<
      std::setw(WIDTH) << "HostUseGB" <<
      std::setw(WIDTH) << "HostInBW" <<
      std::setw(WIDTH) << "HostOutBW" <<
      std::setw(WIDTH * 2) << "HostName";
  return ss.str();
}

string Dashboard::report() {
  mu_.lock();
  auto copy = data_;
  mu_.unlock();
  std::stringstream ss;
  ss << title() << "\n";
  for (const auto& it : copy) {
    ss << report(it.first, it.second) << "\n";
  }
  return ss.str();
}

string Dashboard::report(const NodeID& node, const HeartbeatReport& report) {
  std::stringstream ss;
  const size_t WIDTH = 10;

  std::stringstream busy_time_with_ratio;
  busy_time_with_ratio << report.busy_time_milli() <<
      "(" << static_cast<uint32>(
          100 * (static_cast<float>(report.busy_time_milli()) / report.total_time_milli())) <<
      "%)";

  std::stringstream net_in_mb_with_speed;
  net_in_mb_with_speed << report.net_in_mb() <<
      "(" << static_cast<uint32>(report.net_in_mb() / (report.total_time_milli() / 1e3)) <<
      ")";

  std::stringstream net_out_mb_with_speed;
  net_out_mb_with_speed << report.net_out_mb() <<
      "(" << static_cast<uint32>(report.net_out_mb() / (report.total_time_milli() / 1e3)) <<
      ")";

  std::stringstream host_memory_usage;
  host_memory_usage << report.host_in_use_gb() << "(" <<
      report.host_in_use_percentage() << "%)";

  ss << std::setiosflags(std::ios::left) <<
      std::setw(WIDTH) << node <<
      std::setw(WIDTH) << report.task_id() <<
      std::setw(WIDTH) << report.process_cpu_usage() <<
      std::setw(WIDTH) << report.process_rss_mb() <<
      std::setw(WIDTH) << report.process_virt_mb() <<
      std::setw(WIDTH) << busy_time_with_ratio.str() <<
      std::setw(WIDTH) << net_in_mb_with_speed.str() <<
      std::setw(WIDTH) << net_out_mb_with_speed.str() <<
      std::setw(WIDTH) << report.host_cpu_usage() <<
      std::setw(WIDTH) << host_memory_usage.str() <<
      std::setw(WIDTH) << (
          report.host_net_in_bw() < 1024 ? std::to_string(report.host_net_in_bw()) : "INIT") <<
      std::setw(WIDTH) << (
          report.host_net_out_bw() < 1024 ? std::to_string(report.host_net_out_bw()) : "INIT") <<
      std::setw(WIDTH * 2) << report.hostname();

  return ss.str();
}

} // namespace ps
