#include <chrono>
#include "system/heartbeat_info.h"

namespace ps {
HeartbeatInfo::HeartbeatInfo() :
  timers_(static_cast<size_t>(HeartbeatInfo::TimerType::NUM)),
  in_bytes_(0),
  out_bytes_(0) {
}

HeartbeatInfo::~HeartbeatInfo() {
  // do nothing
}

void HeartbeatInfo::startTimer(const HeartbeatInfo::TimerType type) {
  Lock l(mu_);
  timers_.at(static_cast<size_t>(type)).start();
}

void HeartbeatInfo::stopTimer(const HeartbeatInfo::TimerType type) {
  Lock l(mu_);
  timers_.at(static_cast<size_t>(type)).stop();
}

HeartbeatInfo::Snapshot HeartbeatInfo::dump() {
  HeartbeatInfo::Snapshot ret;

  // process cpu
  std::ifstream my_cpu_stat("/proc/self/stat", std::ifstream::in);
  CHECK(my_cpu_stat) << "open /proc/self/stat failed [" << strerror(errno) << "]";
  string pid, comm, state, ppid, pgrp, session, tty_nr;
  string tpgid, flags, minflt, cminflt, majflt, cmajflt;
  string utime, stime, cutime, cstime, priority, nice;
  my_cpu_stat >> pid >> comm >> state >> ppid >> pgrp >> session >>
    tty_nr >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt >>
    utime >> stime >> cutime >> cstime >> priority >> nice;
  my_cpu_stat.close();
  ret.process_user = std::stoull(utime);
  ret.process_sys = std::stoull(stime);

  // host cpu
  std::ifstream host_cpu_stat("/proc/stat", std::ifstream::in);
  CHECK(host_cpu_stat) << "open /proc/stat failed [" << strerror(errno) << "]";
  string label, host_user, host_nice, host_sys, host_idle, host_iowait;
  host_cpu_stat >> label >> host_user >> host_nice >> host_sys >> host_idle >>
    host_iowait;
  host_cpu_stat.close();
  ret.host_user = std::stoull(host_user);
  ret.host_sys = std::stoull(host_sys);
  ret.host_cpu = std::stoull(host_user) + std::stoull(host_nice) +
    std::stoull(host_sys) + std::stoull(host_idle) + std::stoull(host_iowait);

  // host network bandwidth usage
  if (!interface_.empty()) {
    std::ifstream host_net_dev_stat("/proc/net/dev", std::ifstream::in);
    CHECK(host_net_dev_stat) << "open /proc/net/dev failed [" << strerror(errno) << "]";

    // find interface
    string line;
    bool interface_found = false;
    while (std::getline(host_net_dev_stat, line)) {
      if (std::string::npos != line.find(interface_)) {
        interface_found = true;
        break;
      }
    }
    CHECK(interface_found) << "I cannot find interface[" << interface_ <<
      "] in /proc/net/dev";

    // read counters
    string face, r_bytes, r_packets, r_errs, r_drop, r_fifo, r_frame;
      string r_compressed, r_multicast, t_bytes, t_packets;
    std::stringstream ss(line);
    ss >> face >> r_bytes >> r_packets >> r_errs >> r_drop >> r_fifo >>
      r_frame >> r_compressed >> r_multicast >> t_bytes >> t_packets;
    host_net_dev_stat.close();

    ret.host_in_bytes = std::stoull(r_bytes);
    ret.host_out_bytes = std::stoull(t_bytes);
  }

  return ret;
}

HeartbeatReport HeartbeatInfo::get() {
  Lock l(mu_);
  HeartbeatReport report;
  HeartbeatInfo::Snapshot snapshot_now = dump();

  // interval between invocations of get()
  double total_milli = total_timer_.stop();
  if (total_milli < 1.0) {
    total_milli = 1.0;
  }

  report.set_hostname(hostname_);

  report.set_seconds_since_epoch(std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::system_clock::now().time_since_epoch()).count());

  report.set_total_time_milli(total_milli);
  report.set_busy_time_milli(
    timers_.at(static_cast<size_t>(HeartbeatInfo::TimerType::BUSY)).get());

  report.set_net_in_mb(in_bytes_ / 1024 / 1024);
  in_bytes_ = 0;
  report.set_net_out_mb(out_bytes_ / 1024 / 1024);
  out_bytes_ = 0;

  uint32 process_now = snapshot_now.process_user + snapshot_now.process_sys;
  uint32 process_last = last_.process_user + last_.process_sys;
  report.set_process_cpu_usage(cpu_core_number_ *
    100 * static_cast<float>(process_now - process_last) /
    (snapshot_now.host_cpu - last_.host_cpu));

  uint32 host_now = snapshot_now.host_user + snapshot_now.host_sys;
  uint32 host_last = last_.host_user + last_.host_sys;
  report.set_host_cpu_usage(cpu_core_number_ *
    100 * static_cast<float>(host_now - host_last) /
    (snapshot_now.host_cpu - last_.host_cpu));

  report.set_process_rss_mb(ResUsage::myPhyMem());
  report.set_process_virt_mb(ResUsage::myVirMem());
  report.set_host_in_use_gb(ResUsage::hostInUseMem() / 1024);
  report.set_host_in_use_percentage(
    100 * ResUsage::hostInUseMem() / ResUsage::hostTotalMem());

  report.set_host_net_in_bw(static_cast<uint32>(
    (snapshot_now.host_in_bytes - last_.host_in_bytes) / (total_milli / 1e3) / 1024 / 1024));
  report.set_host_net_out_bw(static_cast<uint32>(
    (snapshot_now.host_out_bytes - last_.host_out_bytes) / (total_milli / 1e3) / 1024 / 1024));

  // reset all timers
  for (auto& timer : timers_) {
    timer.reset();
    timer.start();
  }
  total_timer_.reset();
  total_timer_.start();

  last_ = snapshot_now;
  return report;
}

void HeartbeatInfo::init(const string& interface, const string& hostname) {
  interface_ = interface;
  hostname_ = hostname;

  // get cpu core number
  char buffer[1024];
  FILE *fp_pipe = popen("grep 'processor' /proc/cpuinfo | wc -l", "r");
  CHECK(nullptr != fp_pipe);
  CHECK(nullptr != fgets(buffer, sizeof(buffer), fp_pipe));
  string core_str(buffer);
  core_str.resize(core_str.size() - 1);
  cpu_core_number_ = std::stoul(core_str);
  pclose(fp_pipe);

  // initialize internal status
  get();
}

}; // namespace ps
