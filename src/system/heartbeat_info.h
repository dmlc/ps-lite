#pragma once

#include "proto/heartbeat.pb.h"
#include "base/resource_usage.h"
#include "base/common.h"

namespace ps {
class HeartbeatInfo {
public:
  enum class TimerType : unsigned char {
    BUSY = 0,
    NUM
  };

public:
  HeartbeatInfo();
  ~HeartbeatInfo();
  HeartbeatInfo(const HeartbeatInfo& other) = delete;
  HeartbeatInfo& operator= (const HeartbeatInfo& rhs) = delete;

  HeartbeatReport get();

  // set network interface which is under use
  //   such as "eth0"
  // set hostname
  void init(const string& interface, const string& hostname);

  void startTimer(const HeartbeatInfo::TimerType type);
  void stopTimer(const HeartbeatInfo::TimerType type);

  // TODO need lock?
  void increaseInBytes(const size_t delta) { Lock l(mu_); in_bytes_ += delta; }
  void increaseOutBytes(const size_t delta) { Lock l(mu_); out_bytes_ += delta; }

private:
  std::vector<MilliTimer> timers_;
  MilliTimer total_timer_;

  size_t in_bytes_;
  size_t out_bytes_;

  string interface_;
  string hostname_;

  // snapshot of performance counters
  struct Snapshot {
    uint64 process_user;
    uint64 process_sys;
    uint64 host_user;
    uint64 host_sys;
    uint64 host_cpu;

    uint64 host_in_bytes;
    uint64 host_out_bytes;

    Snapshot() :
      process_user(0),
      process_sys(0),
      host_user(0),
      host_sys(0),
      host_cpu(0),
      host_in_bytes(0),
      host_out_bytes(0) {
        // do nothing
    }

    string shortDebugString() {
      std::stringstream ss;
      ss << "{";
      ss << "process_user: " << process_user << ", ";
      ss << "process_sys: " << process_sys << ", ";
      ss << "host_user: " << host_user << ", ";
      ss << "host_sys: " << host_sys << ", ";
      ss << "host_cpu: " << host_cpu << ", ";
      ss << "host_in_bytes: " << host_in_bytes << ", ";
      ss << "host_out_bytes: " << host_out_bytes;
      ss << "}";

      return ss.str();
    }
  }; // struct Snapshot

  HeartbeatInfo::Snapshot last_;
  HeartbeatInfo::Snapshot dump();

  std::mutex mu_;
  size_t cpu_core_number_;
}; // class Heartbeatinfo
}; // namespace ps
