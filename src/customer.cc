/**
 *  Copyright (c) 2015 by Contributors
 */
#include "ps/internal/customer.h"

#include <atomic>
#include <chrono>
#include <fstream>
#include <list>
#include <map>
#include <set>

#include "ps/internal/postoffice.h"
#include "ps/internal/threadsafe_queue.h"

namespace ps {
const int Node::kEmpty = std::numeric_limits<short>::max();
const int Meta::kEmpty = std::numeric_limits<short>::max();

Customer::Customer(int app_id, int customer_id,
                   const Customer::RecvHandle& recv_handle,
                   Postoffice* postoffice)
    : app_id_(app_id),
      customer_id_(customer_id),
      recv_handle_(recv_handle),
      postoffice_(postoffice) {
  postoffice_->AddCustomer(this);
  recv_thread_ =
      std::unique_ptr<std::thread>(new std::thread(&Customer::Receiving, this));
}

Customer::~Customer() {
  postoffice_->RemoveCustomer(this);
  Message msg;
  msg.meta.control.cmd = Control::TERMINATE;
  recv_queue_.Push(msg);
  recv_thread_->join();
}

int Customer::NewRequest(int recver) {
  CHECK(recver == kServerGroup) << recver;
  std::lock_guard<std::mutex> lk(tracker_mu_);
  // for push/pull requests, the worker only communication with one instance
  // from each server instance group
  int num = postoffice_->GetNodeIDs(recver).size() / postoffice_->group_size();
  tracker_.push_back(std::make_pair(num, 0));
  return tracker_.size() - 1;
}

void Customer::WaitRequest(int timestamp) {
  std::unique_lock<std::mutex> lk(tracker_mu_);
  tracker_cond_.wait(lk, [this, timestamp] {
    return tracker_[timestamp].first == tracker_[timestamp].second;
  });
}

int Customer::NumResponse(int timestamp) {
  std::lock_guard<std::mutex> lk(tracker_mu_);
  return tracker_[timestamp].second;
}

void Customer::AddResponse(int timestamp, int num) {
  std::lock_guard<std::mutex> lk(tracker_mu_);
  tracker_[timestamp].second += num;
}

void Customer::Receiving() {
  while (true) {
    Message recv;
    recv_queue_.WaitAndPop(&recv);
    if (!recv.meta.control.empty() &&
        recv.meta.control.cmd == Control::TERMINATE) {
      break;
    }
    recv_handle_(recv);
    if (!recv.meta.request) {
      std::lock_guard<std::mutex> lk(tracker_mu_);
      tracker_[recv.meta.timestamp].second++;
      tracker_cond_.notify_all();
    }
  }
}
}  // namespace ps
