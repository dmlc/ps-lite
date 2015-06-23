/**
 * @file   ps-inl.h
 * @brief  Implementation of ps.h
 */
#pragma once
#include "ps.h"
namespace ps {

inline Filter* SyncOpts::AddFilter(Filter::Type type) {
  filters.push_back(Filter());
  filters.back().set_type(type);
  return &(filters.back());
}

template<typename Val>
Task KVWorker<Val>::GetTask(const SyncOpts& opts) {
  Task req;
  req.set_request(true);
  for (int l : opts.deps) req.add_wait_time(l);
  for (const auto& f : opts.filters) req.add_filter()->CopyFrom(f);
  if (opts.cmd != -1) req.set_cmd(opts.cmd);
  return req;
}

inline int NextCustomerID() {
  return Postoffice::instance().manager().NextCustomerID();
}

inline Range<Key> MyKeyRange() { return Range<Key>(MyNode().key()); }
}  // namespace ps
