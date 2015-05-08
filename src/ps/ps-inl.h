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

inline void SyncOpts::GetTask(Task* req) const {
  req->Clear();
  req->set_request(true);
  for (int l : deps) req->add_wait_time(l);
  for (const auto& f : filters) req->add_filter()->CopyFrom(f);
}

}  // namespace ps
