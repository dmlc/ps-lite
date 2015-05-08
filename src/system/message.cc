#include "system/message.h"
namespace ps {

// Message::Message(const NodeID& dest, int time, int wait_time)
//     : recver(dest) {
//   task.set_time(time);
//   if (wait_time != kInvalidTime) task.add_wait_time(wait_time);
// }

Filter* Message::add_filter(Filter::Type type) {
  auto ptr = task.add_filter();
  ptr->set_type(type);
  return ptr;
}

size_t Message::mem_size() {
  size_t nbytes = task.SpaceUsed() + key.MemSize();
  for (const auto& v : value) nbytes += v.MemSize();
  return nbytes;
}

std::string Message::ShortDebugString() const {
  std::stringstream ss;
  if (key.size()) ss << "key [" << key.size() << "] ";
  if (value.size()) {
    ss << "value [";
    for (size_t i = 0; i < value.size(); ++i) {
      ss << value[i].size();
      if (i < value.size() - 1) ss << ",";
    }
    ss << "] ";
  }
  auto t = task; t.clear_msg(); ss << t.ShortDebugString();
  return ss.str();
}

std::string Message::DebugString() const {
  std::stringstream ss;
  ss << "[message]: " << sender << "=>" << recver
     << "[task]:" << task.ShortDebugString()
     << "\n[key]:" << key.size()
     << "\n[" << value.size() << " value]: ";
  for (const auto& x: value)
    ss << x.size() << " ";
  return ss.str();
}


} // namespace ps
