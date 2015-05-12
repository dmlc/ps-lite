#include "system/remote_node.h"
#include "ps/shared_array.h"
#include "ps/app.h"
namespace ps {

IFilter* RemoteNode::FindFilterOrCreate(const Filter& conf) {
  int id = conf.type();
  auto it = filters.find(id);
  if (it == filters.end()) {
    filters[id] = IFilter::create(conf);
    it = filters.find(id);
  }
  return it->second;
}

void RemoteNode::EncodeMessage(Message* msg) {
  const auto& tk = msg->task;
  for (int i = 0; i < tk.filter_size(); ++i) {
    FindFilterOrCreate(tk.filter(i))->Encode(msg);
  }
}
void RemoteNode::DecodeMessage(Message* msg) {
  const auto& tk = msg->task;
  // a reverse order comparing to encode
  for (int i = tk.filter_size()-1; i >= 0; --i) {
    FindFilterOrCreate(tk.filter(i))->Decode(msg);
  }
}

void RemoteNode::AddGroupNode(RemoteNode* rnode) {
  CHECK_NOTNULL(rnode);
  // insert s into sub_nodes such as sub_nodes is still ordered
  size_t pos = 0;
  Range<Key> kr(rnode->node.key());
  while (pos < group.size()) {
    if (kr.InLeft(Range<Key>(group[pos]->node.key()))) {
      break;
    }
    ++ pos;
  }
  group.insert(group.begin() + pos, rnode);
  keys.insert(keys.begin() + pos, kr);
}

void RemoteNode::RemoveGroupNode(RemoteNode* rnode) {
  size_t n = group.size();
  CHECK_EQ(n, keys.size());
  for (size_t i = 0; i < n; ++i) {
    if (group[i] == rnode) {
      group.erase(group.begin() + i);
      keys.erase(keys.begin() + i);
      return;
    }
  }
}

} // namespace ps
