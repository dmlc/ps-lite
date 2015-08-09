#include "system/executor.h"
#include "ps/app.h"
#include <thread>
namespace ps {

Executor::Executor(Customer& obj) : obj_(obj), sys_(Postoffice::instance()) {
  my_node_ = Postoffice::instance().manager().van().my_node();
  // insert virtual group nodes
  for (auto id : GroupIDs()) {
    Node node;
    node.set_role(Node::GROUP);
    node.set_id(id);
    AddNode(node);
  }

  thread_ = new std::thread(&Executor::Run, this);
}

Executor::~Executor() {
  if (done_) return;
  done_ = true;

  // wake thread_
  { Lock l(msg_mu_); }
  dag_cond_.notify_all();

  CHECK_NOTNULL(thread_)->join();
  delete thread_;
}

bool Executor::CheckFinished(RemoteNode* rnode, int timestamp, bool sent) {
  CHECK(rnode);
  if (timestamp < 0) return true;
  auto& tracker = sent ? rnode->sent_req_tracker : rnode->recv_req_tracker;
  if (!rnode->alive || tracker.IsFinished(timestamp)) return true;
  if (rnode->node.role() == Node::GROUP) {
    for (auto r : rnode->group) {
      auto& r_tracker = sent ? r->sent_req_tracker : r->recv_req_tracker;
      if (r->alive && !r_tracker.IsFinished(timestamp)) return false;
      // well, set this group node as been finished
      r_tracker.Finish(timestamp);
    }
    return true;
  }
  return false;
}
int Executor::NumFinished(RemoteNode* rnode, int timestamp, bool sent) {
  CHECK(rnode);
  if (timestamp < 0 || !rnode->alive) return 0;
  auto& tracker = sent ? rnode->sent_req_tracker : rnode->recv_req_tracker;
  if (rnode->node.role() == Node::GROUP) {
    int fin = 0;
    for (auto r : rnode->group) {
      auto& r_tracker = sent ? r->sent_req_tracker : r->recv_req_tracker;
      if (r->alive && r_tracker.IsFinished(timestamp)) ++ fin;
    }
    return fin;
  } else {
    return tracker.IsFinished(timestamp);
  }
}

void Executor::WaitSentReq(int timestamp) {
  std::unique_lock<std::mutex> lk(node_mu_);
  VLOG(1) << obj_.id() << ": wait sent request " << timestamp;
  const NodeID& recver = sent_reqs_[timestamp].recver;
  CHECK(recver.size());
  auto rnode = GetRNode(recver);
  sent_req_cond_.wait(lk, [this, rnode, timestamp] {
      return CheckFinished(rnode, timestamp, true);
    });
}

void Executor::WaitRecvReq(int timestamp, const NodeID& sender) {
  std::unique_lock<std::mutex> lk(node_mu_);
  VLOG(1) << obj_.id() << ": wait request "
          << timestamp << " from " << sender;
  auto rnode = GetRNode(sender);
  recv_req_cond_.wait(lk, [this, rnode, timestamp] {
      return CheckFinished(rnode, timestamp, false);
    });
}

int Executor::QueryRecvReq(int timestamp, const NodeID& sender) {
  Lock l(node_mu_);
  return NumFinished(GetRNode(sender), timestamp, false);
}

void Executor::FinishRecvReq(int timestamp, const NodeID& sender) {
  std::unique_lock<std::mutex> lk(node_mu_);
  VLOG(1) << obj_.id() << ": finish request "
          << timestamp << " from " << sender;
  auto rnode = GetRNode(sender);
  rnode->recv_req_tracker.Finish(timestamp);
  if (rnode->node.role() == Node::GROUP) {
    for (auto r : rnode->group) {
      r->recv_req_tracker.Finish(timestamp);
    }
  }
  lk.unlock();
  recv_req_cond_.notify_all();

  { Lock lk(msg_mu_); }
  dag_cond_.notify_all();
}


int Executor::Submit(Message* msg) {
  CHECK(msg); CHECK(msg->recver.size());
  Lock l(node_mu_);

  // timestamp and other flags
  int ts = msg->task.has_time() ? msg->task.time() : time_;
  // CHECK_LT(time_, ts) << my_node_.id() << " has a newer timestamp";
  msg->task.set_time(ts);
  msg->task.set_request(true);
  msg->task.set_customer_id(obj_.id());

  // store something
  time_ = ts+1;
  auto& req_info = sent_reqs_[ts];
  req_info.recver = msg->recver;
  if (msg->callback) {
    req_info.callback = msg->callback;
  }

  // slice "msg"
  RemoteNode* rnode = GetRNode(msg->recver);
  std::vector<Message*> msgs(rnode->keys.size());
  for (auto& m : msgs) m = new Message(msg->task);
  obj_.Slice(*msg, rnode->keys, &msgs);
  CHECK_EQ(msgs.size(), rnode->group.size());

  // send them one by one
  for (size_t i = 0; i < msgs.size(); ++i) {
    RemoteNode* r = CHECK_NOTNULL(rnode->group[i]);
    Message* m = CHECK_NOTNULL(msgs[i]);
    if (!m->valid) {
      // do not sent, just mark it as done
      r->sent_req_tracker.Finish(ts);
      continue;
    }
    r->EncodeMessage(m);
    m->recver = r->node.id();
    sys_.Queue(m);
  }
  return ts;
}

void Executor::Reply(Message* request, Message* response) {
  const auto& req = CHECK_NOTNULL(request)->task;
  if (!req.request()) return;

  auto& res = CHECK_NOTNULL(response)->task;
  res.set_request(false);
  if (req.has_control()) res.set_control(req.control());
  if (req.has_customer_id()) res.set_customer_id(req.customer_id());
  if (req.has_cmd() && !res.has_cmd()) res.set_cmd(req.cmd());
  res.set_time(req.time());

  response->recver = request->sender;
  node_mu_.lock();
  GetRNode(response->recver)->EncodeMessage(response);
  node_mu_.unlock();
  sys_.Queue(response);

  request->replied = true;
}

bool Executor::PickActiveMsg() {
  std::unique_lock<std::mutex> lk(msg_mu_);
  // VLOG(1) << obj_.id() << ": try to pick a message";
  auto it = recv_msgs_.begin();
  while (it != recv_msgs_.end()) {
    bool process = true;
    Message* msg = *it; CHECK(msg); CHECK(!msg->task.control());

    // check if the remote node is still alive.
    Lock l(node_mu_);

    auto nit = nodes_.find(msg->sender);
    if (nit == nodes_.end()) {
      // it happens AddNode(msg->sender) is not executed yet, simply wait
      continue;
    }
    auto rnode = &nit->second;
    if (!rnode->alive) {
      LOG(WARNING) << my_node_.id() << ": rnode " << msg->sender <<
          " is not alive, ignore received message: " << msg->ShortDebugString();
      it = recv_msgs_.erase(it);
      delete msg;
      continue;
    }
    // check if double receiving
    bool req = msg->task.request();
    int ts = msg->task.time();
    if ((req && rnode->recv_req_tracker.IsFinished(ts)) ||
        (!req && rnode->sent_req_tracker.IsFinished(ts))) {
      LOG(WARNING) << my_node_.id() << ": received message twice. ignore: " <<
          msg->ShortDebugString();
      it = recv_msgs_.erase(it);
      delete msg;
      continue;
    }

    // check for dependency constraint. only needed for request message.
    if (req) {
      for (int i = 0; i < msg->task.wait_time_size(); ++i) {
        int wait_time = msg->task.wait_time(i);
        if (wait_time <= Message::kInvalidTime) continue;
        if (!rnode->recv_req_tracker.IsFinished(wait_time)) {
          process = false;
          ++ it;
          break;
        }
      }
    }
    if (process) {
      VLOG(1) << obj_.id() << ": pick the "
              << std::distance(recv_msgs_.begin(), it) << "-th messge in ["
              << recv_msgs_.size() << "] from " << msg->sender
              << ": " << msg->ShortDebugString();

      active_msg_ = std::shared_ptr<Message>(msg);
      recv_msgs_.erase(it);
      rnode->DecodeMessage(active_msg_.get());
      return true;
    }
  }

  // sleep until received a new message or another message been marked as
  // finished.
  VLOG(1) << obj_.id() << ": pick nothing. msg buffer size "
          << recv_msgs_.size();
  dag_cond_.wait(lk);
  return false;
}

void Executor::ProcessActiveMsg() {
  // ask the customer to process the picked message, and do post-processing
  bool req = active_msg_->task.request();
  int ts = active_msg_->task.time();
  if (req) {
    last_request_ = active_msg_;
    obj_.ProcessRequest(active_msg_.get());

    if (active_msg_->finished) {
      // if this message is marked as finished, then set the mark in tracker,
      // otherwise, the user application need to call `Customer::FinishRecvReq`
      // to set the mark
      FinishRecvReq(ts, active_msg_->sender);
      // reply an empty ACK message if necessary
      if (!active_msg_->replied) obj_.Reply(active_msg_.get());
    }
  } else {
    last_response_ = active_msg_;
    obj_.ProcessResponse(active_msg_.get());

    std::unique_lock<std::mutex> lk(node_mu_);
    // mark as finished
    auto rnode = GetRNode(active_msg_->sender);
    rnode->sent_req_tracker.Finish(ts);

    // check if the callback is ready to run
    auto it = sent_reqs_.find(ts);
    CHECK(it != sent_reqs_.end());
    const NodeID& orig_recver = it->second.recver;
    if (orig_recver != active_msg_->sender) {
      auto onode = GetRNode(orig_recver);
      if (onode->node.role() == Node::GROUP) {
        // the orginal recver is a group node, need to check whether repsonses
        // have been received from all nodes in this group
        for (auto r : onode->group) {
          if (r->alive && !r->sent_req_tracker.IsFinished(ts)) {
            return;
          }
        }
        onode->sent_req_tracker.Finish(ts);
      } else {
        // the orig_recver should be dead, and active_msgs_->sender is the
        // replacement of this dead node. Just run callback
      }
    }
    lk.unlock();

    if (it->second.callback) {
      // run the callback, and then empty it
      it->second.callback();
      it->second.callback = Message::Callback();
    }

    sent_req_cond_.notify_all();
  }
}

void Executor::Accept(Message* msg) {
  {
    Lock l(msg_mu_);
    recv_msgs_.push_back(msg);
    // VLOG(1) << obj_.id() << ": accept " << msg->ShortDebugString();
  }
  dag_cond_.notify_one();
}


void Executor::ReplaceNode(const Node& old_node, const Node& new_node) {
  // TODO
}

void Executor::RemoveNode(const Node& node) {
  VLOG(1) << obj_.id() << "remove node: " << node.ShortDebugString();
  auto id = node.id();
  if (nodes_.find(id) == nodes_.end()) return;
  auto r = GetRNode(id);
  for (const NodeID& gid : GroupIDs()) {
    nodes_[gid].RemoveGroupNode(r);
  }
  // do not remove r from nodes_
  r->alive = false;
}

void Executor::AddNode(const Node& node) {
  Lock l(node_mu_);
  VLOG(1) << obj_.id() << "add node: " << node.ShortDebugString();
  // add "node"
  if (node.id() == my_node_.id()) {
    my_node_ = node;
  }
  auto id = node.id();
  if (nodes_.find(id) != nodes_.end()) {
    // update
    auto r = GetRNode(id);
    CHECK(r->alive);
    r->node = node;
    for (const NodeID& gid : GroupIDs()) {
      nodes_[gid].RemoveGroupNode(r);
    }
  } else {
    // create
    nodes_[id].node = node;
  }

  // add "node" into group
  auto role = node.role();
  auto w = GetRNode(id);
  if (role != Node::GROUP) {
    nodes_[id].AddGroupNode(w); nodes_[kLiveGroup].AddGroupNode(w);
  }
  if (role == Node::SERVER) {
    nodes_[kServerGroup].AddGroupNode(w); nodes_[kCompGroup].AddGroupNode(w);
  }
  if (role == Node::WORKER) {
    nodes_[kWorkerGroup].AddGroupNode(w); nodes_[kCompGroup].AddGroupNode(w);
  }

  // update replica group and owner group if necessary
  if (node.role() != Node::SERVER || my_node_.role() != Node::SERVER) return;
  if (num_replicas_ <= 0) return;

  const auto& servers = nodes_[kServerGroup];
  for (int i = 0; i < (int)servers.group.size(); ++i) {
    auto s = servers.group[i];
    if (s->node.id() != my_node_.id()) continue;

    // the replica group is just before me
    auto& replicas = nodes_[kReplicaGroup];
    replicas.group.clear(); replicas.keys.clear();
    for (int j = std::max(i-num_replicas_, 0); j < i; ++ j) {
      replicas.group.push_back(servers.group[j]);
      replicas.keys.push_back(servers.keys[j]);
    }

    // the owner group is just after me
    auto& owners = nodes_[kOwnerGroup];
    owners.group.clear(); owners.keys.clear();
    for (int j = std::max(i-num_replicas_, 0); j < i; ++ j) {
      owners.group.push_back(servers.group[j]);
      owners.keys.push_back(servers.keys[j]);
    }
    break;
  }
  dag_cond_.notify_one();
}

} // namespace ps
