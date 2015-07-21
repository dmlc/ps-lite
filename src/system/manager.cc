#include "system/manager.h"
#include "system/postoffice.h"
#include "ps/app.h"
namespace ps {

DECLARE_int32(num_servers);
DECLARE_int32(num_workers);
DECLARE_int32(num_replicas);
DECLARE_int32(report_interval);

DEFINE_int32(sync_timeout, 10, "connection timeout in sec.");

DEFINE_uint64(max_key, -1, "maximal global key");

Manager::Manager() {}
Manager::~Manager() {
  for (auto& it : customers_) {
    if (it.second.second) delete it.second.first;
  }
  delete node_assigner_;
  delete app_;
}

void Manager::Init(int argc, char *argv[]) {
  env_.Init(argv[0]);
  van_.Init();
  net_usage_.AddMyNode(van_.my_node());

  app_ = App::Create(argc, argv);
  CHECK(app_ != NULL) << ": failed to create app";

  if (IsScheduler()) {
    if (!FLAGS_logtostderr) {
      LOG(INFO) << "Staring system. Logging into " << FLAGS_log_dir
                << "/" << basename(argv[0]) <<".log.*";
    }
    if (FLAGS_max_key == (uint64)-1) {
      node_assigner_ = new NodeAssigner(FLAGS_num_servers, Range<Key>::All());
    } else {
      node_assigner_ = new NodeAssigner(FLAGS_num_servers, Range<Key>(0, FLAGS_max_key));
    }

    // add my node directly rather than sending a REGISTER_NODE request
    AddNode(van_.my_node());
  } else {
    // ask the scheduler to broadcast this node to others
    Task task = NewControlTask(Control::REGISTER_NODE);
    *task.mutable_ctrl()->add_node() = van_.my_node();
    SendTask(van_.scheduler(), task);
  }
}

void Manager::Run() {
  if (Timeout(FLAGS_sync_timeout, [this] { return inited_; })) {
    LOG(FATAL) << "Timeout ("
               << FLAGS_sync_timeout
               << " sec) to wait all other nodes initialized."
               << " See commmets for more information";
    // First check if all logs are normal. In default the logs are avaialable at
    // /tmp/, you can specify the log directory by "-log_dir your_log_dir". If
    // everything goes fine, then probably it's due to the cluster resource manager
    // (such as yarn) is slow to start the jobs. You can increase the sync
    // timeout value, such as waiting 1000 sec:  "-sync_timeout 1000"
  }

  CHECK_NOTNULL(app_)->Run();
}

void Manager::Stop() {
  if (IsScheduler()) {
    // wait all other nodes are ready for exit. cannot set a timeout here, some
    // apps such as cxxnet has an empty scheduler
    while(true) {
      usleep(100000);
      Lock lk(nodes_mu_);
      if (active_nodes_.size() == 0) break;
    }

    // broadcast the terminate signal to all workers and servers
    in_exit_ = true;
    Task task = NewControlTask(Control::EXIT);
    SendTask(kCompGroup, task);

    // wait others are done
    if (Timeout(FLAGS_sync_timeout, [this] {
          Lock lk(nodes_mu_);
          return alive_nodes_.size() == unknown_dead_nodes_ + 1;
        })) {
      LOG(ERROR) << "Timeout ("
                 << FLAGS_sync_timeout
                 << " sec) to wait all other nodes exited.";
    }

    // kill myself
    SendTask(van_.my_node(), task);
  } else {
    Task task = NewControlTask(Control::READY_TO_EXIT);
    SendTask(van_.scheduler(), task);

    // run as a daemon until received the termination message
    while (!done_) usleep(50000);
  }
  LOG(INFO) << van_.my_node().id() << " stopped";

  // exit(0);
}


bool Manager::Process(Message* msg) {
  const Task& task = msg->task;
  CHECK(task.control());
  if (!task.request()) return true;
  Task reply;
  reply.set_control(true);
  reply.set_request(false);
  reply.set_time(task.time());

  CHECK(task.has_ctrl());
  const auto& ctrl = task.ctrl();
  switch (ctrl.cmd()) {
    case Control::REGISTER_NODE: {
      CHECK(IsScheduler());
      CHECK_EQ(ctrl.node_size(), 1);
      Node sender = ctrl.node(0);
      // assign key range
      CHECK_NOTNULL(node_assigner_)->Assign(&sender);
      AddNode(sender);
      break;
    }
    case Control::READY_TO_RUN: {
      if (IsScheduler()) {
        Lock lk(nodes_mu_);
        active_nodes_.insert(msg->sender);
        if ((int)active_nodes_.size() >= FLAGS_num_workers + FLAGS_num_servers) {
          Task task = NewControlTask(Control::READY_TO_RUN);
          SendTask(kCompGroup, task);
          inited_ = true;
        }
      } else {
        inited_ = true;
      }
      break;
    }
    case Control::READY_TO_EXIT: {
      CHECK(IsScheduler());
      Lock lk(nodes_mu_);
      active_nodes_.erase(msg->sender);
      break;
    }
    case Control::ADD_NODE: {
      for (int i = 0; i < ctrl.node_size(); ++i) {
        AddNode(ctrl.node(i));
      } break;
    }
    case Control::REMOVE_NODE: {
      for (int i = 0; i < ctrl.node_size(); ++i) {
        RemoveNode(ctrl.node(i).id());
      } break;
    }
    case Control::EXIT: {
      done_ = true;
      return false;
    }
  }
  SendTask(msg->sender, reply);
  return true;
}

void Manager::AddNode(const Node& node) {
  // add to system
  nodes_mu_.lock();
  if (nodes_.find(node.id()) == nodes_.end()) {
    CHECK(van_.Connect(node));
    if (node.role() == Node::WORKER) ++ num_workers_;
    if (node.role() == Node::SERVER) ++ num_servers_;
  } else {
    LOG(ERROR) << "addnode: " << node.id() << " exits.";
  }
  nodes_[node.id()] = node;
  alive_nodes_.insert(node.id());
  nodes_mu_.unlock();

  net_usage_.AddNode(node);

  // add to app
  for (auto& it : customers_) {
    it.second.first->executor()->AddNode(node);
  }

  if (num_workers_ >= FLAGS_num_workers &&
      num_servers_ >= FLAGS_num_servers) {
    // this happens only if more nodes than expected are added. it may due to
    // node failure. it's safe to have core dump here since i didn't test it too much.
    CHECK(!inited_) << "didn't test yet, see comments.";

    if (IsScheduler()) {
      // broadcast all nodes info
      Task add_node = NewControlTask(Control::ADD_NODE);
      for (const auto& it : nodes_) {
        *add_node.mutable_ctrl()->add_node() = it.second;
      }
      SendTask(kCompGroup, add_node);
    } else {
      // tell the scheduler that this node is already to run
      Task task = NewControlTask(Control::READY_TO_RUN);
      SendTask(van_.scheduler(), task);
    }
  }

  // if (node.id() == van_.my_node().id()) is_my_node_inited_ = true;
  VLOG(1) << "add node: " << node.ShortDebugString();
}


void Manager::RemoveNode(const NodeID& node_id) {
  nodes_mu_.lock();
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return;
  Node node = it->second;
  // van_.disconnect(node);
  if (node.role() == Node::WORKER) -- num_workers_;
  if (node.role() == Node::SERVER) -- num_servers_;
  active_nodes_.erase(node_id);
  nodes_.erase(it);
  nodes_mu_.unlock();

  // TODO use *replace* for server
  // TODO remove from customers
  // remove from customers
  // for (auto& it : customers_) {
  //   it.second.first->exec().removeNode(node);
  // }

  // remove from app
  for (auto& it : customers_) {
    it.second.first->executor()->RemoveNode(node);
  }

  // broadcast
  if (IsScheduler() && node.id() != van_.my_node().id()) {
    for (const auto& it : nodes_) {
      if (it.first == van_.my_node().id() || it.first == node.id()) {
        continue;
      }
      Task remove_node = NewControlTask(Control::REMOVE_NODE);
      *remove_node.mutable_ctrl()->add_node() = node;
      SendTask(it.second, remove_node);
    }
  }

  if (!IsScheduler() && node.role() == Node::SERVER) {
    LOG(WARNING) << "server node " << node.id() << " died, exit";
    ForceExit();
  }
  VLOG(1) << "remove node: " << node.ShortDebugString();
}

void Manager::NodeDisconnected(const NodeID node_id) {
  nodes_mu_.lock();
  auto it = alive_nodes_.find(node_id);
  if (it != alive_nodes_.end()) {
    alive_nodes_.erase(it);
  } else {
    ++ unknown_dead_nodes_;
  }
  nodes_mu_.unlock();

  // alreay in shutting down?
  if (in_exit_) return;

  // call handlers
  for (const auto& h : node_failure_handlers_) h(node_id);

  if (IsScheduler()) {
    LOG(INFO) << node_id << " is disconnected";
    RemoveNode(node_id);
  } else {
    LOG(WARNING) << "the scheduler is died, exit";
    ForceExit();
  }
}

Task Manager::NewControlTask(Control::Command cmd) {
  Task task;
  task.set_control(true);
  task.set_request(true);
  task.set_time(IsScheduler() ? time_ * 2 : time_ * 2 + 1);
  ++ time_;
  task.mutable_ctrl()->set_cmd(cmd);
  return task;
}

void Manager::SendTask(const NodeID& recver, const Task& task) {
  if (recver == kCompGroup) {
    // a hack..
    for (const auto& it : nodes_) {
      auto role = it.second.role();
      if (role == Node::WORKER || role == Node::SERVER) {
        Message* msg = new Message(task);
        msg->recver = it.first;
        Postoffice::instance().Queue(msg);
      }
    }
  } else {
    Message* msg = new Message(task);
    msg->recver = recver;
    Postoffice::instance().Queue(msg);
  }
}

bool Manager::Timeout(int sec, const std::function<bool()>& pred) {
  int itv = 10000;
  int i =  sec * 1000000 / itv;
  while (i > 0 && !pred()) {
    usleep(itv); --i;
  }
  if (i == 0 && !pred()) return true;
  return false;
}

// customers
Customer* Manager::customer(int id) {
  auto it = customers_.find(id);
  if (it == customers_.end()) CHECK(false) << id << " does not exist";
  return it->second.first;
}

void Manager::AddCustomer(Customer* obj) {
  CHECK_EQ(customers_.count(obj->id()), (size_t)0)
      << obj->id() << " already exists";
  customers_[obj->id()] = std::make_pair(obj, false);
  nodes_mu_.lock();
  for (const auto& it : nodes_) {
    obj->executor()->AddNode(it.second);
  }
  nodes_mu_.unlock();
}


void Manager::TransferCustomer(Customer* obj) {
  CHECK_EQ(customers_.count(obj->id()), (size_t)1)
      << obj->id() << " dose not exist";
  customers_[obj->id()].second = true;
}

void Manager::RemoveCustomer(int id) {
  auto it = customers_.find(id);
  // only assign it to NULL, because the call chain could be:
  // ~CustomerManager() -> ~Customer() -> remove(int id)
  if (it != customers_.end()) it->second.first = NULL;
}

int Manager::NextCustomerID() {
  int id = 0;
  for (const auto& it : customers_) id = std::min(id, it.second.first->id());
  return id - 1;
}

} // namespace ps
