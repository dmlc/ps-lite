#include "system/manager.h"
#include "system/postoffice.h"
#include "ps/app.h"
namespace ps {

DECLARE_int32(num_servers);
DECLARE_int32(num_workers);
DECLARE_int32(num_replicas);
DECLARE_int32(report_interval);

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

    node_assigner_ = new NodeAssigner(FLAGS_num_servers);

    // add my node into app_
    AddNode(van_.my_node());
  } else {
    // ask the scheduler to broadcast this node to others
    Task task = NewControlTask(Control::REGISTER_NODE);
    *task.mutable_ctrl()->add_node() = van_.my_node();
    SendTask(van_.scheduler(), task);
  }
}

void Manager::Run() {
  // app_ is created by postoffice::recv_thread (except for the scheduler),
  // while run() is called by the main thread, so here should be a thread
  // synchronization.

  // wait my node info is updated
  while (!is_my_node_inited_) usleep(500);
  if (van_.my_node().role() == Node::WORKER) {
    WaitServersReady();
    usleep(1000);  // sleep a while to let all servers has been connected to me
  }
  VLOG(1) << "run app..";
  CHECK_NOTNULL(app_)->Run();
}

void Manager::Stop() {
  if (IsScheduler()) {
    // wait all other nodes are ready for exit for 5 seceonds
    int itv = 100000;
    int i = 5 * 1000000 / itv;
    while (i > 0 && num_active_nodes_ > 1) {
      usleep(itv); --i;
    }
    if (num_active_nodes_ > 1) {
      LOG(WARNING) << "scheduler: timeout (5sec) to wait the remain "
                   << num_active_nodes_ - 1 << " nodes";
    }
    // broadcast the terminate signal
    in_exit_ = true;
    for (const auto& it : nodes_) {
      Task task = NewControlTask(Control::EXIT);
      SendTask(it.second, task);
    }
    // sleep .5 sec to let other nodes exit
    usleep(500000);
    LOG(INFO) << "System stopped";
  } else {
    Task task = NewControlTask(Control::READY_TO_EXIT);
    SendTask(van_.scheduler(), task);

    // run as a daemon until received the termination message
    while (!done_) usleep(5000);
  }
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
      CHECK(van_.Connect(ctrl.node(0)));
      Node sender = ctrl.node(0);
      CHECK_NOTNULL(node_assigner_)->assign(&sender);
      AddNode(sender);
      break;
    }
    case Control::REPORT_PERF: {
      CHECK(IsScheduler());
      // TODO
      break;
    }
    case Control::READY_TO_EXIT: {
      CHECK(IsScheduler());
      -- num_active_nodes_;
      break;
    }
    case Control::ADD_NODE:
    case Control::UPDATE_NODE: {
      for (int i = 0; i < ctrl.node_size(); ++i) {
        AddNode(ctrl.node(i));
      } break;
    }
    case Control::REPLACE_NODE: {
      // TODO
      break;
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
    if (!IsScheduler()) {
      // the scheduler has already connect this node when processing REQUEST_APP
      CHECK(van_.Connect(node));
    }
    if (node.role() == Node::WORKER) ++ num_workers_;
    if (node.role() == Node::SERVER) ++ num_servers_;
    ++ num_active_nodes_;
  }
  nodes_[node.id()] = node;
  nodes_mu_.unlock();

  // add to app
  for (auto& it : customers_) {
    it.second.first->executor()->AddNode(node);
  }

  if (IsScheduler() && node.id() != van_.my_node().id()) {
    // send all existing nodes info to sender
    Task add_node = NewControlTask(Control::ADD_NODE);
    for (const auto& it : nodes_) {
      *add_node.mutable_ctrl()->add_node() = it.second;
    }
    SendTask(node, add_node);

    // broadcast this new sender info
    for (const auto& it : nodes_) {
      if (it.first == van_.my_node().id() || it.first == node.id()) {
        continue;
      }
      Task add_new_node = NewControlTask(Control::ADD_NODE);
      *add_new_node.mutable_ctrl()->add_node() = node;
      SendTask(it.second, add_new_node);
    }
  }

  if (node.id() == van_.my_node().id()) is_my_node_inited_ = true;
  net_usage_.AddNode(node);
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
  -- num_active_nodes_;
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

  VLOG(1) << "remove node: " << node.ShortDebugString();
}

void Manager::NodeDisconnected(const NodeID node_id) {
  // alreay in shutting down?
  if (in_exit_) return;

  // call handlers
  for (const auto& h : node_failure_handlers_) h(node_id);

  if (IsScheduler()) {
    LOG(INFO) << node_id << " is disconnected";
    RemoveNode(node_id);
  } else {
    // sleep a while, in case this node is already in terminating
    for (int i = 0; i < 500; ++i) {
      usleep(1000);
      if (done_) return;
    }
    LOG(WARNING) << van_.my_node().id() << ": the scheduler is died, killing myself";
    string kill = "kill -9 " + std::to_string(getpid());
    int ret = system(kill.c_str());
    if (ret != 0) LOG(WARNING) << "failed to " << kill;
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
  Message* msg = new Message(task);
  msg->recver = recver;
  Postoffice::instance().Queue(msg);
}

void Manager::WaitServersReady() {
  while (num_servers_ < FLAGS_num_servers) {
    usleep(500);
  }
}
void Manager::WaitWorkersReady() {
  while (num_workers_ < FLAGS_num_workers) {
    usleep(500);
  }
}

// customers
Customer* Manager::customer(int id) {
  auto it = customers_.find(id);
  if (it == customers_.end()) CHECK(false) << id << " does not exist";
  return it->second.first;
}

void Manager::AddCustomer(Customer* obj) {
  CHECK_EQ(customers_.count(obj->id()), 0) << obj->id() << " already exists";
  customers_[obj->id()] = std::make_pair(obj, false);
  nodes_mu_.lock();
  for (const auto& it : nodes_) {
    obj->executor()->AddNode(it.second);
  }
  nodes_mu_.unlock();
}


void Manager::TransferCustomer(Customer* obj) {
  CHECK_EQ(customers_.count(obj->id()), 1) << obj->id() << " dose not exist";
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
