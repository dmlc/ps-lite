#include "system/manager.h"
#include "system/postoffice.h"
#include "ps/app.h"
namespace ps {

DECLARE_int32(num_servers);
DECLARE_int32(num_workers);
DECLARE_int32(num_replicas);
DECLARE_int32(report_interval);

DEFINE_int32(connection_timeout, 10, "connection timeout in sec.");

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
  while (!is_my_node_inited_) usleep(5000);

  // wait all other nodes are ready
  auto role = van_.my_node().role();
  if (role == Node::SCHEDULER) {
    LOG(INFO) << "waiting " << FLAGS_num_servers << " servers and "
              << FLAGS_num_workers << " are ready";
    int num_nodes = FLAGS_num_servers + FLAGS_num_workers;
    if (Timeout(FLAGS_connection_timeout, [this, num_nodes](){
          return num_ready_nodes_ < num_nodes;
        })) {
      LOG(ERROR) << "Timeout ("
                 << FLAGS_connection_timeout
                 << " sec) for the scheduler to wait all nodes ("
                 << num_ready_nodes_ << "/" << num_nodes
                 << ") ready. Exit.";
      LOG(ERROR) << "You may want to set a larger timeout via -connection_timeout";
      return;
    }
  } else {
    if (role == Node::WORKER) {
      WaitServersReady();
    } else if (role == Node::SERVER) {
      WaitWorkersReady();
    }
    Task task = NewControlTask(Control::READY_TO_RUN);
    SendTask(van_.scheduler(), task);
  }


  VLOG(1) << "run app..";
  CHECK_NOTNULL(app_)->Run();
}

void Manager::Stop() {
  if (IsScheduler()) {
    // wait all other nodes are ready for exit. cannot set a timeout here, some
    // apps such as cxxnet has an empty scheduler
    while (unfinished_nodes_.size() > 0) usleep(100000);

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
    while (!done_) usleep(50000);
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
      CHECK_NOTNULL(node_assigner_)->Assign(&sender);
      AddNode(sender);
      break;
    }
    case Control::READY_TO_RUN: {
      CHECK(IsScheduler());
      Lock lk(nodes_mu_);
      unfinished_nodes_.insert(msg->sender);
      ++ num_ready_nodes_;
      break;
    }
    case Control::REPORT_PERF: {
      CHECK(IsScheduler());
      // TODO
      break;
    }
    case Control::READY_TO_EXIT: {
      CHECK(IsScheduler());
      Lock lk(nodes_mu_);
      unfinished_nodes_.erase(msg->sender);
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
  unfinished_nodes_.erase(node_id);
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
    LOG(INFO) << van_.my_node().id() << ": the scheduler is died, exit";
    string kill = "kill -9 " + std::to_string(getpid());
    int ret = system(kill.c_str());
    if (ret != 0) LOG(INFO) << "failed to " << kill;
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

bool Manager::Timeout(int sec, const std::function<bool()>& pred) {
  int itv = 10000;
  int i =  sec * 1000000 / itv;
  while (i > 0 && pred()) {
    usleep(itv); --i;
  }
  if (i == 0 && pred()) return true;
  return false;
}

bool Manager::WaitServersReady() {
  if (Timeout(FLAGS_connection_timeout, [this](){
        return num_servers_ < FLAGS_num_servers;
      })) {
    LOG(ERROR) << van_.my_node().id() << ": timeout ("
               << FLAGS_connection_timeout
               << " sec) for waiting servers ready. "
               << num_servers_ << "/" << FLAGS_num_servers;
    return false;
  }
  return true;
}

bool Manager::WaitWorkersReady() {
  if (Timeout(FLAGS_connection_timeout, [this](){
        return num_workers_ < FLAGS_num_workers;
      })) {
    LOG(ERROR) << van_.my_node().id() << ": timeout ("
               << FLAGS_connection_timeout
               << " sec) for waiting workers ready. "
               << num_workers_ << "/" << FLAGS_num_workers;
    return false;
  }
  return true;
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
