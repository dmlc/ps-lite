#include "system/van.h"
#include <string.h>
#include <zmq.h>
#include <libgen.h>
#include <stdlib.h>
#include <time.h>
#include <functional>
#include "ps/shared_array.h"
#include "system/manager.h"
#include "system/postoffice.h"

namespace ps {
DEFINE_int32(bind_to, 0, "binding port");
DEFINE_bool(local, false, "run in local");

DECLARE_string(my_node);
DECLARE_string(scheduler);
DECLARE_int32(num_workers);
DECLARE_int32(num_servers);

Van::~Van() {
  for (auto& it : senders_) zmq_close(it.second);
  zmq_close(receiver_);
  zmq_ctx_destroy(context_);
}

void Van::Init() {
  scheduler_ = ParseNode(FLAGS_scheduler);
  my_node_ = ParseNode(FLAGS_my_node);
  LOG(INFO) << "I'm [" << my_node_.ShortDebugString() << "]";

  context_ = zmq_ctx_new();
  CHECK(context_ != NULL) << "create 0mq context failed";

  // one need to "sudo ulimit -n 65536" or edit /etc/security/limits.conf
  zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
  // zmq_ctx_set(context_, ZMQ_IO_THREADS, 4);

  Bind();
  // connect(my_node_);
  Connect(scheduler_);

  // setup monitor
  if (IsScheduler()) {
    CHECK(!zmq_socket_monitor(receiver_, "inproc://monitor", ZMQ_EVENT_ALL));
  } else {
    CHECK(!zmq_socket_monitor(
        senders_[scheduler_.id()], "inproc://monitor", ZMQ_EVENT_ALL));
  }
  monitor_thread_ = new std::thread(&Van::Monitor, this);
  monitor_thread_->detach();
}


void Van::Bind() {
  receiver_ = zmq_socket(context_, ZMQ_ROUTER);
  CHECK(receiver_ != NULL)
      << "create receiver socket failed: " << zmq_strerror(errno);
  string addr = "tcp://*:";
  bool retry = false;
  if (FLAGS_bind_to) {
    addr += std::to_string(FLAGS_bind_to);
  } else {
    CHECK(my_node_.has_port()) << my_node_.ShortDebugString();
    if (!IsScheduler()) retry = true;
    addr += std::to_string(my_node_.port());
  }
  if (FLAGS_local) {
    addr = "ipc:///tmp/" + my_node_.id();
  }
  std::hash<std::string> hash;
  srand((int)time(NULL) + hash(my_node_.id()));
  int max_retry = retry ? 40 : 1;
  for (int i = 0; i < max_retry; ++i) {
    if (zmq_bind(receiver_, addr.c_str()) == 0) break;
    CHECK_NE(i, max_retry - 1)
        << "bind to " << addr << " failed: " << " " << zmq_strerror(errno);

    my_node_.set_port(10000 + rand() % 40000);
    addr = "tcp://*:" + std::to_string(my_node_.port());
  }

  VLOG(1) << "BIND address " << addr;
}

void Van::Disconnect(const Node& node) {
  CHECK(node.has_id()) << node.ShortDebugString();
  NodeID id = node.id();
  if (senders_.find(id) != senders_.end()) {
    zmq_close (senders_[id]);
  }
  senders_.erase(id);
  VLOG(1) << "DISCONNECT from " << node.id();
}

bool Van::Connect(const Node& node) {
  CHECK(node.has_id()) << node.ShortDebugString();
  CHECK(node.has_port()) << node.ShortDebugString();
  CHECK(node.has_hostname()) << node.ShortDebugString();
  NodeID id = node.id();
  if (id == my_node_.id()) {
    // update my node info
    my_node_ = node;
  }
  if (senders_.find(id) != senders_.end()) {
    return true;
  }

  if ((node.role() == my_node_.role()) && (node.role() != Node::SCHEDULER)) {
    return true;
  }

  void *sender = zmq_socket(context_, ZMQ_DEALER);
  CHECK(sender != NULL) << zmq_strerror(errno)
                        << ". it often can be solved by \"sudo ulimit -n 65536\" or edit /etc/security/limits.conf";

  string my_id = my_node_.id(); // address(my_node_);
  zmq_setsockopt (sender, ZMQ_IDENTITY, my_id.data(), my_id.size());

  // uint64_t hwm = 500000;
  // zmq_setsockopt(sender, ZMQ_SNDHWM, &hwm, sizeof(hwm));

  // connect
  string addr = "tcp://" + node.hostname() + ":" + std::to_string(node.port());
  if (FLAGS_local) {
    addr = "ipc:///tmp/" + node.id();
  }
  if (zmq_connect(sender, addr.c_str()) != 0) {
    LOG(WARNING) << "connect to " + addr + " failed: " + zmq_strerror(errno);
    return false;
  }

  senders_[id] = sender;

  VLOG(1) << "CONNECT to " << id << " [" << addr << "]";
  return true;
}

bool Van::Send(Message* msg, size_t* send_bytes) {
  // find the socket
  NodeID id = msg->recver;
  auto it = senders_.find(id);
  if (it == senders_.end()) {
    LOG(WARNING) << "there is no socket to node " + id;
    return false;
  }
  void *socket = it->second;

  // double check
  bool has_key = !msg->key.empty();
  if (has_key) {
    msg->task.set_has_key(has_key);
  } else {
    msg->task.clear_has_key();
  }
  int n = has_key + msg->value.size();

  // send task
  int task_size = msg->task.ByteSize();
  char* task_buf = new char[task_size+5];
  CHECK(msg->task.SerializeToArray(task_buf, task_size))
      << "failed to serialize " << msg->task.ShortDebugString();

  int tag = ZMQ_SNDMORE;
  if (n == 0) tag = 0; // ZMQ_DONTWAIT;
  zmq_msg_t task_msg;
  zmq_msg_init_data(&task_msg, task_buf, task_size, FreeData, NULL);

  while (true) {
    if (zmq_msg_send(&task_msg, socket, tag) == task_size) break;
    if (errno == EINTR) continue;
    LOG(WARNING) << "failed to send message to node [" << id
                 << "] errno: " << errno << " " << zmq_strerror(errno);
    return false;
  }
  *send_bytes += task_size;

  // send data
  for (int i = 0; i < n; ++i) {
    SArray<char>* data = new SArray<char>(
        (has_key && i == 0) ? msg->key : msg->value[i-has_key]);
    zmq_msg_t data_msg;
    int data_size = data->size();
    zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
    if (i == n - 1) tag = 0; // ZMQ_DONTWAIT;
    while (true) {
      if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
      if (errno == EINTR) continue;
      // if (errno == EINTR || errno == EAGAIN) {
      //   usleep(1000);
      //   continue;
      // }
      LOG(WARNING) << "failed to send message to node [" << id
                   << "] errno: " << errno << " " << zmq_strerror(errno)
                   << ". " << i << "/" << n << ": " << msg->ShortDebugString();
      return false;
    }
    *send_bytes += data_size;
  }

  VLOG(1) << "TO " << msg->recver << " " << msg->ShortDebugString();
  return true;
}

bool Van::Recv(Message* msg, size_t* recv_bytes) {
  msg->clear_data();
  for (int i = 0; ; ++i) {
    // zmq_msg_t zmsg;
    zmq_msg_t* zmsg = new zmq_msg_t;
    CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
    while (true) {
      if (zmq_msg_recv(zmsg, receiver_, 0) != -1) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to receive message. errno: "
                   << errno << " " << zmq_strerror(errno);
      return false;
    }
    char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
    size_t size = zmq_msg_size(zmsg);
    *recv_bytes += size;

    if (i == 0) {
      // identify
      msg->sender = std::string(buf, size);
      msg->recver = my_node_.id();
      CHECK(zmq_msg_more(zmsg));
      zmq_msg_close(zmsg);
      delete zmsg;
    } else if (i == 1) {
      // task
      CHECK(msg->task.ParseFromArray(buf, size))
          << "failed to parse string from " << msg->sender
          << ". this is " << my_node_.id() << " " << size;
      // ZMQ_IDENTITY_FD is not availabe after zmq-4.0.1-rc1
      // if (IsScheduler() && msg->task.has_ctrl() &&
      //     msg->task.ctrl().cmd() == Control::REGISTER_NODE) {
      //   // it is the first time the scheduler receive message from the
      //   // sender. store the file desciptor of the sender for the monitor
      //   int val[64]; size_t val_len = msg->sender.size();
      //   CHECK_LT(val_len, 64*sizeof(int));
      //   memcpy(val, msg->sender.data(), val_len);
      //   CHECK(!zmq_getsockopt(
      //       receiver_,  ZMQ_IDENTITY_FD, (char*)val, &val_len))
      //       << "failed to get the file descriptor of " << msg->sender
      //       << ". error: " << errno << " " << zmq_strerror(errno);
      //   CHECK_EQ(val_len, (size_t)4);
      //   int fd = val[0];

      //   VLOG(1) << "node [" << msg->sender << "] is on file descriptor " << fd;
      //   Lock l(fd_to_nodeid_mu_);
      //   fd_to_nodeid_[fd] = msg->sender;
      // }
      zmq_msg_close(zmsg);
      if (!zmq_msg_more(zmsg)) break;
      delete zmsg;
    } else {
      // copy data
      // SArray<char> data; data.CopyFrom(buf, size);

      // ugly zero-copy
      SArray<char> data;
      data.reset(buf, size, [zmsg](char*) {
          zmq_msg_close(zmsg);
          delete zmsg;
        });

      if (i == 2 && msg->task.has_key()) {
        msg->key = data;
      } else {
        msg->value.push_back(data);
      }
      if (!zmq_msg_more(zmsg)) { break; }
    }
  }

  VLOG(1) << "FROM: " << msg->sender << " " << msg->ShortDebugString();
  return true;
}

Node Van::ParseNode(const string& node_str) {
  Node node;
  CHECK(google::protobuf::TextFormat::ParseFromString(node_str, &node));
  if (!node.has_id()) {
    string str = node.hostname() + "_" + std::to_string(node.port());
    if (node.role() == Node::SCHEDULER) {
      str = "H";
    } else if (node.role() == Node::WORKER) {
      str = "W_" + str;
    } else if (node.role() == Node::SERVER) {
      str = "S_" + str;
    }
    node.set_id(str);
  }
  return node;
}

void Van::Monitor() {
  VLOG(1) << "starting monitor...";
  void *s = CHECK_NOTNULL(zmq_socket (context_, ZMQ_PAIR));
  CHECK(!zmq_connect (s, "inproc://monitor"));
  while (true) {
    //  First frame in message contains event number and value
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    if (zmq_msg_recv(&msg, s, 0) == -1) {
      if (errno == EINTR) continue;
      break;
    }
    uint8_t *data = (uint8_t *)zmq_msg_data (&msg);
    int event = *(uint16_t *)(data);
    int value = *(uint32_t *)(data + 2);

    //  Second frame in message contains event address. it's just the router's
    // address. no help
    // zmq_msg_init(&msg);
    // if (zmq_msg_recv(&msg, s, 0) == -1) {
    //   if (errno == EINTR) continue;
    //   break;
    // }
    // std::string address((char*)zmq_msg_data(&msg), zmq_msg_size(&msg));
    // LOG(INFO) << event << " " << value << " " << address;

    if (event == ZMQ_EVENT_DISCONNECTED) {
      auto& manager = Postoffice::instance().manager();
      if (IsScheduler()) {

        Lock l(fd_to_nodeid_mu_);
        if (fd_to_nodeid_.find(value) == fd_to_nodeid_.end()) {
          manager.NodeDisconnected("JUST_A_UNKNOWN_NODE");
        } else {
          manager.NodeDisconnected(fd_to_nodeid_[value]);
        }
      } else {
        manager.NodeDisconnected(scheduler_.id());
      }
    }
    if (event == ZMQ_EVENT_MONITOR_STOPPED) break;
  }
  zmq_close (s);
  VLOG(1) << "monitor stopped.";
}

} // namespace ps


// check whether I could connect to a specified node
// bool connected(const Node& node);
// bool Van::connected(const Node& node) {
//   auto it = senders_.find(node.id());
//   return it != senders_.end();
// }
