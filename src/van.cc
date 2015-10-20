#include "ps/internal/van.h"
#include <zmq.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include "ps/base.h"
#include "ps/sarray.h"
#include "ps/internal/postoffice.h"
#include "ps/internal/customer.h"
#include "ps/internal/meta_message.pb.h"

namespace ps {

/**
 * \brief return the IP address for given interface eth0, eth1, ...
 */
void GetIP(const std::string& interface, std::string* ip) {
  struct ifaddrs * ifAddrStruct = NULL;
  struct ifaddrs * ifa = NULL;
  void * tmpAddrPtr = NULL;

  getifaddrs(&ifAddrStruct);
  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) continue;
    if (ifa->ifa_addr->sa_family==AF_INET) {
      // is a valid IP4 Address
      tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      if (strncmp(ifa->ifa_name,
                  interface.c_str(),
                  interface.size()) == 0) {
        *ip = addressBuffer;
        break;
      }
    }
  }
  if (ifAddrStruct != NULL) freeifaddrs(ifAddrStruct);
}

/**
 * \brief return the IP address and Interface the first interface which is not
 * loopback
 *
 * only support IPv4
 */
void GetAvailableInterfaceAndIP(
    std::string* interface, std::string* ip) {
  struct ifaddrs * ifAddrStruct = nullptr;
  struct ifaddrs * ifa = nullptr;

  interface->clear();
  ip->clear();
  getifaddrs(&ifAddrStruct);
  for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
    if (nullptr == ifa->ifa_addr) continue;

    if (AF_INET == ifa->ifa_addr->sa_family &&
        0 == (ifa->ifa_flags & IFF_LOOPBACK)) {

      char address_buffer[INET_ADDRSTRLEN];
      void* sin_addr_ptr = &((struct sockaddr_in*)ifa->ifa_addr)->sin_addr;
      inet_ntop(AF_INET, sin_addr_ptr, address_buffer, INET_ADDRSTRLEN);

      *ip = address_buffer;
      *interface = ifa->ifa_name;

      break;
    }
  }
  if (nullptr != ifAddrStruct) freeifaddrs(ifAddrStruct);
  return;
}

/**
 * \brief return an available port on local machine
 *
 * only support IPv4
 * \return 0 on failure
 */
unsigned short GetAvailablePort() {
  struct sockaddr_in addr;
  addr.sin_port = htons(0); // have system pick up a random port available for me
  addr.sin_family = AF_INET; // IPV4
  addr.sin_addr.s_addr = htonl(INADDR_ANY); // set our addr to any interface

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (0 != bind(sock, (struct sockaddr*)&addr, sizeof(struct sockaddr_in))) {
    perror("bind():");
    return 0;
  }

  socklen_t addr_len = sizeof(struct sockaddr_in);
  if (0 != getsockname(sock, (struct sockaddr*)&addr, &addr_len)) {
    perror("getsockname():");
    return 0;
  }

  unsigned short ret_port = ntohs(addr.sin_port);
  close(sock);
  return ret_port;
}

void Van::Start() {
  // start zmq
  context_ = zmq_ctx_new();
  CHECK(context_ != NULL) << "create 0mq context failed";
  zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
  // zmq_ctx_set(context_, ZMQ_IO_THREADS, 4);

  // get scheduler info
  scheduler_.set_hostname(std::string(CHECK_NOTNULL(getenv("DMLC_PS_ROOT_URI"))));
  scheduler_.set_port(atoi(CHECK_NOTNULL(getenv("DMLC_PS_ROOT_PORT"))));
  scheduler_.set_role(Node::SCHEDULER);
  scheduler_.set_id(kScheduler);
  is_scheduler_ = Postoffice::Get()->is_scheduler();

  // get my node info
  if (is_scheduler_) {
    my_node_ = scheduler_;
  } else {
    auto role = is_scheduler_ ? Node::SCHEDULER :
                (Postoffice::Get()->is_worker() ? Node::WORKER : Node::SERVER);
    std::string interface;
    const char*  itf = getenv("DMLC_INTERFACE");
    if (itf) interface = std::string(itf);
    std::string ip;
    if (interface.size()) {
      GetIP(interface, &ip);
    } else {
      GetAvailableInterfaceAndIP(&interface, &ip);
    }
    int port = GetAvailablePort();
    CHECK(!ip.empty()) << "failed to get ip";
    CHECK(!interface.empty()) << "failed to get the interface";
    CHECK(port) << "failed to get a port";
    my_node_.set_role(role);
    my_node_.set_hostname(ip);
    my_node_.set_port(port);
    // cannot determine my id now, the scheduler will assign it later
  }

  // bind. do multiple retries on binding the port. since it's possible that
  // different nodes on the same machine picked the same port. but no retry for
  // the scheduler
  receiver_ = zmq_socket(context_, ZMQ_ROUTER);
  CHECK(receiver_ != NULL)
      << "create receiver socket failed: " << zmq_strerror(errno);
  int local = GetEnv("DMLC_LOCAL", 0);
  std::string addr = local ? "ipc:///tmp/" : "tcp://*:";
  int max_retry = is_scheduler_ ? 40 : 1;
  for (int i = 0; i < max_retry; ++i) {
    auto address = addr + std::to_string(my_node_.port());
    if (zmq_bind(receiver_, address.c_str()) == 0) break;
    CHECK_NE(i, max_retry - 1)
        << "bind failed after " << max_retry << " retries";
    srand((int)time(NULL) + my_node_.port());
    my_node_.set_port(10000 + rand() % 40000);
  }

  // connect to the scheduler
  Connect(scheduler_);

  // start monitor
  // if (is_scheduler_) {
  //   CHECK(!zmq_socket_monitor(receiver_, "inproc://monitor", ZMQ_EVENT_ALL));
  // } else {
  //   CHECK(!zmq_socket_monitor(
  //       senders_[kScheduler], "inproc://monitor", ZMQ_EVENT_ALL));
  // }
  // monitor_thread_ = std::unique_ptr<std::thread>(
  //     new std::thread(&Van::Monitoring, this));

  // start receiver
  receiver_thread_ = std::unique_ptr<std::thread>(
      new std::thread(&Van::Receiving, this));

  if (!is_scheduler_) {
    // let the schduler know myself
    Message msg;
    msg.recver = kScheduler;
    auto ctrl = msg.meta.mutable_control();
    ctrl->set_cmd(Control::ADD_NODE);
    ctrl->add_node()->CopyFrom(my_node_);
    Send_(msg);
  }
  // wait until ready
  while(!ready_) usleep(500);
}

void Van::Stop() {
  // stop threads
  Message exit;
  exit.meta.mutable_control()->set_cmd(Control::TERMINATE);
  exit.recver = my_node_.id();
  Send_(exit);
  receiver_thread_->join();

  // close sockets
  for (auto& it : senders_) zmq_close(it.second);
  zmq_close(receiver_);
  zmq_ctx_destroy(context_);
}

void Van::Connect(const Node& node) {
  CHECK(node.has_id()) << node.ShortDebugString();
  CHECK(node.has_port()) << node.ShortDebugString();
  CHECK(node.has_hostname()) << node.ShortDebugString();

  int id = node.id();

  if (senders_.find(id) != senders_.end()) {
    zmq_close(senders_[id]);
  }

  // worker doesn't need to connect to the other workers. same for server
  if ((node.role() == my_node_.role()) &&
      (node.id() != my_node_.id())) {
    return;
  }

  void *sender = zmq_socket(context_, ZMQ_DEALER);
  CHECK(sender != NULL)
      << zmq_strerror(errno)
      << ". it often can be solved by \"sudo ulimit -n 65536\""
      << " or edit /etc/security/limits.conf";

  if (my_node_.has_id()) {
    std::string my_id = "ps" + std::to_string(my_node_.id());
    zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
  }


  // connect
  std::string addr = "tcp://" + node.hostname() + ":" + std::to_string(node.port());
  if (GetEnv("DMLC_LOCAL", 0)) {
    addr = "ipc:///tmp/" + std::to_string(node.port());
  }

  if (zmq_connect(sender, addr.c_str()) != 0) {
    LOG(FATAL) <<  "connect to " + addr + " failed: " + zmq_strerror(errno);
  }

  senders_[id] = sender;
}

/**
 * \brief be smart on freeing recved data
 */
void FreeData(void *data, void *hint) {
  if (hint == NULL) {
    delete [] (char*)data;
  } else {
    delete (SArray<char>*)hint;
  }
}

int Van::Send_(const Message& msg) {
  std::lock_guard<std::mutex> lk(mu_);

  // find the socket
  int id = msg.recver;
  auto it = senders_.find(id);
  if (it == senders_.end()) {
    LOG(WARNING) << "there is no socket to node " + id;
    return -1;
  }
  void *socket = it->second;

  // send meta
  int meta_size = msg.meta.ByteSize();
  char* meta_buf = new char[meta_size+5];
  CHECK(msg.meta.SerializeToArray(meta_buf, meta_size))
      << "failed to serialize " << msg.meta.ShortDebugString();

  int tag = ZMQ_SNDMORE;
  int n = msg.data.size();
  if (n == 0) tag = 0;
  zmq_msg_t meta_msg;
  zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);

  while (true) {
    if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
    if (errno == EINTR) continue;
    LOG(WARNING) << "failed to send message to node [" << id
                 << "] errno: " << errno << " " << zmq_strerror(errno);
    return -1;
  }
  int send_bytes = meta_size;

  // send data
  for (int i = 0; i < n; ++i) {
    zmq_msg_t data_msg;
    SArray<char>* data = new SArray<char>(msg.data[i]);
    int data_size = data->size();
    zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
    if (i == n - 1) tag = 0;
    while (true) {
      if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to send message to node [" << id
                   << "] errno: " << errno << " " << zmq_strerror(errno)
                   << ". " << i << "/" << n << ": " << msg.meta.ShortDebugString();
      return -1;
    }
    send_bytes += data_size;
  }
  send_bytes_ += send_bytes;
  return send_bytes;
}

int Van::GetNodeID(const char* buf, size_t size) {
  if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
    int id = 0;
    size_t i = 2;
    for (; i < size; ++i) {
      if (buf[i] >= '0' && buf[i] <= '9') {
        id = id * 10 + buf[i] - '0';
      } else {
        break;
      }
    }
    if (i == size) return id;
  }
  return Message::kInvalidNode;
}

int Van::Recv(Message* msg) {
  msg->data.clear();
  size_t recv_bytes = 0;
  for (int i = 0; ; ++i) {
    zmq_msg_t* zmsg = new zmq_msg_t;
    CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
    while (true) {
      if (zmq_msg_recv(zmsg, receiver_, 0) != -1) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to receive message. errno: "
                   << errno << " " << zmq_strerror(errno);
      return -1;
    }
    char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
    size_t size = zmq_msg_size(zmsg);
    recv_bytes += size;

    if (i == 0) {
      // identify
      msg->sender = GetNodeID(buf, size);
      msg->recver = my_node_.id();
      CHECK(zmq_msg_more(zmsg));
      zmq_msg_close(zmsg);
      delete zmsg;
    } else if (i == 1) {
      // task
      CHECK(msg->meta.ParseFromArray(buf, size))
          << "failed to parse string from " << msg->sender
          << ". this is " << my_node_.id() << " " << size;
      zmq_msg_close(zmsg);
      if (!zmq_msg_more(zmsg)) break;
      delete zmsg;
    } else {
      // zero-copy
      SArray<char> data;
      data.reset(buf, size, [zmsg,size](char*) {
          zmq_msg_close(zmsg);
          delete zmsg;
        });
      msg->data.push_back(data);
      if (!zmq_msg_more(zmsg)) { break; }
    }
  }
  recv_bytes_ += recv_bytes;
  return recv_bytes;
}

void Van::Receiving() {
  // for scheduler usage
  MetaMessage nodes;

  while (true) {
    Message msg; CHECK_GE(Recv(&msg), 0);
    if (msg.meta.has_control()) {
      // do some management
      const auto& ctrl = msg.meta.control();
      if (ctrl.cmd() == Control::TERMINATE) {
        break;
      } else if (ctrl.cmd() == Control::ADD_NODE) {
        // assign an id
        if (msg.sender == Message::kInvalidNode) {
          CHECK(is_scheduler_);
          CHECK_EQ(ctrl.node_size(), 1);
          auto node = msg.meta.mutable_control()->mutable_node(0);
          if (node->role() == Node::SERVER) {
            node->set_id(Postoffice::ServerRankToID(num_servers_));
          } else {
            CHECK_EQ(node->role(), Node::WORKER);
            node->set_id(Postoffice::WorkerRankToID(num_workers_));
          }
          nodes.mutable_control()->add_node()->CopyFrom(*node);
        }

        // update my id
        for (int i = 0; i < ctrl.node_size(); ++i) {
          const auto& node = ctrl.node(i);
          if (my_node_.hostname() == node.hostname() &&
              my_node_.port() == node.port()) {
            my_node_.set_id(node.id());
            std::string rank = std::to_string(Postoffice::IDtoRank(node.id()));
            setenv("DMLC_RANK", rank.c_str(), true);
          }
        }

        // connect to these nodes
        for (int i = 0; i < ctrl.node_size(); ++i) {
          const auto& node = ctrl.node(i);
          if (node.role() == Node::SERVER) ++num_servers_;
          if (node.role() == Node::WORKER) ++num_workers_;
          Connect(node);
        }

        if (num_servers_ == Postoffice::Get()->num_servers() &&
            num_workers_ == Postoffice::Get()->num_workers()) {
          if (is_scheduler_) {
            nodes.mutable_control()->add_node()->CopyFrom(my_node_);
            nodes.mutable_control()->set_cmd(Control::ADD_NODE);
            Message back; back.meta = nodes;
            for (int r : Postoffice::Get()->GetNodeIDs(
                     kWorkerGroup + kServerGroup)) {
              back.recver = r; Send_(back);
            }
          }
          ready_ = true;
        }
      } else if (ctrl.cmd() == Control::BARRIER) {
        if (msg.meta.request()) {
          if (barrier_count_.empty()) {
            barrier_count_.resize(8,0);
          }
          CHECK(ctrl.has_barrier_group());
          int group = ctrl.barrier_group();
          ++ barrier_count_[group];
          if (barrier_count_[group] ==
              (int)Postoffice::Get()->GetNodeIDs(group).size()) {
            barrier_count_[group] = 0;
            Message res;
            res.meta.set_request(false);
            res.meta.mutable_control()->set_cmd(Control::BARRIER);
            for (int r : Postoffice::Get()->GetNodeIDs(group)) {
              res.recver = r;
              CHECK_GT(Send_(res), 0);
            }
          }
        } else {
          Postoffice::Get()->Manage(msg);
        }
      }
    } else {
      CHECK_NE(msg.sender, Message::kInvalidNode);
      CHECK_NE(msg.recver, Message::kInvalidNode);
      CHECK(msg.meta.has_customer_id());
      int id = msg.meta.customer_id();
      auto* obj = Postoffice::Get()->GetCustomer(id, 5);
      CHECK(obj) << "timeout (5 sec) to wait App " << id << " ready";
      obj->Accept(msg);
    }
  }
}

void Van::Monitoring() {
  void *s = CHECK_NOTNULL(zmq_socket(context_, ZMQ_PAIR));
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
    // int value = *(uint32_t *)(data + 2);

    // Second frame in message contains event address. it's just the router's
    // address. no help

    if (event == ZMQ_EVENT_DISCONNECTED) {
      // huh...
    }
    if (event == ZMQ_EVENT_MONITOR_STOPPED) break;
  }
  zmq_close (s);
}

}  // namespace ps
