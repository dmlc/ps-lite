/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_ZMQ_VAN_H_
#define PS_ZMQ_VAN_H_
#include <stdio.h>
#include <cstdlib>
#include <zmq.h>
#include <string>
#include <cstring>
#include <thread>
#include <cmath>
#include <atomic>
#include <tuple>
#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"
#if _MSC_VER
#define rand_r(x) rand()
#endif

namespace ps {

struct ZmqBufferContext { // for clarity, don't merge meta and data
  int sender;
  zmq_msg_t* meta_zmsg;
  std::vector<zmq_msg_t*> data_zmsg;
};

/**
 * \brief be smart on freeing recved data
 */
inline void FreeData(void* data, void* hint) {
  if (hint == NULL) {
    delete[] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}

/**
 * \brief ZMQ based implementation
 */
class ZMQVan : public Van {
 public:
  ZMQVan() {}
  virtual ~ZMQVan() {}

 protected:
  void Start(int customer_id) override {
    // start zmq
    start_mu_.lock();
    if (context_ == nullptr) {
      context_ = zmq_ctx_new();
      CHECK(context_ != NULL) << "create 0mq context failed";
    }
    start_mu_.unlock();

    auto val1 = Environment::Get()->find("BYTEPS_ZMQ_MAX_SOCKET");
    int byteps_zmq_max_socket = val1 ? atoi(val1) : 1024;
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, byteps_zmq_max_socket);
    PS_VLOG(1) << "BYTEPS_ZMQ_MAX_SOCKET set to " << byteps_zmq_max_socket;

    auto val2 = Environment::Get()->find("BYTEPS_ZMQ_NTHREADS");
    int byteps_zmq_nthreads = val2 ? atoi(val2) : 4;
    zmq_ctx_set(context_, ZMQ_IO_THREADS, byteps_zmq_nthreads);
    PS_VLOG(1) << "BYTEPS_ZMQ_NTHREADS set to " << byteps_zmq_nthreads;

    Van::Start(customer_id);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();
    // join all threads
    should_stop_ = true;
    for (auto t : thread_list_) t->join();
    PS_VLOG(1) << my_node_.ShortDebugString() << " all threads joined and destroyed";
    // close sockets
    int linger = 0;
    int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
    CHECK(rc == 0 || errno == ETERM);
    CHECK_EQ(zmq_close(receiver_), 0);
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& it : senders_) {
      int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
      CHECK(rc == 0 || errno == ETERM);
      CHECK_EQ(zmq_close(it.second), 0);
    }
    senders_.clear();
    zmq_ctx_destroy(context_);
    context_ = nullptr;
  }

  int Bind(const Node& node, int max_retry) override {
    receiver_ = zmq_socket(context_, ZMQ_ROUTER);
    int option = 1;
    CHECK(!zmq_setsockopt(receiver_, ZMQ_ROUTER_MANDATORY, &option, sizeof(option)))
        << zmq_strerror(errno);
    CHECK(receiver_ != NULL)
        << "create receiver socket failed: " << zmq_strerror(errno);
    int local = GetEnv("DMLC_LOCAL", 0);
    std::string hostname = node.hostname.empty() ? "*" : node.hostname;
    int use_kubernetes = GetEnv("DMLC_USE_KUBERNETES", 0);
    if (use_kubernetes > 0 && node.role == Node::SCHEDULER) {
      hostname = "0.0.0.0";
    }
    std::string addr = local ? "ipc:///tmp/" : "tcp://" + hostname + ":";
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL) + port);
    for (int i = 0; i < max_retry + 1; ++i) {
      auto address = addr + std::to_string(port);
      if (zmq_bind(receiver_, address.c_str()) == 0) break;
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    std::lock_guard<std::mutex> lk(mu_);
    is_worker_ = (node.role == Node::WORKER ? true : false);
    auto t = new std::thread(&ZMQVan::CallZmqRecvThread, this, (void*) receiver_);
    thread_list_.push_back(t);

    return port;
  }

  void Connect(const Node& node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    int id = node.id;
    mu_.lock();
    auto it = senders_.find(id);
    if (it != senders_.end()) {
      zmq_close(it->second);
    }
    mu_.unlock();
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }
    void* sender = zmq_socket(context_, ZMQ_DEALER);
    CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
    if (my_node_.id != Node::kEmpty) {
      std::string my_id = "ps" + std::to_string(my_node_.id);
      zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
      std::lock_guard<std::mutex> lk(mu_);
      if (is_worker_ && (senders_.find(id)==senders_.end())) {
        auto t = new std::thread(&ZMQVan::CallZmqRecvThread, this, (void*) sender);
        thread_list_.push_back(t);
      }
    }
    // connect
    std::string addr =
        "tcp://" + node.hostname + ":" + std::to_string(node.port);
    if (GetEnv("DMLC_LOCAL", 0)) {
      addr = "ipc:///tmp/" + std::to_string(node.port);
    }
    if (zmq_connect(sender, addr.c_str()) != 0) {
      LOG(FATAL) << "connect to " + addr + " failed: " + zmq_strerror(errno);
    }
    std::lock_guard<std::mutex> lk(mu_);
    senders_[id] = sender;
  }

  int SendMsg(Message& msg) override {
    if (!is_worker_) return NonWorkerSendMsg(msg);

    std::lock_guard<std::mutex> lk(mu_);

    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);

    // find the socket
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "there is no socket to node " << id;
      return -1;
    }

    void* socket = it->second;

    return ZmqSendMsg(socket, msg);
  }

  int RecvMsg(Message* msg) override {
    msg->data.clear();

    ZmqBufferContext notification;
    recv_buffers_.WaitAndPop(&notification);

    size_t recv_bytes = 0;

    msg->meta.sender = notification.sender;
    msg->meta.recver = my_node_.id;

    char* meta_buf = CHECK_NOTNULL((char*)zmq_msg_data(notification.meta_zmsg));
    size_t meta_len = zmq_msg_size(notification.meta_zmsg);

    UnpackMeta(meta_buf, meta_len, &(msg->meta));
    recv_bytes += meta_len;

    for (size_t i = 0; i < notification.data_zmsg.size(); ++i) {
      auto zmsg = notification.data_zmsg[i];
      char* buf = CHECK_NOTNULL((char*)zmq_msg_data(zmsg));
      size_t size = zmq_msg_size(zmsg);
      recv_bytes += size;


      SArray<char> data;
      // zero copy
      data.reset(buf, size, [zmsg, size](void *) {
        zmq_msg_close(zmsg);
        delete zmsg;
      });
      msg->data.push_back(data);
    }

    return recv_bytes;
  }

 private:

  int NonWorkerSendMsg(Message& msg) {
    std::lock_guard<std::mutex> lk(mu_);

    // find the socket
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);

    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "there is no socket to node " << id;
      return -1;
    }

    void* socket;

    if (msg.meta.simple_app || !msg.meta.control.empty()
               || (GetRoleFromId(id) != Node::WORKER)) {
      socket = it->second;
    }
    else { // data msg, and recver is WORKER
      socket = receiver_; // scheduler/server using receiver socket --> worker sender socket

      // first, send dst id
      std::string dst = "ps" + std::to_string(id);
      int len = dst.size();
      char *dst_array = new char[len + 1];
      strcpy(dst_array, dst.c_str());
      CHECK(dst_array);

      zmq_msg_t zmsg_dstid;
      CHECK_EQ(zmq_msg_init_data(
          &zmsg_dstid, dst_array, len, FreeData, NULL), 0);
      while (true) {
        if (len == zmq_msg_send(&zmsg_dstid, receiver_, ZMQ_SNDMORE)) break;
        if (errno == EINTR) continue;
        CHECK(0) << zmq_strerror(errno);
      }

      // second, send my id
      std::string my_id = "ps" + std::to_string(my_node_.id);
      len = my_id.size();
      char *myid_array = new char[len + 1];
      strcpy(myid_array, my_id.c_str());
      CHECK(myid_array);

      zmq_msg_t zmsg_myid;
      CHECK_EQ(zmq_msg_init_data(
          &zmsg_myid, myid_array, len, FreeData, NULL), 0);
      while (true) {
        if (len == zmq_msg_send(&zmsg_myid, receiver_, ZMQ_SNDMORE)) break;
        if (errno == EINTR) continue;
        CHECK(0) << zmq_strerror(errno);
      }
    }

    return ZmqSendMsg(socket, msg);
  }

  void CallZmqRecvThread(void* socket) {
    CHECK(socket);
    LOG(INFO) << "Start ZMQ recv thread";

    while (true) {
      ZmqBufferContext *buf_ctx = new ZmqBufferContext();

      for (int i = 0;; ++i) {
        zmq_msg_t* zmsg = new zmq_msg_t;
        CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
        while (true) {
          std::lock_guard<std::mutex> lk(mu_);
          // the zmq_msg_recv should be non-blocking, otherwise deadlock will happen
          int tag = ZMQ_DONTWAIT;
          if (should_stop_ || zmq_msg_recv(zmsg, socket, tag) != -1) break;
          if (errno == EINTR) {
            std::cout << "interrupted";
            continue;
          } else if (errno == EAGAIN) { // ZMQ_DONTWAIT
            continue;
          }
          CHECK(0) << "failed to receive message. errno: " << errno << " "
                       << zmq_strerror(errno);
        }
        if (should_stop_) break;
        char* buf = CHECK_NOTNULL((char*)zmq_msg_data(zmsg));
        size_t size = zmq_msg_size(zmsg);

        if (i == 0) {
          // identify
          buf_ctx->sender = GetNodeID(buf, size);
          CHECK(zmq_msg_more(zmsg));
          zmq_msg_close(zmsg);
          delete zmsg;
        }
        else if (i == 1) {
          // task
          buf_ctx->meta_zmsg = zmsg;
          bool more = zmq_msg_more(zmsg);
          if (!more) break;
        }
        else {
          buf_ctx->data_zmsg.push_back(zmsg);
          bool more = zmq_msg_more(zmsg);
          if (!more) break;
        }
      } // for
      if (should_stop_) break;
      recv_buffers_.Push(*buf_ctx);
    } // while
  }

  int ZmqSendMsg(void* socket, Message& msg) {
    // send meta
    int meta_size;
    char* meta_buf = nullptr;
    PackMeta(msg.meta, &meta_buf, &meta_size);
    int tag = ZMQ_SNDMORE;
    int n = msg.data.size();
    if (n == 0) tag = 0;
    zmq_msg_t meta_msg;
    zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
    while (true) {
      if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
      if (errno == EINTR) continue;
      CHECK(0) << zmq_strerror(errno);
    }
    zmq_msg_close(&meta_msg);
    int send_bytes = meta_size;

    // send data
    for (int i = 0; i < n; ++i) {
      zmq_msg_t data_msg;
      SArray<char>* data = new SArray<char>(msg.data[i]);
      int data_size = data->size();
      zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
      if (i == n - 1) tag = ZMQ_DONTWAIT;
      while (true) {
        if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "failed to send message, errno: "
                     << errno << " " << zmq_strerror(errno)
                     << ". " << i << "/" << n;
        return -1;
      }
      zmq_msg_close(&data_msg);
      send_bytes += data_size;
    }
    return send_bytes;
  }

  /**
   * return the node id given the received identity
   * \return -1 if not find
   */
  int GetNodeID(const char* buf, size_t size) {
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
    return Meta::kEmpty;
  }

  Node::Role GetRoleFromId(int id) {
    if (id < 8) return Node::SCHEDULER;
    if (id % 2) return Node::WORKER;
    return Node::SERVER;
  }

  void* context_ = nullptr;
  /**
   * \brief node_id to the socket for sending data to this node
   */
  std::unordered_map<int, void*> senders_;
  std::mutex mu_;
  void* receiver_ = nullptr;

  bool is_worker_;

  // Recv buffer queue
  ThreadsafeQueue<ZmqBufferContext> recv_buffers_;

  std::atomic<bool> should_stop_{false};

  std::vector<std::thread*> thread_list_;

};
}  // namespace ps

#endif  // PS_ZMQ_VAN_H_

// monitors the liveness other nodes if this is
// a schedule node, or monitors the liveness of the scheduler otherwise
// aliveness monitor
// CHECK(!zmq_socket_monitor(
//     senders_[kScheduler], "inproc://monitor", ZMQ_EVENT_ALL));
// monitor_thread_ = std::unique_ptr<std::thread>(
//     new std::thread(&Van::Monitoring, this));
// monitor_thread_->detach();

// void Van::Monitoring() {
//   void *s = CHECK_NOTNULL(zmq_socket(context_, ZMQ_PAIR));
//   CHECK(!zmq_connect(s, "inproc://monitor"));
//   while (true) {
//     //  First frame in message contains event number and value
//     zmq_msg_t msg;
//     zmq_msg_init(&msg);
//     if (zmq_msg_recv(&msg, s, 0) == -1) {
//       if (errno == EINTR) continue;
//       break;
//     }
//     uint8_t *data = static_cast<uint8_t*>(zmq_msg_data(&msg));
//     int event = *reinterpret_cast<uint16_t*>(data);
//     // int value = *(uint32_t *)(data + 2);

//     // Second frame in message contains event address. it's just the router's
//     // address. no help

//     if (event == ZMQ_EVENT_DISCONNECTED) {
//       if (!is_scheduler_) {
//         PS_VLOG(1) << my_node_.ShortDebugString() << ": scheduler is dead.
//         exit."; exit(-1);
//       }
//     }
//     if (event == ZMQ_EVENT_MONITOR_STOPPED) {
//       break;
//     }
//   }
//   zmq_close(s);
// }
