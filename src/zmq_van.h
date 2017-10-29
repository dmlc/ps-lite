#pragma once
/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_ZMQ_VAN_H_
#define PS_ZMQ_VAN_H_
#include <zmq.h>
#include <stdlib.h>
#include <thread>
#include <string>
#include "ps/internal/van.h"
#include "dmlc/DIME.h"
#include "PHubAllocator.h"
 //#include "infiniband_van.h"

#if _MSC_VER
#define rand_r(x) rand()
#endif


namespace ps {
    /**
     * \brief be smart on freeing recved data
     */
    inline void FreeData(void *data, void *hint) {
        if (hint == NULL) {
            delete[] static_cast<char*>(data);
        }
        else {
            delete static_cast<SArray<char>*>(hint);
        }
    }

    /**
     * \brief ZMQ based implementation
     */
    class ZMQVan : public Van {
    public:
        ZMQVan()
        {
            if (GetEnv("DMLC_NUM_SERVER", 1) == 1)
            {
                FeatureSet = 0;//Van::SupportsKeyChunking;
            }
	    else
	    {
		FeatureSet = 0;
	    }
        }
        virtual ~ZMQVan() { }
        void Start() override {
            // start zmq
            context_ = zmq_ctx_new();
            CHECK(context_ != NULL) << "create 0mq context failed";
            zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
            // zmq_ctx_set(context_, ZMQ_IO_THREADS, 4);
            //printf("[unknown]ZMQ van active.\n");
            Van::Start();
            //printf("[%d]Van fully initialized.\n", my_node().id)
        }

        virtual int PHUBGetKeySharding(Key key) override
        {
            return key % (ps::Postoffice::Get()->num_servers());
        }

        virtual std::string VanType() override
        {
            return "zmq";
        }

        void Stop() override {
            PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
            Van::Stop();
            // close sockets
            int linger = 0;
            int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
            CHECK(rc == 0 || errno == ETERM);
            CHECK_EQ(zmq_close(receiver_), 0);
            for (auto& it : senders_) {
                int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
                CHECK(rc == 0 || errno == ETERM);
                CHECK_EQ(zmq_close(it.second), 0);
            }
            zmq_ctx_destroy(context_);
        }

        int Bind(const Node& node, int max_retry) override {
            receiver_ = zmq_socket(context_, ZMQ_ROUTER);
            CHECK(receiver_ != NULL)
                << "create receiver socket failed: " << zmq_strerror(errno);
            int local = GetEnv("DMLC_LOCAL", 0);
            std::string addr = local ? "ipc:///tmp/" : "tcp://*:";
            int port = node.port;
            unsigned seed = static_cast<unsigned>(time(NULL) + port);
            for (int i = 0; i < max_retry + 1; ++i) {
                auto address = addr + std::to_string(port);
                if (zmq_bind(receiver_, address.c_str()) == 0) break;
                if (i == max_retry) {
                    port = -1;
                }
                else {
                    port = 10000 + rand_r(&seed) % 40000;
                }
            }
            return port;
        }

        void Connect(const Node& node) override {
            CHECK_NE(node.id, node.kEmpty);
            CHECK_NE(node.port, node.kEmpty);
            CHECK(node.hostname.size());
            int id = node.id;
            auto it = senders_.find(id);
            if (it != senders_.end()) {
                zmq_close(it->second);
            }
            // worker doesn't need to connect to the other workers. same for server
            if ((node.role == my_node_.role) &&
                (node.id != my_node_.id)) {
                return;
            }
            void *sender = zmq_socket(context_, ZMQ_DEALER);
            CHECK(sender != NULL)
                << zmq_strerror(errno)
                << ". it often can be solved by \"sudo ulimit -n 65536\""
                << " or edit /etc/security/limits.conf";
            if (my_node_.id != Node::kEmpty) {
                std::string my_id = "ps" + std::to_string(my_node_.id);
                zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
            }
            // connect
            std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
            if (GetEnv("DMLC_LOCAL", 0)) {
                addr = "ipc:///tmp/" + std::to_string(node.port);
            }
            if (zmq_connect(sender, addr.c_str()) != 0) {
                LOG(FATAL) << "connect to " + addr + " failed: " + zmq_strerror(errno);
            }
            senders_[id] = sender;
        }
        std::mutex mu_;

        int SendMsg(const Message& msg) override {
            std::lock_guard<std::mutex> lk(mu_);
            // find the socket
            int id = msg.meta.recver;
            CHECK_NE(id, Meta::kEmpty);
            auto it = senders_.find(id);
            if (it == senders_.end()) {
                LOG(WARNING) << "there is no socket to node " << id;
                return -1;
            }
            void *socket = it->second;

            // send meta
            int meta_size; char* meta_buf;
#ifdef PHUB_PERF_DIAG
            if (msg.meta.control.empty() && msg.meta.simple_app == false && msg.meta.head == 0)
            {
                PerfMonLite& pMon = PerfMonLite::Get();
                const_cast<Message&>(msg).meta.body.clear();
                const_cast<Message&>(msg).meta.body += std::to_string(pMon.GetCurrentTimestamp());
                //printf("sending bdy = %s\n", msg.meta.body.c_str());
            }
#endif

            PackMeta(msg.meta, &meta_buf, &meta_size);
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
            zmq_msg_close(&meta_msg);
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
                        << ". " << i << "/" << n;
                    return -1;
                }
                zmq_msg_close(&data_msg);
                send_bytes += data_size;
            }
            //its a send.

            return send_bytes;
        }

        int RecvCtrlMsgNoWait(Message* msg) {
            msg->data.clear();
            size_t recv_bytes = 0;
            for (int i = 0; ; ++i)
            {
                zmq_msg_t* zmsg = new zmq_msg_t;
                CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
                while (true)
                {
                    //receiverLock.lock();
                    if (zmq_msg_recv(zmsg, receiver_, ZMQ_DONTWAIT) == -1)
                    {
                        //receiverLock.unlock();
                        if (errno == EINTR) continue;
                        else if (errno == EAGAIN)
                        {
                            delete zmsg;
                            return -1;
                        }
                        else
                        {
                            LOG(WARNING) << "failed to receive message. errno: "
                                << errno << " " << zmq_strerror(errno);
                        }
                    }
                    else
                    {
                        break;
                    }
                }
                char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
                size_t size = zmq_msg_size(zmsg);
                recv_bytes += size;

                if (i == 0) {
                    // identify
                    msg->meta.sender = GetNodeID(buf, size);
                    msg->meta.recver = my_node_.id;
                    CHECK(zmq_msg_more(zmsg));
                    zmq_msg_close(zmsg);
                    delete zmsg;
                }
                else if (i == 1) {
                    // task
                    UnpackMeta(buf, size, &(msg->meta));
                    zmq_msg_close(zmsg);
                    bool more = zmq_msg_more(zmsg);
                    delete zmsg;
                    if (!more) break;
                }
                else
                {
                    // zero-copy
                    SArray<char> data;
                    data.reset(buf, size, [zmsg, size](char* buf) {
                        zmq_msg_close(zmsg);
                        delete zmsg;
                    });
                    msg->data.push_back(data);
                    if (!zmq_msg_more(zmsg)) { break; }

                }
            }
            return recv_bytes;
        }

        int RecvMsg(Message* msg) override {
            msg->data.clear();
            size_t recv_bytes = 0;
            for (int i = 0; ; ++i) {
                zmq_msg_t* zmsg = new zmq_msg_t;
                CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
                while (true) {
                    //receiverLock.lock();
                    if (zmq_msg_recv(zmsg, receiver_, 0) != -1)
                    {
                        //receiverLock.unlock();
                        break;
                    }
                    //receiverLock.unlock();
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
                    msg->meta.sender = GetNodeID(buf, size);
                    msg->meta.recver = my_node_.id;
                    CHECK(zmq_msg_more(zmsg));
                    zmq_msg_close(zmsg);
                    delete zmsg;
                }
                else if (i == 1) {
                    // task
                    UnpackMeta(buf, size, &(msg->meta));
                    zmq_msg_close(zmsg);
                    bool more = zmq_msg_more(zmsg);
                    delete zmsg;
                    if (!more) break;
                }
                else {
                    // zero-copy
                    SArray<char> data;
                    data.reset(buf, size, [zmsg, size](char* buf) {
                        zmq_msg_close(zmsg);
                        delete zmsg;
                    });
                    msg->data.push_back(data);
                    if (!zmq_msg_more(zmsg)) { break; }
                }
            }
#ifdef PHUB_PERF_DIAG
            if (msg->meta.control.empty() && msg->meta.simple_app == false && msg->meta.head == 0)
            {
                //	printf("trying %s. %s\n", msg->meta.body.c_str(), msg->DebugString().c_str());
                uint64_t t = std::stol(msg->meta.body);

                if (my_node_.role == Node::WORKER)
                {
                    if (msg->meta.push == 0)
                    {
                        //this is a pull ack
                        Key k = *((Key*)msg->data[0].data());
                        MEASURE_THIS_TIME_BEG_EX(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PULL_ACK, k, t);
                        MEASURE_THIS_TIME_END(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PULL_ACK, k);
                    }
                    else
                    {
                        //this is a push ack. a push ack is the same regardless of keys.
                        MEASURE_THIS_TIME_BEG_EX(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PUSH_ACK, msg->meta.control.msg_sig, t);
                        MEASURE_THIS_TIME_END(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PUSH_ACK, msg->meta.control.msg_sig);
                    }
                }
                else if (my_node_.role == Node::SERVER)
                {
                    Key k = *((Key*)msg->data[0].data());
                    if (msg->meta.push == 0)
                    {
                        //printf("in_flight_time_pull msg=%s, t=%d\n", msg->DebugString().c_str(), (int)t);
                        //this is a pull from worker
                        MEASURE_THIS_TIME_BEG_EX(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL, k, t);
                        MEASURE_THIS_TIME_END(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL, k);
                    }
                    else
                    {
                        MEASURE_THIS_TIME_BEG_EX(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PUSH, k, t);
                        MEASURE_THIS_TIME_END(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PUSH, k);
                    }
                }
            }
#endif
            return recv_bytes;
        }

    private:
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
                    }
                    else {
                        break;
                    }
                }
                if (i == size) return id;
            }
            return Meta::kEmpty;
        }

        void *context_ = nullptr;
        /**
         * \brief node_id to the socket for sending data to this node
         */
        std::unordered_map<int, void*> senders_;
        std::mutex receiverLock;
        void *receiver_ = nullptr;
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
//         PS_VLOG(1) << my_node_.ShortDebugString() << ": scheduler is dead. exit.";
//         exit(-1);
//       }
//     }
//     if (event == ZMQ_EVENT_MONITOR_STOPPED) {
//       break;
//     }
//   }
//   zmq_close(s);
// }
