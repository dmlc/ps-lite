#include "system/postoffice.h"
#include "ps/app.h"

namespace ps {

DECLARE_string(interface);

Postoffice::~Postoffice() {
  if (recv_thread_) recv_thread_->join();
  if (send_thread_) {
    Message* stop = new Message(); stop->terminate = true; Queue(stop);
    send_thread_->join();
  }
}

void Postoffice::Run(int* argc, char*** argv) {
  google::InitGoogleLogging((*argv)[0]);
  google::ParseCommandLineFlags(argc, argv, true);

  manager_.Init(*argc, *argv);

  // start the I/O threads
  recv_thread_ =
      std::unique_ptr<std::thread>(new std::thread(&Postoffice::Recv, this));
  send_thread_ =
      std::unique_ptr<std::thread>(new std::thread(&Postoffice::Send, this));

  manager_.Run();
}

void Postoffice::Send() {
  Message* msg;
  while (true) {
    sending_queue_.wait_and_pop(msg);
    if (msg->terminate) break;
    size_t send_bytes = 0;
    manager_.van().Send(msg, &send_bytes);
    manager_.net_usage().IncrSend(msg->recver, send_bytes);
    if (msg->task.request()) {
      // a request "msg" is safe to be deleted only if the response is received
      manager_.AddRequest(msg);
    } else {
      delete msg;
    }
  }
}

void Postoffice::Recv() {
  while (true) {
    // receive a message
    Message* msg = new Message();
    size_t recv_bytes = 0;
    CHECK(manager_.van().Recv(msg, &recv_bytes));
    manager_.net_usage().IncrRecv(msg->sender, recv_bytes);

    // process
    if (!msg->task.request()) manager_.AddResponse(msg);
    if (msg->task.control()) {
      if (!manager_.Process(msg)) {
        delete msg; break;
      }
      delete msg;
    } else {
      int id = msg->task.customer_id();
      // let the executor to delete "msg"
      manager_.customer(id)->executor()->Accept(msg);
    }
  }
}

} // namespace ps

// the version support packed message...

// void Postoffice::Queue(Message* msg) {
//   if (!msg->task.has_more()) {
//     sending_queue_.push(msg);
//   } else {
//     // do pack
//     CHECK(msg->task.request());
//     CHECK(msg->task.has_customer_id());
//     CHECK(!msg->has_data()) << " don't know how to pack data";
//     Lock lk(pack_mu_);
//     auto key = std::make_pair(msg->recver, msg->task.customer_id());
//     auto& value = pack_[key];
//     value.push_back(msg);

//     if (!msg->task.more()) {
//       // it's the final message, pack and send
//       Message* pack_msg = new Message();
//       pack_msg->recver = msg->recver;
//       for (auto m : value) {
//         m->task.clear_more();
//         *pack_msg->task.add_task() = m->task;
//         delete m;
//       }
//       value.clear();
//       sending_queue_.push(pack_msg);
//     }
//   }
// }

// void Postoffice::Recv() {
//   while (true) {
//     // receive a message
//     Message* msg = new Message();
//     size_t recv_bytes = 0;
//     CHECK(manager_.van().Recv(msg, &recv_bytes));
//     manager_.net_usage().IncrRecv(msg->sender, recv_bytes);

//     if (msg->task.task_size()) {
//       // packed task
//       CHECK(!msg->has_data());
//       for (int i = 0; i < msg->task.task_size(); ++i) {
//         Message* unpack_msg = new Message();
//         unpack_msg->recver = msg->recver;
//         unpack_msg->sender = msg->sender;
//         unpack_msg->task = msg->task.task(i);
//         if (!Process(unpack_msg)) break;
//       }
//       delete msg;
//     } else {
//       if (!Process(msg)) break;
//     }
//   }
// }
