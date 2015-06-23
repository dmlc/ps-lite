#include "ps.h"
using Val = float;
using Key = ps::Key;

using MyVal = std::vector<Val>;

class MyHandle {
 public:
  void SetCaller(void *obj) { obj_ = (ps::Customer*)obj; }

  void Start(bool push, int timestamp, int cmd, void* msg) {
    ps::Message *m = (ps::Message*) msg;
    std::cout << "-------\naccepts " << (push ? "push" : "pull") << " from " << m->sender
              << " with timestamp " << timestamp
              << " and command " << cmd
              << std::endl;
    ts_ = timestamp;
  }

  void Finish() {
    std::cout << "finished " << obj_->NumDoneReceivedRequest(ts_, ps::kWorkerGroup)
              << " / " << ps::NumWorkers() << " on timestamp " << ts_
              << "\n-------" << std::endl;
  }

  void Init(Key key, MyVal& val) {
    std::cout << "init key" << key << std::endl;
  }

  void Push(Key recv_key, ps::Blob<const Val> recv_val, MyVal& my_val) {
    size_t n = recv_val.size;
    if (my_val.empty()) my_val.resize(n);
    CHECK_EQ(my_val.size(), n);
    for (size_t i = 0; i < n; ++i) my_val[i] += recv_val[i];

    std::cout << "handle push: key " << recv_key << ", val " << recv_val << std::endl;
  }

  void Pull(Key recv_key, MyVal& my_val, ps::Blob<Val>& send_val) {
    send_val.data = my_val.data();
    send_val.size = my_val.size();

    std::cout << "handle pull: key " << recv_key << std::endl;
  }
 private:
  ps::Customer* obj_ = nullptr;
  int ts_ = 0;
};

int CreateServerNode(int argc, char *argv[]) {
  using Server = ps::OnlineServer<MyVal, Val, MyHandle>;
  Server server(MyHandle());
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;
  KVWorker<Val> wk;
  std::vector<Key> key = {1, 3,       8    };
  std::vector<Val> val = {1, 3, 4, 5, 9, 10};
  std::vector<int> siz = {1, 3,       2    };

  std::vector<Val> recv_val;
  std::vector<int> recv_siz;

  wk.Wait(wk.VPush(key, val, siz));
  wk.Wait(wk.VPull(key, &recv_val, &recv_siz));

  std::cout << "values pulled at " << MyNodeID() << ": "
            << Blob<const Val>(recv_val) << "\n"
            << Blob<const int>(recv_siz) << std::endl;
  return 0;
}
