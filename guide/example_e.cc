#include "ps.h"

typedef float Val;

namespace ps {
DECLARE_int32(num_workers);
class MyHandle : ps::IOnlineHandle<Val, Val> {
 public:
  void SetCaller(void *obj) { obj_ = (Customer*)obj; }

  void Start(bool push, int timestamp, int cmd, void* msg) {
    Message *m = (Message*) msg;
    std::cout << "accept " << (push ? "push" : "pull") << " from " << m->sender
              << " with timestamp " << timestamp
              << " and command " << cmd
              << std::endl;
    ts_ = timestamp;
  }

  void Finish() {
    std::cout << "finished " << obj_->NumDoneReceivedRequest(ts_, kWorkerGroup)
              << " / " << FLAGS_num_workers << " on timestamp " << ts_ << std::endl;
  }

  void Init(Key key, Val& val) {
    val = 1;
    std::cout << "init (" << key << ", " << val << ") " << std::endl;
  }

  void Push(Key recv_key, Blob<const Val> recv_val, Val& my_val) {
    for (const Val& v : recv_val) my_val += v;
    std::cout << "handle push: key " << recv_key << " val " << recv_val << std::endl;
  }

  void Pull(Key recv_key, const Val& my_val, Blob<Val>& send_val) {
    for (Val& v : send_val) v = my_val;
    std::cout << "handle pull: key " << recv_key << std::endl;
  }
 private:
  Customer* obj_ = nullptr;
  int ts_ = 0;
};
}

int CreateServerNode(int argc, char *argv[]) {
  ps::OnlineServer<Val, Val, ps::MyHandle> server;
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;
  std::vector<Key> key = {1,    3,    5};
  std::vector<Val> val = {1, 2, 3, 4, 5, 6};
  std::vector<Val> recv_val;

  KVWorker<Val> wk;
  int ts = wk.Push(key, val);
  wk.Wait(ts);

  ts = wk.Pull(key, &recv_val);
  wk.Wait(ts);

  return 0;
}
