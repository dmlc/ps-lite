#include "ps.h"
using Val = float;
using Key = ps::Key;

struct MyVal {
  std::vector<Val> w;
  inline void Load(dmlc::Stream *fi) { fi->Read(&w); }
  inline void Save(dmlc::Stream *fo) const { fo->Write(w); }
  inline bool Empty() const { return w.empty(); }
};

class MyHandle {
 public:
  void Start(bool push, int timestamp, int cmd, void* msg) {
    ps::Message *m = (ps::Message*) msg;
    std::cout << "-------\naccepts " << (push ? "push" : "pull") << " from " << m->sender
              << " with timestamp " << timestamp
              << " and command " << cmd
              << std::endl;
    ts_ = timestamp;
  }

  void Finish() {
    std::cout << "finished timestamp " << ts_
              << "\n-------" << std::endl;
  }

  void Push(Key recv_key, ps::Blob<const Val> recv_val, MyVal& my_val) {
    size_t n = recv_val.size;
    auto& w = my_val.w;
    if (w.empty()) w.resize(n);
    for (size_t i = 0; i < n; ++i) w[i] += recv_val[i];

    std::cout << "handle push: key " << recv_key << ", val " << recv_val << std::endl;
  }

  void Pull(Key recv_key, MyVal& my_val, ps::Blob<Val>& send_val) {
    send_val.data = my_val.w.data();
    send_val.size = my_val.w.size();

    std::cout << "handle pull: key " << recv_key << std::endl;
  }

  inline void Load(dmlc::Stream *fi) { }
  inline void Save(dmlc::Stream *fo) const { }
 private:
  int ts_ = 0;
};

int CreateServerNode(int argc, char *argv[]) {
  using Server = ps::OnlineServer<Val, MyVal, MyHandle>;
  Server server;
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
