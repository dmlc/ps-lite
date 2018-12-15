#include <chrono>
#include <cmath>
#include "ps/ps.h"

using namespace ps;
std::unordered_map<int, KVPairs<float> > mem_map;

template <typename Val>
void EmptyHandler(const KVMeta &req_meta, const KVPairs<Val> &req_data, KVServer<Val> *server) {
  if (req_meta.push) {
    KVPairs<float> res;
    server->Response(req_meta, res);
  } else {
    auto iter = mem_map.find(0);
    if(iter==mem_map.end()){
      LOG(INFO) << "init...";
      KVPairs<float> res;
      res.keys = req_data.keys;
      res.vals.resize(req_data.keys.size());
      server->Response(req_meta, res);
    }else{
      LOG(INFO) << "in-place memory reuse";
      KVPairs<float> *res_ptr = &iter->second;
      res_ptr->keys = req_data.keys;
      res_ptr->vals.resize(req_data.keys.size());
      server->Response(req_meta, *res_ptr);
    }
  }
}

void StartServer() {
  if (!IsServer()) return;
  auto server = new KVServer<float>(0);
  server->set_request_handle(EmptyHandler<float>);
  RegisterExitCallback([server]() { delete server; });
}

void RunWorker() {
  if (!IsWorker()) return;
  KVWorker<float> kv(0, 0);

  // init
  int num = 10000000;
  std::vector<Key> keys(num);
  std::vector<float> vals(num);

  int rank = MyRank();
  srand(rank + 7);
  for (int i = 0; i < num; ++i) {
    keys[i] = kMaxKey / num * i + rank;
    vals[i] = (rand() % 1000);
  }

  int repeat = 1;

  // push
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < repeat; ++i) {
    kv.Wait(kv.Push(keys, vals));
  }
  auto end = std::chrono::high_resolution_clock::now();
  LL << "num = " << num << ", Push average time cost: " << (end - start).count() / 1e6 << "ms";

  // pull
  start = std::chrono::high_resolution_clock::now();
  std::vector<float> rets;
  for (int i = 0; i < repeat; ++i) {
    kv.Wait(kv.Pull(keys, &rets));
  }
  end = std::chrono::high_resolution_clock::now();
  LL << "num = " << num << ", Pull average time cost: " << (end - start).count() / 1e6 << "ms";

}

int main(int argc, char *argv[]) {
  // start system
  Start(0);
  // setup server nodes
  StartServer();
  // run worker nodes
  RunWorker();
  // stop system
  Finalize(0, true);
  return 0;
}
