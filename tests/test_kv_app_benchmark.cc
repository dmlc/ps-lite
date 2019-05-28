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

void RunWorker(int argc, char *argv[]) {
  if (!IsWorker()) return;
  KVWorker<float> kv(0, 0);

  // init
  int len = (argc >= 2 ? atoi(argv[1]) : 10000000);
  std::vector<float> vec(len);

  SArray<Key> keys(1);
  keys.push_back(0);

  SArray<int> lens(1);
  lens.push_back(len);

  SArray<float> vals(vec);

  int repeat = (argc >= 3 ? atoi(argv[2]) : 10);

  // init push, to register memory, better not count this into time cost
  kv.Wait(kv.ZPush(keys, vals, lens));

  // push
  std::vector<int> ts_list;
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < repeat; ++i) {
    ts_list.push_back(kv.ZPush(keys, vals, lens));
  }
  for (auto& ts : ts_list) {
    kv.Wait(ts);
  }
  auto end = std::chrono::high_resolution_clock::now();

  auto duration = (end - start).count(); // nano second
  LL << "push_byte=" << len * sizeof(float)
     << ", repeat=" << repeat
     << ", total_time="
     << duration / 1e6
     << "ms, tput="
     << len * 1.0 / duration * 8 * sizeof(float)
     << "Gbps";

}

int main(int argc, char *argv[]) {
  // start system
  Start(0);
  // setup server nodes
  StartServer();
  // run worker nodes
  RunWorker(argc, argv);
  // stop system
  Finalize(0, true);
  return 0;
}
