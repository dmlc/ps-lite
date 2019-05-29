/*
This code only works for 1 worker VS N server
*/
#include <chrono>
#include <cmath>
#include <cstdlib>
#include "ps/ps.h"

using namespace ps;

std::unordered_map<uint64_t, KVPairs<float> > mem_map;

template <typename Val>
void EmptyHandler(const KVMeta &req_meta, const KVPairs<Val> &req_data, KVServer<Val> *server) {
  uint64_t key = req_data.keys[0];
  if (req_meta.push) {
    CHECK(req_data.lens.size());
    CHECK_EQ(req_data.vals.size(), (size_t)req_data.lens[0]);

    if (mem_map.find(key) == mem_map.end()) {
      size_t len = (size_t) req_data.vals.size();
      mem_map[key].keys.push_back(key);
      mem_map[key].vals.CopyFrom(req_data.vals);
      mem_map[key].lens.push_back(len);
    }

    // send push response (empty)
    KVPairs<float> res;
    server->Response(req_meta, res);
  }
  else {
    auto iter = mem_map.find(key);
    CHECK_NE(iter, mem_map.end());
    server->Response(req_meta, iter->second);
  }
}

void StartServer() {
  if (!IsServer()) return;
  int myrank = Postoffice::Get()->my_rank();
  LOG(INFO) << "This is server " << myrank;
  auto server = new KVServer<float>(0);
  server->set_request_handle(EmptyHandler<float>);
  RegisterExitCallback([server]() { delete server; });
}

void RunWorker(int argc, char *argv[]) {
  if (!IsWorker()) return;
  CHECK_EQ(argc, 2) << "input argument should be: [SCRIPT, LEN]";
  KVWorker<float> kv(0, 0);

  auto krs = Postoffice::Get()->GetServerKeyRanges();
  const int num_servers = krs.size();
  CHECK_GT(num_servers, 0);

  // init
  int len = atoi(argv[1]);

  std::vector<float> vec(len);

  std::vector<SArray<Key> >   keys(num_servers);
  std::vector<SArray<int> >   lens(num_servers);
  std::vector<SArray<float> > vals;

  for (int i = 0; i < num_servers ; ++i) {
    int key = i;
    int server = (key * 9973) % num_servers;
    ps::Key ps_key = krs[server].begin() + key;
    CHECK_LT(ps_key, krs[server].end());

    SArray<float> tmp_vals(vec);

    keys[i].push_back(ps_key);
    lens[i].push_back(len);
    vals.push_back(tmp_vals);

    // init push, to register memory, better not count this into time cost
    kv.Wait(kv.ZPush(keys[i], vals[i], lens[i]));
  }

  std::vector<int> timestamp_list;
  while (1) {
    for (int j = 0; j < num_servers; ++j) {
      timestamp_list.push_back(kv.ZPush(keys[j], vals[j], lens[j]));
      timestamp_list.push_back(kv.ZPull(keys[j], &vals[j], &lens[j]));
    }
    if (timestamp_list.size() >= 30) { // flow control
      for (auto& ts : timestamp_list) {
        kv.Wait(ts);
      }
      timestamp_list.clear();
    }
  }
}

int main(int argc, char *argv[]) {
  // disable multi-threaded processing first
  setenv("ENABLE_SERVER_MULTIPULL", "0", 1);
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
