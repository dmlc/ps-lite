#include <chrono>
#include <cmath>
#include <cstdlib>
#include "ps/ps.h"

using namespace ps;

enum MODE {
    PUSH_THEN_PULL = 0,
    PUSH_PULL_MIX_ENDLESS = 1
};
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
    LOG(INFO) << "pull key=" << (int)iter->second.keys[0]
              << ", val_len=" << iter->second.vals.size()
              << ", len=" << (int)iter->second.lens[0];

    server->Response(req_meta, iter->second);
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
  CHECK_EQ(argc, 4) << "input argument should be: [SCRIPT, LEN, REPEAT, MODE]";
  KVWorker<float> kv(0, 0);

  // init
  int len = atoi(argv[1]);
  int repeat = atoi(argv[2]);
  MODE mode = static_cast<MODE>(atoi(argv[3]));

  std::vector<float> vec(len);
  SArray<Key> keys;
  keys.push_back(0);
  SArray<int> lens;
  lens.push_back(len);
  SArray<float> vals(vec);

  // init push, to register memory, better not count this into time cost
  kv.Wait(kv.ZPush(keys, vals, lens));

  switch(mode) {
    case PUSH_THEN_PULL: {
        LOG(INFO) << "PUSH_THEN_PULL mode";
        // push
        uint64_t accumulated_ms = 0;
        for (int i = 0; i < repeat; ++i) {
          auto start = std::chrono::high_resolution_clock::now();
          kv.Wait(kv.ZPush(keys, vals, lens));
          auto end = std::chrono::high_resolution_clock::now();
          accumulated_ms += (end - start).count(); // ns
        }
        LL << "push_byte=" << len * sizeof(float)
           << ", repeat=" << repeat
           << ", total_time="
           << accumulated_ms / 1e6 << "ms";

        // pull
        accumulated_ms = 0;
        for (int i = 0; i < repeat; ++i) {
          auto start = std::chrono::high_resolution_clock::now();
          kv.Wait(kv.ZPull(keys, &vals, &lens));
          auto end = std::chrono::high_resolution_clock::now();
          accumulated_ms += (end - start).count(); // ns
        }

        LL << "pull_byte=" << len * sizeof(float)
           << ", repeat=" << repeat
           << ", total_time="
           << accumulated_ms / 1e6 << "ms";
      }
      break;

    case PUSH_PULL_MIX_ENDLESS: {
        LOG(INFO) << "PUSH_PULL_MIX_ENDLESS mode, should exit by Ctrl+C";
        std::vector<int> timestamp_list;
        while (1) {
          timestamp_list.push_back(kv.ZPush(keys, vals, lens));
          timestamp_list.push_back(kv.ZPull(keys, &vals, &lens));
          if (timestamp_list.size()==20) { // flow control
            for (auto& ts : timestamp_list) {
              kv.Wait(ts);
            }
            timestamp_list.clear();
          }
        }
      }
      break;

    default:
      CHECK(0) << "unknown mode " << mode;
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
