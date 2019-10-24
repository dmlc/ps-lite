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
    server->Response(req_meta, iter->second);
  }
}

void StartServer() {
  if (!IsServer()) return;
  auto server = new KVServer<float>(0);
  server->set_request_handle(EmptyHandler<float>);
  RegisterExitCallback([server]() { delete server; });
}

struct PSKV {
  SArray<ps::Key> keys;  // n keys
  SArray<int> lens;      // the length of the i-th value
};
std::unordered_map<uint64_t, PSKV> ps_kv_;

void RunWorker(int argc, char *argv[]) {
  if (!IsWorker()) return;
  CHECK_GE(argc, 3) << "input argument should be at least 3: SCRIPT, LEN, REPEAT, (OPTIONAL) MODE";
  KVWorker<float> kv(0, 0);
  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();

  const int num_servers = krs.size();
  LOG(INFO) << num_servers << " servers in total";
  CHECK_GT(num_servers, 0);

  // init
  int len = atoi(argv[1]);
  int repeat = atoi(argv[2]);
  MODE mode = (argc > 3) ? static_cast<MODE>(atoi(argv[3])) : PUSH_PULL_MIX_ENDLESS;

  std::vector<SArray<float> > server_vals;
  for (int server = 0; server < num_servers; server++) {
    std::vector<float> vec(len);
    SArray<float> vals(vec);
    server_vals.push_back(vals);
  }

  // init push, do not count this into time cos
  for (int server = 0; server < num_servers; server++) {
    int key = server; // could be other value
    auto vals = server_vals[server];
    PSKV& pskv = ps_kv_[key];
    SArray<Key> keys;
    ps::Key ps_key = krs[server].begin() + key;
    keys.push_back(ps_key);
    SArray<int> lens;
    lens.push_back(len);
    pskv.keys.push_back(ps_key);
    pskv.lens.push_back(len);

    kv.Wait(kv.ZPush(keys, vals, lens));
  }

  switch(mode) {
    case PUSH_THEN_PULL: {
        LOG(INFO) << "PUSH_THEN_PULL mode";
        // push
        uint64_t accumulated_ms = 0;
        for (int i = 0; i < repeat; ++i) {
          auto start = std::chrono::high_resolution_clock::now();
          for (int server = 0; server < num_servers; server++) {
            int key = server;
            PSKV& pskv = ps_kv_[key];
            auto keys = pskv.keys;
            auto lens = pskv.lens;
            auto vals = server_vals[server];

            kv.Wait(kv.ZPush(keys, vals, lens));
          }
          auto end = std::chrono::high_resolution_clock::now();
          accumulated_ms += (end - start).count(); // ns
        }
        LL << "push " << len * sizeof(float)
           << " bytes to each server, repeat=" << repeat
           << ", total_time="
           << accumulated_ms / 1e6 << "ms";

        // pull
        accumulated_ms = 0;
        for (int i = 0; i < repeat; ++i) {
          auto start = std::chrono::high_resolution_clock::now();
          for (int server = 0; server < num_servers; server++) {
            int key = server;
            PSKV& pskv = ps_kv_[key];
            auto keys = pskv.keys;
            auto lens = pskv.lens;
            auto vals = server_vals[server];

            kv.Wait(kv.ZPull(keys, &vals, &lens));
          }
          auto end = std::chrono::high_resolution_clock::now();
          accumulated_ms += (end - start).count(); // ns
        }

        LL << "pull " << len * sizeof(float)
           << " bytes to each server, repeat=" << repeat
           << ", total_time="
           << accumulated_ms / 1e6 << "ms";
      }
      break;

    case PUSH_PULL_MIX_ENDLESS: {
        LOG(INFO) << "PUSH_PULL_MIX_ENDLESS mode, should exit by Ctrl+C";
        std::vector<int> timestamp_list;
        auto start = std::chrono::high_resolution_clock::now();
        auto end = std::chrono::high_resolution_clock::now();
        auto val = Environment::Get()->find("THRESHOLD");
        unsigned int threshold = val ? atoi(val) : 10;
        int cnt = 0;
        while (1) {
          for (int server = 0; server < num_servers; server++) {
            int key = server;
            PSKV& pskv = ps_kv_[key];
            auto keys = pskv.keys;
            auto lens = pskv.lens;
            auto vals = server_vals[server];

            timestamp_list.push_back(kv.ZPush(keys, vals, lens));
            timestamp_list.push_back(kv.ZPull(keys, &vals, &lens));
          }
          if (timestamp_list.size()/2/num_servers >= threshold) { // flow control
            for (auto& ts : timestamp_list) {
              kv.Wait(ts);
            }
            timestamp_list.clear();
            cnt++;
            if (cnt % 100 == 0) {
              end = std::chrono::high_resolution_clock::now();
              LL << "Benchmark throughput: " 
                 << 8.0 * len * sizeof(float) * num_servers * cnt * threshold / (end - start).count() 
                 << " Gbps";
              cnt = 0;
              start = std::chrono::high_resolution_clock::now();
            }
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
