#include <chrono>
#include <cmath>
#include <cstdlib>
#include <fcntl.h>
#include <numa.h>
#include <unistd.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include "ps/ps.h"
#define DATA_TYPE char
using namespace ps;

enum MODE {
    PUSH_THEN_PULL = 0,
    PUSH_PULL_MIX_ENDLESS = 1
};
std::unordered_map<uint64_t, KVPairs<DATA_TYPE> > mem_map;
std::unordered_map<std::string, void *> _key_shm_addr;
std::unordered_map<std::string, size_t> _key_shm_size;
std::unordered_map<uint64_t, char*> store_;
std::mutex mu_;

void* OpenSharedMemory(const std::string& prefix,
                                           uint64_t key, size_t size) {
  std::string shm_name(prefix);
  shm_name += std::to_string(key);
  int shm_fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0666);
  CHECK_GE(shm_fd, 0) << "shm_open failed for " << shm_name;
  CHECK_GE(ftruncate(shm_fd, size), 0) << strerror(errno);

  void* ptr = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
  CHECK_NE(ptr, (void*)-1) << strerror(errno);

  LOG(INFO) << "initialized share memory size=" << size 
            << " for key=" << key << ", name=" << shm_name;
  _key_shm_addr[shm_name] = ptr;
  _key_shm_size[shm_name] = size;
  return ptr;
}

uint64_t DecodeKey(ps::Key key) {
  auto kr = ps::Postoffice::Get()->GetServerKeyRanges()[ps::MyRank()];
  return key - kr.begin();
}

template <typename Val>
void EmptyHandler(const KVMeta &req_meta, const KVPairs<Val> &req_data, KVServer<Val> *server) {
  std::lock_guard<std::mutex> lk(mu_);
  uint64_t key = DecodeKey(req_data.keys[0]);
  if (req_meta.push) {
    CHECK(req_data.lens.size());
    CHECK_EQ(req_data.vals.size(), (size_t)req_data.lens[0]);

    if (mem_map.find(key) == mem_map.end()) {
      PS_VLOG(1) << "key " << key << " from worker-" << req_meta.sender;
      size_t len = (size_t) req_data.vals.size();
      mem_map[key].keys.push_back(key);
      mem_map[key].lens.push_back(len);

      store_[key] = (char*) malloc(len);
      mem_map[key].vals = ps::SArray<char>(store_[key], len, false);
    }

    // send push response (empty)
    KVPairs<DATA_TYPE> res;
    server->Response(req_meta, res);
  } else { // pull request
    auto iter = mem_map.find(key);
    CHECK_NE(iter, mem_map.end());
    server->Response(req_meta, iter->second);
  }
}

void StartServer() {
  if (!IsServer()) return;
  auto server = new KVServer<DATA_TYPE>(0);
  server->set_request_handle(EmptyHandler<DATA_TYPE>);
  RegisterExitCallback([server]() { delete server; });
}

struct PSKV {
  SArray<ps::Key> keys;  // n keys
  SArray<int> lens;      // the length of the i-th value
};
std::unordered_map<uint64_t, PSKV> ps_kv_;

uint64_t EncodeKey(uint64_t seed) {
  return seed << 16;
}

void RunWorker(int argc, char *argv[]) {
  if (!IsWorker()) return;
  CHECK_GE(argc, 3) << "input argument should be at least 3: SCRIPT, LEN, REPEAT, (OPTIONAL) MODE";
  KVWorker<DATA_TYPE> kv(0, 0);
  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();

  const int num_servers = krs.size();
  LOG(INFO) << num_servers << " servers in total";
  CHECK_GT(num_servers, 0);

  // init
  auto val = Environment::Get()->find("BYTEPS_PARTITION_BYTES");
  unsigned int partition_bytes = val ? atoi(val) : 4096000;
  int len = atoi(argv[1]);
  CHECK_GE(partition_bytes, len) 
      << "tensor partition is not supported in this benchmark"
      << ", try reduce tensor size or increase BYTEPS_PARTITION_BYTES";
  int repeat = atoi(argv[2]);
  MODE mode = (argc > 3) ? static_cast<MODE>(atoi(argv[3])) : PUSH_PULL_MIX_ENDLESS;

  std::vector<SArray<DATA_TYPE> > server_vals;
  for (int server = 0; server < num_servers; server++) {
    auto key = EncodeKey(server); 
    auto addr = (char*) OpenSharedMemory(std::string("BytePS_ShM_"), key, len);
    SArray<DATA_TYPE> vals(addr, len, false);
    server_vals.push_back(vals);
  }

  // init broadcast
  for (int server = 0; server < num_servers; server++) {
    auto key = EncodeKey(server); 
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
            auto key = EncodeKey(server);
            PSKV& pskv = ps_kv_[key];
            auto keys = pskv.keys;
            auto lens = pskv.lens;
            auto vals = server_vals[server];

            kv.Wait(kv.ZPush(keys, vals, lens));
          }
          auto end = std::chrono::high_resolution_clock::now();
          accumulated_ms += (end - start).count(); // ns
        }
        LL << "push " << len * sizeof(DATA_TYPE)
           << " bytes to each server, repeat=" << repeat
           << ", total_time="
           << accumulated_ms / 1e6 << "ms";

        // pull
        accumulated_ms = 0;
        for (int i = 0; i < repeat; ++i) {
          auto start = std::chrono::high_resolution_clock::now();
          for (int server = 0; server < num_servers; server++) {
            auto key = EncodeKey(server);
            PSKV& pskv = ps_kv_[key];
            auto keys = pskv.keys;
            auto lens = pskv.lens;
            auto vals = server_vals[server];

            kv.Wait(kv.ZPull(keys, &vals, &lens));
          }
          auto end = std::chrono::high_resolution_clock::now();
          accumulated_ms += (end - start).count(); // ns
        }

        LL << "pull " << len * sizeof(DATA_TYPE)
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
        val = Environment::Get()->find("LOG_DURATION");
        unsigned int log_duration = val ? atoi(val) : 50;
        int cnt = 0;
        while (1) {
          for (int server = 0; server < num_servers; server++) {
            auto key = EncodeKey(server);
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
            if (cnt % log_duration == 0) {
              end = std::chrono::high_resolution_clock::now();
              LL << "Application goodput: " 
                 << 8.0 * len * sizeof(DATA_TYPE) * num_servers * cnt * threshold / (end - start).count() 
                 << " Gbps";
              cnt = 0;
              start = std::chrono::high_resolution_clock::now();
            }
          }
        }
      } break;
    default:
      CHECK(0) << "unknown mode " << mode;
  }
}

int main(int argc, char *argv[]) {
  // disable multi-threaded processing first
  setenv("ENABLE_SERVER_MULTIPULL", "0", 1);
  setenv("BYTEPS_LOCAL_SIZE", "1", 1);
  setenv("BYTEPS_ENABLE_IPC", "1", 0);
  // start system
  Start(0);
  // setup server nodes
  StartServer();
  // run worker nodes
  RunWorker(argc, argv);
  // stop system
  Finalize(0, true);    
  // release shm
  for (auto &it : _key_shm_addr) {
    munmap(it.second, _key_shm_size[it.first]);
    shm_unlink(it.first.c_str());
  }
  return 0;
}
