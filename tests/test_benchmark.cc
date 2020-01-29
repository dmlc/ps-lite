#include <chrono>
#include <cmath>
#include <cstdlib>
#include <unistd.h>
#include "ps/ps.h"

#define DIVUP(x, y) (((x)+(y)-1)/(y))
#define ROUNDUP(x, y) (DIVUP((x), (y))*(y))
#define DEBUG_PRINT_TENSOR_VALUE(X) (*((float *)(X) + 0))
#define DEBUG_PRINT_TENSOR_ADDRESS(X) (reinterpret_cast<uint64_t>(X))

using namespace ps;

enum MODE {
    PUSH_THEN_PULL = 0,
    PUSH_PULL = 1,
    PUSH_ONLY = 2, 
    PULL_ONLY = 3
};
std::unordered_map<uint64_t, KVPairs<char> > mem_map;
bool debug_mode_ = false;

void aligned_memory_alloc(void** ptr, size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  void* p;
  int size_aligned = ROUNDUP(size, page_size);
  int ret = posix_memalign(&p, page_size, size_aligned);
  CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
  CHECK(p);
  memset(p, 1, size);
  *ptr = p;
}

void float_sum(float *dst, float *src, size_t len) {
  if (len == 0) return;
  for (size_t i = 0; i < len / (size_t) sizeof(float); ++i) {
    dst[i] = dst[i] + src[i];
  }
}

template <typename Val>
void EmptyHandler(const KVMeta &req_meta, const KVPairs<Val> &req_data, KVServer<Val> *server) {
  uint64_t key = req_data.keys[0];
  if (req_meta.push) {
    CHECK(req_data.lens.size());
    CHECK_EQ(req_data.vals.size(), (size_t)req_data.lens[0]) 
        << "key=" << key << ", " << req_data.vals.size() << ", " << req_data.lens[0];


    if (mem_map.find(key) == mem_map.end()) {
      size_t len = (size_t) req_data.vals.size();

      void* ptr_val;
      aligned_memory_alloc(&ptr_val, len);  
      mem_map[key].vals.reset((char*)ptr_val, len, [](void *){ });

      void* ptr_key;
      aligned_memory_alloc(&ptr_key, sizeof(Key));  
      mem_map[key].keys.reset((Key*)ptr_key, 1, [](void *){ });
      memcpy(ptr_key, &key, sizeof(Key));

      void* ptr_len;
      aligned_memory_alloc(&ptr_len, sizeof(int));  
      mem_map[key].lens.reset((int*)ptr_len, 1, [](void *){ });
      memcpy(ptr_len, &len, sizeof(int));
    }

    auto recved = reinterpret_cast<char*>(req_data.vals.data());
    // only sum the first 4 bytes
    size_t sum_len = debug_mode_ ? req_data.vals.size() : 0;
    float_sum((float*) mem_map[key].vals.data(), (float*) recved, sum_len);

    if (debug_mode_) {
      LOG(INFO) << "recved tensor! key=" << key << "\t"
          << "store: " << DEBUG_PRINT_TENSOR_VALUE(mem_map[key].vals.data()) << "\t"
          << "recv: " << DEBUG_PRINT_TENSOR_VALUE(recved) << "\t"
          << "address: " << DEBUG_PRINT_TENSOR_ADDRESS(recved) << "\t"
          << "len: " << req_data.vals.size() << "\t"
          << "sender: " << req_meta.sender;
    }

    // send push response (empty)
    KVPairs<char> res;
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
  debug_mode_ = Environment::Get()->find("DEBUG_MODE") ? true : false;

  auto server = new KVServer<char>(0);
  server->set_request_handle(EmptyHandler<char>);
  RegisterExitCallback([server]() { delete server; });
}

void push_pull(KVWorker<char> &kv, 
               std::vector<SArray<Key> > &server_keys,
               std::vector<SArray<char> > &server_vals, 
               std::vector<SArray<int> > &server_lens,
               int len, int num_servers, int total_key_num, 
               int how_many_key_per_server, MODE mode) {
  CHECK_GT(mode, 0);
  switch (mode) {
    case PUSH_PULL: 
      LOG(INFO) << "========= PUSH_PULL mode =========";
      LOG(INFO) << "========= msg_size=" << len*sizeof(char) << " bytes =========";
      break;
    case PUSH_ONLY: 
      LOG(INFO) << "========= PUSH_ONLY mode =========";
      LOG(INFO) << "========= msg_size=" << len*sizeof(char) << " bytes =========";
       break;
    case PULL_ONLY: 
      LOG(INFO) << "========= PULL_ONLY mode =========";
      LOG(INFO) << "========= msg_size=" << len*sizeof(char) << " bytes =========";
      break;
    default: CHECK(0);
  }

  std::vector<int> timestamp_list;
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  
  auto val = Environment::Get()->find("LOG_DURATION");
  unsigned int log_duration = val ? atoi(val) : 10;
  
  int cnt = 0;
  while (1) {
    for (int key = 0; key < total_key_num; key++) {
      auto keys = server_keys[key];
      auto lens = server_lens[key];
      auto vals = server_vals[key];

      switch (mode) {
        case PUSH_PULL: {
          timestamp_list.push_back(kv.ZPush(keys, vals, lens));
          timestamp_list.push_back(kv.ZPull(keys, &vals, &lens));
        } break;
        case PUSH_ONLY: {
          timestamp_list.push_back(kv.ZPush(keys, vals, lens));
        } break;
        case PULL_ONLY: {
          timestamp_list.push_back(kv.ZPull(keys, &vals, &lens));
        } break;
        default: {
          CHECK(0);
          break;
        } 
      }
    }

    for (auto& ts : timestamp_list) { kv.Wait(ts); }
    timestamp_list.clear();
    
    cnt++;
    if (cnt % log_duration != 0) continue;

    end = std::chrono::high_resolution_clock::now();
    LL << "Application goodput: " 
        << 8.0 * len * sizeof(char) * total_key_num * cnt / (end - start).count() 
        << " Gbps";
    cnt = 0;
    start = std::chrono::high_resolution_clock::now();
  }
}

void RunWorker(int argc, char *argv[]) {
  if (!IsWorker()) return;
  KVWorker<char> kv(0, 0);
  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();

  const int num_servers = krs.size();
  LOG(INFO) << num_servers << " servers in total";
  CHECK_GT(num_servers, 0);

  // init
  int len = (argc > 1) ? atoi(argv[1]) : 1024000;
  int repeat = (argc > 2) ? atoi(argv[2]) : 10;
  MODE mode = (argc > 3) ? static_cast<MODE>(atoi(argv[3])) : PUSH_PULL;

  auto v = Environment::Get()->find("NUM_KEY_PER_SERVER");
  const int how_many_key_per_server = v ? atoi(v) : 40;
  const int total_key_num = num_servers * how_many_key_per_server;

  std::vector<SArray<char> > server_vals;
  std::vector<SArray<Key> > server_keys;
  std::vector<SArray<int> > server_lens;
  for (int key = 0; key < total_key_num; key++) {
    void* ptr;
    aligned_memory_alloc(&ptr, len);
    SArray<char> vals;
    vals.reset((char*) ptr, len * sizeof(char), [](void *){});
    server_vals.push_back(vals);
  }

  // init push, do not count this into time cost
  for (int key = 0; key < total_key_num; key++) {
    int server = key % num_servers;
    PS_VLOG(1) << "key=" << key << " assigned to server " << server;

    auto vals = server_vals[key];

    // page aligned keys
    void* ptr_key;
    aligned_memory_alloc(&ptr_key, sizeof(Key));
    SArray<Key> keys;
    keys.reset((Key*) ptr_key, 1, [](void *){});
    ps::Key ps_key = krs[server].begin() + key;
    memcpy(ptr_key, &ps_key, sizeof(Key));
    server_keys.push_back(keys);

    // page aligned vals
    void* ptr_len;
    aligned_memory_alloc(&ptr_len, sizeof(int));
    SArray<int> lens;
    lens.reset((int*) ptr_len, 1, [](void *){});
    memcpy(ptr_len, &len, sizeof(len));
    server_lens.push_back(lens);

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
          auto keys = server_keys[server];
          auto lens = server_lens[server];
          auto vals = server_vals[server];

          kv.Wait(kv.ZPush(keys, vals, lens));
        }
        auto end = std::chrono::high_resolution_clock::now();
        accumulated_ms += (end - start).count(); // ns
      }
      LL << "push " << len * sizeof(char)
          << " bytes to each server, repeat=" << repeat
          << ", total_time="
          << accumulated_ms / 1e6 << "ms";

      // pull
      accumulated_ms = 0;
      for (int i = 0; i < repeat; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        for (int server = 0; server < num_servers; server++) {
          auto keys = server_keys[server];
          auto lens = server_lens[server];
          auto vals = server_vals[server];

          kv.Wait(kv.ZPull(keys, &vals, &lens));
        }
        auto end = std::chrono::high_resolution_clock::now();
        accumulated_ms += (end - start).count(); // ns
      }

      LL << "pull " << len * sizeof(char)
          << " bytes to each server, repeat=" << repeat
          << ", total_time="
          << accumulated_ms / 1e6 << "ms";
    } break;
    case PUSH_PULL: 
    case PUSH_ONLY: 
    case PULL_ONLY: 
      push_pull(kv, server_keys, server_vals, server_lens, len, num_servers, total_key_num, how_many_key_per_server, mode);
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