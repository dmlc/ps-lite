#include <chrono>
#include <cmath>
#include <thread>
#include <cstdlib>
#include <unistd.h>
#include "ps/ps.h"

#if DMLC_USE_CUDA
#include <cuda_runtime.h>
#define CUDA_CALL(func)                                      \
  {                                                          \
    cudaError_t e = (func);                                  \
    CHECK(e == cudaSuccess || e == cudaErrorCudartUnloading) \
        << "CUDA: " << cudaGetErrorString(e);                \
  }
#endif

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

// A map for the registered buffers
std::unordered_map<int64_t, std::unordered_map<ps::Key, SArray<char>>> registered_buffs;

bool debug_mode_ = false;
int num_ports = 1;
bool enable_recv_buffer = false;
int local_size = 0;
bool enable_cpu = 0;
bool skip_dev_id_check = false;
int num_gpu_server = 0;
bool enable_cpu_server = 0;
bool is_server = false;

bool env2bool(const char* var, bool default_val) {
  auto env_str = Environment::Get()->find(var);
  bool val = env_str ? atoi(env_str) != 0 : default_val;
  return val;
}

int env2int(const char* var, int default_val) {
  auto env_str = Environment::Get()->find(var);
  int val = env_str ? atoi(env_str) : default_val;
  return val;
}

// when local_size > 0, we use context CPU(-1), GPU(0), GPU(1), etc
// otherwise, we use CPU(0), CPU(1), etc
int src_key2ctx(int key) {
  int dev_id;
  if (local_size == 0) {
    dev_id = key % num_ports;
  } else {
    if (enable_cpu) {
      int num_devices = local_size + 1;
      dev_id = key % num_devices;
      dev_id -= 1;
    } else {
      dev_id = key % local_size;
    }
  }
  return dev_id;
}

int dst_key2ctx(int key) {
  int dev_id;
  if (num_gpu_server == 0) {
    dev_id = key % num_ports;
  } else {
    if (enable_cpu_server) {
      int num_devices = num_gpu_server + 1;
      dev_id = key % num_devices;
      dev_id -= 1;
    } else {
      dev_id = key % num_gpu_server;
    }
  }
  return dev_id;
}

void aligned_memory_alloc(void** ptr, size_t size, int device_idx, DeviceType device) {
  if (device == CPU) {
    // CPU Alloc
    size_t page_size = sysconf(_SC_PAGESIZE);
    void* p;
    int size_aligned = ROUNDUP(size, page_size);
    int ret = posix_memalign(&p, page_size, size_aligned);
    CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
    CHECK(p);
    memset(p, 1, size);
    *ptr = p;
  } else {
    CHECK(device == GPU);
#if DMLC_USE_CUDA
    // GPU Alloc, malloc should automatically gives page aligned.
    CUDA_CALL(cudaSetDevice(device_idx));
    CUDA_CALL(cudaMalloc(ptr, size));
#else
    CHECK(false) << "Please build with USE_CUDA=1";
#endif
  }
}


void float_sum(float *dst, float *src, size_t len) {
  // TODO: sum floats on the GPU
  return;
  if (len == 0) return;
  for (size_t i = 0; i < len / (size_t) sizeof(float); ++i) {
    dst[i] = dst[i] + src[i];
  }
}

uint64_t DecodeKey(ps::Key key) {
  auto kr = ps::Postoffice::Get()->GetServerKeyRanges()[ps::MyRank()];
  return key - kr.begin();
}

void ErrHandler(void* data, ps::ErrorCode status, std::string reason) {
  LOG(INFO) << "Custom error handler invoked(" << (int) status << "). "
            << "reason: " << reason << ". aborting in 5 seconds";
  std::this_thread::sleep_for(std::chrono::seconds(5));
  abort();
}

template <typename Val>
void EmptyHandler(const KVMeta &req_meta, const KVPairs<Val> &req_data, KVServer<Val> *server) {
  uint64_t key = req_data.keys[0];
  if (req_meta.push) {
    CHECK(req_data.lens.size());
    CHECK_EQ(req_data.vals.size(), (size_t)req_data.lens[0]) 
        << "key=" << key << ", " << req_data.vals.size() << ", " << req_data.lens[0];

    auto key_decoded = DecodeKey(key);
    // check device id.
    if (!skip_dev_id_check) {
      int expected_device_id = dst_key2ctx(key_decoded);
      CHECK_EQ(req_data.vals.dst_device_id_, expected_device_id)
        << "key=" << key_decoded << ", "
        << req_data.vals.dst_device_id_ << " v.s. " << expected_device_id;
    }
    auto recved = reinterpret_cast<char*>(req_data.vals.data());

    if (mem_map.find(key) == mem_map.end()) {
      if (debug_mode_) {
        LOG(INFO) << "recved tensor! key=" << key << "\t" << "not in mem_map";
      }
      size_t len = (size_t) req_data.vals.size();

      void* ptr_val;
      aligned_memory_alloc(&ptr_val, len, -1, CPU);
      mem_map[key].vals.reset((char*)ptr_val, len, [](void *){ });

      void* ptr_key;
      aligned_memory_alloc(&ptr_key, sizeof(Key), -1, CPU);  
      mem_map[key].keys.reset((Key*)ptr_key, 1, [](void *){ });
      memcpy(ptr_key, &key, sizeof(Key));

      void* ptr_len;
      aligned_memory_alloc(&ptr_len, sizeof(int), -1, CPU);
      mem_map[key].lens.reset((int*)ptr_len, 1, [](void *){ });
      memcpy(ptr_len, &len, sizeof(int));
    }
    if (enable_recv_buffer) {
      int worker_id = req_meta.sender;
      int64_t pair_id = server->instance_idx_;
      pair_id = (pair_id << 32) + worker_id;
      CHECK(registered_buffs.find(pair_id) != registered_buffs.end())
        << worker_id << " " << server->instance_idx_ << " " << pair_id;
      auto& buffs = registered_buffs[pair_id];
      CHECK(buffs.find(key_decoded) != buffs.end()) << key_decoded;
      auto registered = buffs[key_decoded].data();
      CHECK(registered == recved) << (long long) registered << " v.s. "
        << (long long) recved << " key=" << key_decoded
        << " sender=" << worker_id << " size=" << req_data.vals.size();
    }
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
  } else {
    auto iter = mem_map.find(key);
    CHECK_NE(iter, mem_map.end());
    server->Response(req_meta, iter->second);
  }
}

void GenerateVals(int total_key_num, int worker_rank,
                  int len, int num_ports,
                  std::vector<SArray<char>>* server_vals) {
  // values are generated on the CPU/GPU depending on the env var
  // We assume LOCAL_SIZE number of GPU contexts
  for (int key = 0; key < total_key_num; key++) {
    void* ptr;
    SArray<char> vals;
    // CPU only
    int src_dev_id = src_key2ctx(key + worker_rank);
    int dst_dev_id = dst_key2ctx(key);

    int dev_id = is_server ? dst_dev_id : src_dev_id;
    int local_gpu_size = is_server ? num_gpu_server : local_size;

    if (local_gpu_size == 0) {
      // Normal all cpu unit test
      aligned_memory_alloc(&ptr, len, dev_id, CPU);
      vals.reset((char*) ptr, len * sizeof(char), [](void *){},
                 CPU, src_dev_id, CPU, dst_dev_id);
    } else {
      DeviceType src_device = src_dev_id < 0 ? CPU : GPU;
      DeviceType dst_device = dst_dev_id < 0 ? CPU : GPU;
      DeviceType dev_type = is_server ? dst_device : src_device;
      aligned_memory_alloc(&ptr, len, dev_id, dev_type);
      vals.reset((char*) ptr, len * sizeof(char), [](void *){},
                 src_device, src_dev_id, dst_device, dst_dev_id);
    }
    server_vals->push_back(vals);
    LOG(INFO) << "Init val[" << key << "]: " << vals.DebugString();
  }
}

void GenerateKeys(int total_key_num, std::vector<SArray<Key>>* server_keys) {
  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();
  const int num_servers = krs.size();
  for (int key = 0; key < total_key_num; key++) {
    int server = key % num_servers;
    // page aligned keys
    void* ptr_key;
    aligned_memory_alloc(&ptr_key, sizeof(Key), -1, CPU);
    SArray<Key> keys;
    keys.reset((Key*) ptr_key, 1, [](void *){});
    ps::Key ps_key = krs[server].begin() + key;
    memcpy(ptr_key, &ps_key, sizeof(Key));
    server_keys->push_back(keys);
    PS_VLOG(1) << "key=" << key << "(" << ps_key << ") assigned to server " << server;
  }
}

void GenerateLens(int total_key_num, int len, std::vector<SArray<int>>* server_lens) {
  for (int key = 0; key < total_key_num; key++) {
    // page aligned lens
    void* ptr_len;
    aligned_memory_alloc(&ptr_len, sizeof(int), -1, CPU);
    SArray<int> lens;
    lens.reset((int*) ptr_len, 1, [](void *){});
    memcpy(ptr_len, &len, sizeof(len));
    server_lens->push_back(lens);
  }
}


void StartServer(int argc, char *argv[], int group_size) {
  if (!IsServer()) return;
  debug_mode_ = Environment::Get()->find("DEBUG_MODE") ? true : false;

  std::vector<KVServer<char>*> servers;
  for (int i = 0; i < group_size; ++i) {
    auto server = new KVServer<char>(0, false, i);
    server->set_request_handle(EmptyHandler<char>);
    servers.push_back(server);
  }

  if (!enable_recv_buffer) return;
  int num_workers = Postoffice::Get()->num_workers();
  int num_servers = Postoffice::Get()->num_servers();
  auto my_rank = ps::Postoffice::Get()->my_rank();
  LOG(INFO) << "Registering buffers for server rank=" << my_rank
            << ", num_servers=" << num_servers;
  auto v = Environment::Get()->find("NUM_KEY_PER_SERVER");
  const int how_many_key_per_server = v ? atoi(v) : 40;
  const int total_key_num = num_servers * how_many_key_per_server;
  int len = (argc > 1) ? atoi(argv[1]) : 1024000;
  // We generate different val buffs for each server instance to avoid conflicts
  for (int instance_idx = 0; instance_idx < group_size; ++instance_idx) {
    auto server = servers[instance_idx];
    for (int worker_rank = 0; worker_rank < num_workers; worker_rank++) {
      std::vector<SArray<char>> server_vals;
      std::vector<SArray<Key>> server_keys;
      std::vector<SArray<int>> server_lens;
      GenerateVals(total_key_num, worker_rank, len, num_ports, &server_vals);
      GenerateKeys(total_key_num, &server_keys);
      GenerateLens(total_key_num, len, &server_lens);
      for (int key = 0; key < total_key_num; ++key) {
        if (my_rank == (key % num_servers)) {
          server->RegisterRecvBufferWithRank(worker_rank, server_keys[key],
                                             server_vals[key], server_lens[key]);
          int64_t pair_id = instance_idx;
          int worker_id = ps::Postoffice::Get()->WorkerRankToID(worker_rank);
          pair_id = (pair_id << 32) + worker_id;
          registered_buffs[pair_id][key] = server_vals[key];

          mem_map[key].keys = server_keys[key];
          mem_map[key].vals = server_vals[key];
          mem_map[key].lens = server_lens[key];

          LOG(INFO) << "Server instance " << instance_idx << " registered buffer for worker_rank="
                    << worker_rank << " key " << key << " ptr "
                    << (long long) server_vals[key].data();
        }
      }
    }
  }
  ps::Postoffice::Get()->Barrier(0, kWorkerGroup + kServerGroup);
}

void push_pull(KVWorker<char>* kv,
               std::vector<SArray<Key> > &server_keys,
               std::vector<SArray<char> > &server_vals, 
               std::vector<SArray<int> > &server_lens,
               int len, int num_servers, int total_key_num, 
               int how_many_key_per_server, MODE mode, int tid) {
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

  auto total_val = Environment::Get()->find("TOTAL_DURATION");
  int total_log_duration = total_val ? atoi(total_val) : 2000000000;
  
  int cnt = 0;
  int total_cnt = 0;
  while (total_cnt < total_log_duration) {
    for (int key = 0; key < total_key_num; key++) {
      auto keys = server_keys[key];
      auto lens = server_lens[key];
      auto vals = server_vals[key];

      switch (mode) {
        case PUSH_PULL: {
          timestamp_list.push_back(kv->ZPush(keys, vals, lens));
          timestamp_list.push_back(kv->ZPull(keys, &vals, &lens));
        } break;
        case PUSH_ONLY: {
          timestamp_list.push_back(kv->ZPush(keys, vals, lens));
        } break;
        case PULL_ONLY: {
          timestamp_list.push_back(kv->ZPull(keys, &vals, &lens));
        } break;
        default: {
          CHECK(0);
          break;
        } 
      }
    }

    for (auto& ts : timestamp_list) { kv->Wait(ts); }
    timestamp_list.clear();
    
    cnt++;
    total_cnt++;
    if (cnt % log_duration != 0) continue;

    end = std::chrono::high_resolution_clock::now();
    auto latency = (end - start).count();

    LL << "[" << tid << "]\tApplication goodput: "
        << 8.0 * len * sizeof(char) * total_key_num * cnt / latency
        << " Gbps.\tAvg latency = " << latency / cnt / total_key_num / 1000.0 << " ns per key";
    cnt = 0;
    start = std::chrono::high_resolution_clock::now();
  }
}

void RunWorker(int argc, char *argv[], KVWorker<char>* kv, int tid) {
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

  auto my_rank = ps::Postoffice::Get()->my_rank();
  std::vector<SArray<char>> server_vals;
  std::vector<SArray<Key>> server_keys;
  std::vector<SArray<int>> server_lens;

  GenerateVals(total_key_num, my_rank, len, num_ports, &server_vals);
  GenerateKeys(total_key_num, &server_keys);
  GenerateLens(total_key_num, len, &server_lens);

  // place a barrier to make sure the server has all the buffers registered.
  if (enable_recv_buffer) {
    ps::Postoffice::Get()->Barrier(0, kWorkerGroup + kServerGroup);
    LOG(INFO) << "Server recv buff registration is DONE.";
  }

  // init push, do not count this into time cost
  for (int key = 0; key < total_key_num; key++) {
    kv->Wait(kv->ZPush(server_keys[key], server_vals[key], server_lens[key]));
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

          kv->Wait(kv->ZPush(keys, vals, lens));
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

          kv->Wait(kv->ZPull(keys, &vals, &lens));
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
      push_pull(kv, server_keys, server_vals, server_lens, len, num_servers, total_key_num,
                how_many_key_per_server, mode, tid);
      break;
    default:
      CHECK(0) << "unknown mode " << mode;
  }
}

int main(int argc, char *argv[]) {
  // disable multi-threaded processing first
  setenv("ENABLE_SERVER_MULTIPULL", "0", 1);
  // init env var options
  num_ports = env2int("DMLC_NUM_PORTS", 1);
  enable_recv_buffer = env2bool("ENABLE_RECV_BUFFER", false);
  skip_dev_id_check = env2bool("SKIP_DEV_ID_CHECK", false);
  // num worker/server env vars
  local_size = env2int("TEST_NUM_GPU_WORKER", 0);
  enable_cpu = env2int("TEST_NUM_CPU_WORKER", 1);
  num_gpu_server = env2int("TEST_NUM_GPU_SERVER", 0);
  enable_cpu_server = env2int("TEST_NUM_CPU_SERVER", 1);

  Van::set_err_handle(ErrHandler);

  // role
  const char* val = CHECK_NOTNULL(Environment::Get()->find("DMLC_ROLE"));
  std::string role_str(val);
  Node::Role role = GetRole(role_str);
  is_server = role_str == std::string("server");
  if (is_server) {
    CHECK(num_gpu_server || enable_cpu_server);
  } else {
    CHECK(local_size || enable_cpu);
  }

  if (role_str == "server" || role_str == "joint") {
    if (enable_recv_buffer) {
      LOG(INFO) << "recv buffer registration is enabled";
    } else {
      LOG(INFO) << "recv buffer registration is NOT enabled";
    }
  }
  LOG(INFO) << "TEST_NUM_GPU_WORKER=" << local_size  << " TEST_NUM_GPU_SERVER="
            << num_gpu_server << " ports per node=" << num_ports;


  // start system
  int my_rank = env2int("DMLC_RANK", -1);
  int group_size = env2int("DMLC_GROUP_SIZE", 1);
  StartPS(0, role, my_rank, true);

  // check rank
  if (my_rank != -1) {
    int assigned_rank = ps::Postoffice::Get()->my_rank();
    CHECK(assigned_rank == my_rank) << assigned_rank << " v.s. " << my_rank;
  }

  // setup server nodes
  StartServer(argc, argv, group_size);
  // run worker nodes
  if (!IsServer() && !IsScheduler()) {
    const int nthread = env2int("BENCHMARK_NTHREAD", 1);
    LOG(INFO) << "number of threads for the same worker = " << nthread;
    std::vector<KVWorker<char>*> kvs;
    std::vector<std::thread> threads;
    for (int i = 0; i < nthread; ++i) {
      KVWorker<char>* kv = new KVWorker<char>(0, 0, i);
      kvs.emplace_back(kv);
      threads.emplace_back(RunWorker, argc, argv, kv, threads.size());
    }
    for (int i = 0; i < nthread; ++i) {
      threads[i].join();
      LOG(INFO) << "Thread " << i << " is done.";
    }
  }
  // stop system
  Finalize(0, role, true);
  return 0;
}
