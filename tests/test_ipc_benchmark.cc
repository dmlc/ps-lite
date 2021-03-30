#include <chrono>
#include <cmath>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include "ps/ps.h"

#define DIVUP(x, y) (((x)+(y)-1)/(y))
#define ROUNDUP(x, y) (DIVUP((x), (y))*(y))

using namespace ps;

enum MODE { IPC };

std::unordered_map<std::string, void *> _key_shm_addr;
std::unordered_map<std::string, size_t> _key_shm_size;
std::unordered_map<uint64_t, char*> store_;
std::mutex mu_;

void* OpenSharedMemory(const std::string& prefix, uint64_t key, size_t size) {
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

uint64_t EncodeKey(uint64_t seed) {
  return seed << 16;
}

uint64_t DecodeKey(ps::Key key) {
  auto kr = ps::Postoffice::Get()->GetServerKeyRanges()[ps::MyRank()];
  return key - kr.begin();
}

void aligned_memory_alloc(void** ptr, size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  void* p;
  int size_aligned = ROUNDUP(size, page_size);
  int ret = posix_memalign(&p, page_size, size_aligned);
  CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
  CHECK(p);
  memset(p, 0, size);
  *ptr = p;
}

std::unordered_map<uint64_t, KVPairs<char> > mem_map;
template <typename Val>
void EmptyHandler(const KVMeta &req_meta, const KVPairs<Val> &req_data, KVServer<Val> *server) {
  uint64_t key = DecodeKey(req_data.keys[0]);
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
  std::vector<int> timestamp_list;
  auto start = std::chrono::high_resolution_clock::now();
  auto end = std::chrono::high_resolution_clock::now();
  auto val = Environment::Get()->find("LOG_DURATION");
  unsigned int log_duration = val ? atoi(val) : 10;
  
  int cnt = 0;
  while (1) {
    for (int i = 0; i < total_key_num; i++) {
      auto keys = server_keys[i];
      auto lens = server_lens[i];
      auto vals = server_vals[i];

      timestamp_list.push_back(kv.ZPush(keys, vals, lens));
      timestamp_list.push_back(kv.ZPull(keys, &vals, &lens));
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

int AllocateServer(int seed, int total_key_num) {
  bool mixed_mode = Environment::Get()->find("BYTEPS_ENABLE_MIXED_MODE") 
                    ? atoi(Environment::Get()->find("BYTEPS_ENABLE_MIXED_MODE")) 
                    : false;
  const int num_server_total = ps::Postoffice::Get()->GetServerKeyRanges().size();
  const int num_worker_total = ps::Postoffice::Get()->num_workers();
  auto num_server_noncolocate = num_server_total - num_worker_total;
  auto num_server_colocate = num_worker_total;

  if (!mixed_mode) {
    return seed % num_server_total;
  }

  // below we assume the seed increases from 0 with step size 1
  auto ratio = (2.0 * num_server_noncolocate * (num_worker_total - 1)) / 
      ((num_worker_total) * (num_worker_total + num_server_noncolocate) - 2 * num_server_noncolocate);
  auto threshold = ratio * total_key_num;
  if (seed < threshold) {
    return seed % num_server_noncolocate;
  } else {
    return num_server_noncolocate + (seed % num_server_colocate);
  }
}

void RunWorker(int argc, char *argv[]) {
  if (IsServer() || IsScheduler()) return;
  KVWorker<char> kv(0, 0);
  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();

  const int num_servers = krs.size();
  LOG(INFO) << num_servers << " servers in total";
  CHECK_GT(num_servers, 0);

  // init
  int len = (argc > 1) ? atoi(argv[1]) : 1024000;
  MODE mode = IPC;

  size_t partition_bytes = Environment::Get()->find("BYTEPS_PARTITION_BYTES") ? 
      atoi(Environment::Get()->find("BYTEPS_PARTITION_BYTES")) : 4096000;
  CHECK_GE(partition_bytes, (size_t)len) 
      << "tensor partition is not supported in this benchmark"
      << ", try reduce tensor size or increase BYTEPS_PARTITION_BYTES";

  auto v = Environment::Get()->find("NUM_KEY_PER_SERVER");
  const int how_many_key_per_server = v ? atoi(v) : 10;
  const int total_key_num = num_servers * how_many_key_per_server;

  std::vector<SArray<char> > server_vals;
  std::vector<SArray<Key> > server_keys;
  std::vector<SArray<int> > server_lens;
  for (int i = 0; i < total_key_num; i++) {
    auto key = EncodeKey(i);
    SArray<char> vals;
    auto addr = (char*) OpenSharedMemory(std::string("BytePS_ShM_"), key, len);
    vals.reset((char*) addr, len, [](void *){});
    server_vals.push_back(vals);
  }

  std::unordered_map<int, size_t> accumulated_key_num;
  // init push, do not count this into time cost
  for (int i = 0; i < total_key_num; i++) {
    int server = AllocateServer(i, total_key_num);
    accumulated_key_num[server] += 1;

    auto vals = server_vals[i];

    auto key = EncodeKey(i);
    PS_VLOG(1) << "key=" << key
               << " (i=" << i << ")" 
               << " assigned to server " << server;

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

  for (int i = 0; i < num_servers; ++i) {
    PS_VLOG(1) << "server-" << i 
        << " load is " << (100.0 * accumulated_key_num[i] / total_key_num)
        << "%";
  }

  push_pull(kv, server_keys, server_vals, server_lens, len, num_servers, total_key_num, how_many_key_per_server, mode);
}

int main(int argc, char *argv[]) {
  // disable multi-threaded processing first
  setenv("BYTEPS_LOCAL_SIZE", "1", 1);
  setenv("BYTEPS_ENABLE_IPC", "1", 1);
  // start system
  const char* val = CHECK_NOTNULL(Environment::Get()->find("DMLC_ROLE"));
  std::string role_str(val);
  Node::Role role = GetRole(role_str);
  int rank = -1; // -1 means no preferred rank
  StartPS(0, role, rank, true);
  // setup server nodes
  StartServer();
  // run worker nodes
  RunWorker(argc, argv);
  // stop system
  Finalize(0, role, true);
  // release shm
  for (auto &it : _key_shm_addr) {
    munmap(it.second, _key_shm_size[it.first]);
    shm_unlink(it.first.c_str());
  }
  return 0;
}
