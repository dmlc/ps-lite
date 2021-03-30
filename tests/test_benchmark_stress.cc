#include <chrono>
#include <cmath>
#include <thread>
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

// ===== Used in worker
const int local_gpu_size = 2;

std::vector<SArray<char> > server_vals_gather_scatter;      // global_session_size * global_gpu_size
std::vector<SArray<Key> >  server_keys_gather_scatter;

std::vector<SArray<char> > server_vals_datascatter;         // global_session_size * global_gpu_size
std::vector<SArray<Key> >  server_keys_datascatter;

std::vector<SArray<char> > server_vals_dense;               // global_session_size * num_server
std::vector<SArray<Key> >  server_keys_dense;

std::vector<SArray<int> >  server_lens;      // We now use all the same len, only the first element is used


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

  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();
  auto node_id_str = Environment::Get()->find("BYTEPS_NODE_ID");
  int node_id = atoi(node_id_str);

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
    CHECK(req_meta.val_len != 0);
    CHECK_NE(iter, mem_map.end()) << "Not found key: " << key - krs[node_id].begin();
    server->Response(req_meta, iter->second);
  }
}

void StartServer() {
  debug_mode_ = Environment::Get()->find("DEBUG_MODE") ? true : false;
  auto server = new KVServer<char>(0);
  server->set_request_handle(EmptyHandler<char>);
  RegisterExitCallback([server]() { delete server; });
}

enum COMM_TYPE {
    SCATTER = 0,
    GATHER = 0,
    DATA_SCATTER = 1, 
    DENSE = 2,
};

inline int GetKeyIndex(COMM_TYPE type,
                       int global_session_rank,
                       int tgt_global_gpu_id /* server_id if type==COMM_TYPE::DENSE */,
                       int global_gpu_size,
                       int num_servers) {
  switch (type) {
    case SCATTER: { // COMM_TYPE::GATHER = COMM_TYPE::SCATTER
      return global_session_rank * global_gpu_size + tgt_global_gpu_id;
    } break;
    case DATA_SCATTER: {
      return global_session_rank * global_gpu_size + tgt_global_gpu_id;
    } break;
    case DENSE: {
      return global_session_rank * num_servers + tgt_global_gpu_id;
    } break;
    default: CHECK(0);
  }
  return -1;
}

inline void InitVals(std::vector<SArray<char> >& server_vals, int num_keys, size_t len) {
  for (int key = 0; key < num_keys; key++) {
    void* ptr;
    aligned_memory_alloc(&ptr, len);
    SArray<char> vals;
    vals.reset((char*) ptr, len * sizeof(char), [](void *){});
    server_vals.push_back(vals);
  }
}

inline void InitOneKeyThenPush(ps::Key ps_key,
                               std::vector<SArray<Key> > &server_keys,
                               std::vector<SArray<int> > &server_lens,
                               SArray<char> &vals,
                               int len,
                               KVWorker<char>* kv,
                               bool should_push = false) {
  void* ptr_key;
  aligned_memory_alloc(&ptr_key, sizeof(Key));
  SArray<Key> keys;
  keys.reset((Key*) ptr_key, 1, [](void *){});
  memcpy(ptr_key, &ps_key, sizeof(Key));
  server_keys.push_back(keys);

  // page aligned vals
  void* ptr_len;
  aligned_memory_alloc(&ptr_len, sizeof(int));
  SArray<int> lens;
  lens.reset((int*) ptr_len, 1, [](void *){});
  memcpy(ptr_len, &len, sizeof(len));
  server_lens.push_back(lens);

  if (should_push) {
    kv->Wait(kv->ZPush(keys, vals, lens));
  }
}

void InitWorker(KVWorker<char>* kv, int len, int global_session_size, int global_gpu_size, int num_servers, bool is_global_root) {
  // Init all the vals, each node has all the keys / vals
  {
    InitVals(server_vals_gather_scatter, global_session_size * global_gpu_size, len);
    InitVals(server_vals_datascatter, global_session_size * global_gpu_size, len);
    InitVals(server_vals_dense, global_session_size * num_servers, len);
  }

  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();
  int latest_key = 0;
  // Init all the keys and init push, do not count this into time cost
  // Only the root node (global session id = 0) would push to the server for server memory init.
  for (int global_session_id = 0; global_session_id < global_session_size; global_session_id ++) {
    for (int global_gpu_id = 0; global_gpu_id < global_gpu_size; global_gpu_id ++) {
      int server_id = global_gpu_id / local_gpu_size;
      LOG(INFO) << "Init key for global_gpu_id " << global_gpu_id << " @ node " << server_id;
      // Init dataScatter
      {
        int idx = GetKeyIndex(COMM_TYPE::DATA_SCATTER, global_session_id, global_gpu_id,
                              global_gpu_size, num_servers);
        CHECK(idx == (int) server_keys_datascatter.size()) 
            << "global_session_id: " << global_session_id
            << " global_gpu_id: " << global_gpu_id
            << " idx: " << idx
            << " server_keys_datascatter.size(): " << server_keys_datascatter.size();
        auto vals = server_vals_datascatter[idx];

        ps::Key ps_key = krs[server_id].begin() + latest_key;
        InitOneKeyThenPush(ps_key, server_keys_datascatter, server_lens, vals, len, kv, is_global_root);
      }
      latest_key ++;

      // Init gather-scatter
      {
        int idx = GetKeyIndex(COMM_TYPE::GATHER, global_session_id, global_gpu_id,
                              global_gpu_size, num_servers);
        CHECK(idx == (int) server_keys_gather_scatter.size());
        auto vals = server_vals_gather_scatter[idx];

        ps::Key ps_key = krs[server_id].begin() + latest_key;
        LOG(INFO) << "Prepare key for gather-scatter " << ps_key;
        InitOneKeyThenPush(ps_key, server_keys_gather_scatter, server_lens, vals, len, kv, is_global_root);
      }
      latest_key ++;
    }

    // Init dense
    for (int server = 0; server < num_servers; server ++) {
      {
        int idx = GetKeyIndex(COMM_TYPE::DENSE, global_session_id, server,
                              global_gpu_size, num_servers);
        CHECK(idx == (int) server_keys_dense.size());
        auto vals = server_vals_dense[idx];

        ps::Key ps_key = krs[server].begin() + latest_key;
        InitOneKeyThenPush(ps_key, server_keys_dense, server_lens, vals, len, kv, is_global_root);
      }
      latest_key ++;
    }
  }
  Postoffice::GetWorker()->Barrier(0, ps::kWorkerGroup);
  LOG(INFO) << "Finish setup.";
}

void RunWorker(int argc, char *argv[], KVWorker<char>* kv, int tid, int nthread) {
  // In UCX usage mode, we have four high level APIs. All of them involes inter-node commnications
  // that invoke ps-lite push/pull under the hood. Data is always transfered src -> dst, and push
  // is called from src, pull called from dst. Gather and scatter share pslite keys. We call these
  // APIs in the order of DataScatter, Gather, Scatter, DenseReduce in a worker session, and multiple
  // worker sessions will be present in the same node. They may be in different threads in the same 
  // process or have their own processes. Each session will be bound to a local GPU card.
  //
  // UCX will enable all the following src-dst memory location combinations.
  //
  // DataScatter: ZPush (src local CPU, dst remote GPU). A session calls a ZPush for every GPU dst on every remote node.
  // Gather: ZPull (src remote GPU, dst local GPU). A session calls a ZPull for every GPU src on every remote node.
  // Scatter: ZPush (src local GPU, dst remote GPU). A session calls a ZPush for every GPU dst on every remote node.
  // DenseReduce: ZPush (src local GPU, dst remote CPU) then one ZPull (src remote CPU, dst local GPU).
  //              A session calls a ZPush + ZPull for each remote node.
  auto krs = ps::Postoffice::Get()->GetServerKeyRanges();

  const int num_servers = krs.size();
  LOG(INFO) << num_servers << " servers in total";
  CHECK_GT(num_servers, 0);

  // init
  int len = (argc > 1) ? atoi(argv[1]) : 1024000 * 30;
  int repeat = (argc > 2) ? atoi(argv[2]) : 100000;

  auto num_node = num_servers;

  // To simulate UCX related usage, per global session we have 
  // denseReduce x1, (scatter, dataScatter) x (global_gpu_size - local_gpu_size),
  // gather use same key as scatter.
  int global_gpu_size = local_gpu_size * num_node;

  auto node_id_str = Environment::Get()->find("BYTEPS_NODE_ID");
  int node_id = atoi(node_id_str);
  int my_global_session_id = nthread * node_id + tid;

  LOG(INFO) << "Gather scatter simulate mode";
  for (int minibatch = 0; minibatch < repeat; ++ minibatch) {
    // DataScatter
    uint64_t accumulated_ms = 0;
    {
      auto start = std::chrono::high_resolution_clock::now();
      std::vector<int> timestamps;
      for (int global_gid = 0; global_gid < global_gpu_size; global_gid ++) {
        int dst_node_id = global_gid / local_gpu_size;          
        // Skip if worker and server on same node.
        if (node_id == dst_node_id) continue;

        int idx = GetKeyIndex(COMM_TYPE::DATA_SCATTER, my_global_session_id, global_gid,
                              global_gpu_size, num_servers);
        auto lens = server_lens[0];
        auto keys = server_keys_datascatter[idx];
        auto vals = server_vals_datascatter[idx];

        // src local CPU, dst remote GPU.
        timestamps.push_back(kv->ZPush(keys, vals, lens));
      }
      for (auto ts : timestamps) {
        kv->Wait(ts);
      }
      auto end = std::chrono::high_resolution_clock::now();
      accumulated_ms += (end - start).count(); // ns
    }
    if (minibatch % 100 == 0)
      LL << "DataScatter " << len * sizeof(char)
          << " bytes to each server, repeat=" << repeat
          << ", total_time="
          << accumulated_ms / 1e6 << "ms";

    // Gather
    accumulated_ms = 0;
    {
      auto start = std::chrono::high_resolution_clock::now();
      std::vector<int> timestamps;
      for (int global_gid = 0; global_gid < global_gpu_size; global_gid ++) {
        int dst_node_id = global_gid / local_gpu_size;          
        // Skip if worker and server on same node.
        if (node_id == dst_node_id) continue;

        int idx = GetKeyIndex(COMM_TYPE::GATHER, my_global_session_id, global_gid,
                              global_gpu_size, num_servers);
        auto lens = server_lens[0];
        auto keys = server_keys_gather_scatter[idx];
        auto vals = server_vals_gather_scatter[idx];

        // src all remote GPUs, dst local GPU.
        timestamps.push_back(kv->ZPull(keys, &vals, &lens));
      }
      for (auto ts : timestamps) {
        kv->Wait(ts);
      }
      auto end = std::chrono::high_resolution_clock::now();
      accumulated_ms += (end - start).count(); // ns
    }
    if (minibatch % 100 == 0)
      LL << "Gather " << len * sizeof(char)
          << " bytes to each server, repeat=" << repeat
          << ", total_time="
          << accumulated_ms / 1e6 << "ms";

    // scatter
    accumulated_ms = 0;
    {
      auto start = std::chrono::high_resolution_clock::now();
      std::vector<int> timestamps;
      for (int global_gid = 0; global_gid < global_gpu_size; global_gid ++) {
        int dst_node_id = global_gid / local_gpu_size;          
        // Skip if worker and server on same node.
        if (node_id == dst_node_id) continue;

        int idx = GetKeyIndex(COMM_TYPE::SCATTER, my_global_session_id, global_gid,
                              global_gpu_size, num_servers);
        auto lens = server_lens[0];
        auto keys = server_keys_gather_scatter[idx];
        auto vals = server_vals_gather_scatter[idx];

        // src one local GPU, dst all remote GPUs.
        timestamps.push_back(kv->ZPush(keys, vals, lens));
      }
      for (auto ts : timestamps) {
        kv->Wait(ts);
      }
      auto end = std::chrono::high_resolution_clock::now();
      accumulated_ms += (end - start).count(); // ns
    }

    if (minibatch % 100 == 0)
      LL << "Scatter " << len * sizeof(char)
          << " bytes to each server, repeat=" << repeat
          << ", total_time="
          << accumulated_ms / 1e6 << "ms";

    // dense
    accumulated_ms = 0;
    {
      auto start = std::chrono::high_resolution_clock::now();
      std::vector<int> timestamps;
      for (int server = 0; server < num_servers; server ++) {
        // Skip if worker and server on same node.
        if (node_id == server) continue;

        int idx = GetKeyIndex(COMM_TYPE::DENSE, my_global_session_id, server, 
                              global_gpu_size, num_servers);
        auto lens = server_lens[0];
        auto keys = server_keys_dense[idx];
        auto vals = server_vals_dense[idx];

        // src one local GPU, dst all remote node's CPU host mem.
        timestamps.push_back(kv->ZPush(keys, vals, lens));
      }
      for (auto ts : timestamps) {
        kv->Wait(ts);
      }

      timestamps.clear();

      for (int server = 0; server < num_servers; server ++) {
        // Skip if worker and server on same node.
        if (node_id == server) continue;

        int idx = GetKeyIndex(COMM_TYPE::DENSE, my_global_session_id, server, 
                              global_gpu_size, num_servers);
        auto lens = server_lens[0];
        auto keys = server_keys_dense[idx];
        auto vals = server_vals_dense[idx];

        // src all remote node's CPU host mem, dst one local GPU.
        timestamps.push_back(kv->ZPull(keys, &vals, &lens));
      }
      for (auto ts : timestamps) {
        kv->Wait(ts);
      }

      auto end = std::chrono::high_resolution_clock::now();
      accumulated_ms += (end - start).count(); // ns
    }

    if (minibatch % 100 == 0)
      LL << "Dense " << len * sizeof(char)
          << " bytes to each server, repeat=" << repeat
          << ", total_time="
          << accumulated_ms / 1e6 << "ms";
  }
}

int main(int argc, char *argv[]) {
  // disable multi-threaded processing first
  setenv("ENABLE_SERVER_MULTIPULL", "0", 1);

  auto v = Environment::Get()->find("BENCHMARK_NTHREAD");
  const int nthread = v ? atoi(v) : 1;
  LOG(INFO) << "number of threads for the same worker = " << nthread;

  // start system
  const char* val = CHECK_NOTNULL(Environment::Get()->find("DMLC_ROLE"));
  std::string role_str(val);
  Node::Role role = GetRole(role_str);
  int rank = -1;
  LOG(INFO) << "PS role = " << role_str;
  if (role == Node::SCHEDULER) {
    StartPS(0, role, rank, true);
    Finalize(0, role, true);
    LOG(INFO) << "scheduler is DONE";
    return 0;
  }

  StartPS(0, role, rank, true);
  // setup server logic
  StartServer();
  // run worker logic
  KVWorker<char> kv(0, 0);
  {
    auto krs = ps::Postoffice::Get()->GetServerKeyRanges();
    const int num_servers = krs.size();

    LOG(INFO) << num_servers << " servers in total";
    CHECK_GT(num_servers, 0);
    int len = (argc > 1) ? atoi(argv[1]) : 1024000 * 30;
    auto num_node = num_servers;
    auto global_session_size = nthread * num_node;
    int global_gpu_size = local_gpu_size * num_node;

    auto node_id_str = Environment::Get()->find("BYTEPS_NODE_ID");
    CHECK(node_id_str) << "Please set BYTEPS_NODE_ID";
    int node_id = atoi(node_id_str);

    bool is_global_root = (node_id == 0);
    InitWorker(&kv, len, global_session_size, global_gpu_size, num_servers, is_global_root);
  }

  std::vector<std::thread> threads;
  for (int i = 0; i < nthread; ++i) {
    threads.emplace_back(RunWorker, argc, argv, &kv, threads.size(), nthread);
  }
  // wait for workers
  for (int i = 0; i < nthread; ++i) {
    threads[i].join();
    LOG(INFO) << "Thread " << i << " is done.";
  }
  // stop system
  Finalize(0, role, true);
  LOG(INFO) << "joint worker/server is DONE";
  return 0;
}
