#include "ps.h"
#include <random>
#include <algorithm>

typedef float Val;

DEFINE_int32(repeat, 1000, "repeat n times");
DEFINE_int32(kv_pair, 1000, "number of key-value pairs a worker send to server each time.");
DEFINE_string(mode, "online", "online or batch. (TODO)");

int CreateServerNode(int argc, char *argv[]) {
  ps::OnlineServer<Val> server;
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;

  int n = FLAGS_kv_pair;
  auto key = std::make_shared<std::vector<Key>>();
  auto val = std::make_shared<std::vector<Val>>();

  std::random_device rd;
  std::mt19937 gen(rd());

  std::uniform_int_distribution<Key> dis(0, kMaxKey);
  key->resize(n);
  for (int i = 0; i < n; ++i) (*key)[i] = dis(gen);
  std::sort(key->begin(), key->end());

  std::uniform_real_distribution<Val> rdis(-1, 1);
  val->resize(n);
  for (int i = 0; i < n; ++i) (*val)[i] = rdis(gen);

  std::vector<Val> recv_val;

  KVWorker<Val> wk;
  for (int i = 0; i < FLAGS_repeat; ++i) {
    SyncOpts opts;
    // opts.AddFilter(Filter::KEY_CACHING);
    int ts = wk.ZPush(key, val, opts);
    wk.Wait(ts);

    ts = wk.ZPull(key, &recv_val, opts);
    wk.Wait(ts);
  }
  return 0;
}
