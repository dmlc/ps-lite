#include "ps.h"
typedef float Val;

int CreateServerNode(int argc, char *argv[]) {
  ps::OnlineServer<Val> server;
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;

  int n = 1000000;
  auto key = std::make_shared<std::vector<Key>>(n);
  for (int i = 0; i < n; ++i) (*key)[i] = kMaxKey / n * i;
  auto val = std::make_shared<std::vector<Val>>(n, 1.0);

  KVWorker<Val> wk;
  std::vector<Val> recv_val;
  for (int i = 0; i < 100; ++i) {
    SyncOpts opts;
    opts.AddFilter(Filter::KEY_CACHING);
    opts.AddFilter(Filter::COMPRESSING);
    wk.Wait(wk.ZPush(key, val, opts));
    wk.Wait(wk.ZPull(key, &recv_val, opts));
  }
  return 0;
}
