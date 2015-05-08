#include "ps.h"
typedef float Val;

int CreateServerNode(int argc, char *argv[]) {
  ps::KVServer<Val> server; server.Run();
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;

  int n = 1000000;
  std::shared_ptr<std::vector<Key>> key(new std::vector<Key>(n));
  for (int i = 0; i < n; ++i) (*key)[i] = kMaxKey / n * i;
  std::shared_ptr<std::vector<Val>> val(new std::vector<Val>(n, 1.0));
  std::vector<Val> recv_val(n);

  KVWorker<Val> wk;
  int m = 100;
  for (int i = 0; i < m; ++i) {
    SyncOpts opts;
    opts.AddFilter(Filter::KEY_CACHING);
    opts.AddFilter(Filter::COMPRESSING);
    int ts = wk.ZPush(key, val, opts);
    wk.Wait(ts);

    ts = wk.ZPull(key, &recv_val, opts);
    wk.Wait(ts);
  }
  return 0;
}
