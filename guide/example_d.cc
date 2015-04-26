#include "ps.h"
typedef float Val;

int CreateServerNode(int argc, char *argv[]) {
  ps::KVServer<Val> server; server.Run();
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;

  int n = 1000000;
  SBlob<Key> key(n);
  SBlob<Val> val(n, 1);
  SBlob<Val> recv_val(n, 0);
  for (int i = 0; i < n; ++i) key[i] = kMaxKey / n * i;

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
