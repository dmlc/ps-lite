#include "ps.h"
typedef float Val;

int CreateServerNode(int argc, char *argv[]) {
  ps::KVServer<Val> server; server.Run();
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;
  std::vector<Key> key = {1, 3, 5};
  std::vector<Val> val = {1, 1, 1};
  std::vector<Val> recv_val(3);

  KVWorker<Val> wk;
  int ts = wk.Push(key, val);

  SyncOpts opts;
  opts.deps = {ts};
  opts.callback = [&recv_val]() {
    std::cout << "values pulled at " << MyNodeID() << ": " <<
    CBlob<Val>(recv_val).ShortDebugString() << std::endl;
  };
  ts = wk.Pull(key, &recv_val, opts);
  wk.Wait(ts);

  return 0;
}
