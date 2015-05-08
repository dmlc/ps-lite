#include "my_handle.h"

int CreateServerNode(int argc, char *argv[]) {
  ps::KVServer<Val, ps::MyHandle> server; server.Run();
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;
  std::vector<Key> key = {1, 3, 5};
  std::vector<Val> val = {1, 1, 1};
  std::vector<Val> recv_val(3);

  KVWorker<Val> wk;
  int ts = wk.Push(key, val);
  wk.Wait(ts);

  ts = wk.Pull(key, &recv_val);
  wk.Wait(ts);

  return 0;
}
