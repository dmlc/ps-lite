#include "ps.h"
typedef float Val;

int CreateServerNode(int argc, char *argv[]) {
  ps::KVServer<Val> server; server.Run();
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;

  SBlob<Key> key = {1, 3, 5};
  SBlob<Val> val = {1, 1, 1};
  SBlob<Val> recv_val(3);

  KVWorker<Val> wk;
  int ts = wk.ZPush(key, val);
  wk.Wait(ts);

  ts = wk.ZPull(key, &recv_val);
  wk.Wait(ts);

  std::cout << "values pulled at " << MyNodeID() << ": " <<
      recv_val.ShortDebugString() << std::endl;
  return 0;
}
