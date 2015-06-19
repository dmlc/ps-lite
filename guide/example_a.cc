#include "ps.h"
typedef float Val;

int CreateServerNode(int argc, char *argv[]) {
  ps::OnlineServer<Val> server;
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;
  std::vector<Key> key = {1, 3, 5};
  std::vector<Val> val = {1, 1, 1};
  std::vector<Val> recv_val;

  KVWorker<Val> wk;
  int ts = wk.Push(key, val);
  wk.Wait(ts);

  ts = wk.Pull(key, &recv_val);
  wk.Wait(ts);

  std::cout << "values pulled at " << MyNodeID() << ": " <<
      Blob<const Val>(recv_val) << std::endl;
  return 0;
}
