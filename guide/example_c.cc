#include "ps.h"
typedef float Val;

int CreateServerNode(int argc, char *argv[]) {
  ps::KVServer<Val> server; server.Run();
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;

  std::shared_ptr<std::vector<Key>> key(new std::vector<Key>({1, 3, 5}));
  std::shared_ptr<std::vector<Val>> val(new std::vector<Val>({1, 1, 1}));
  std::vector<Val> recv_val(3);

  KVWorker<Val> wk;
  int ts = wk.ZPush(key, val);
  wk.Wait(ts);

  ts = wk.ZPull(key, &recv_val);
  wk.Wait(ts);

  std::cout << "values pulled at " << MyNodeID() << ": "
            << Blob<const Val>(recv_val) << std::endl;
  return 0;
}
