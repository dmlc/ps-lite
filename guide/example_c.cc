#include "ps.h"
typedef float Val;

int CreateServerNode(int argc, char *argv[]) {
  ps::OnlineServer<Val> server;
  return 0;
}

int WorkerNodeMain(int argc, char *argv[]) {
  using namespace ps;
  auto key = std::make_shared<std::vector<Key>>();
  auto val = std::make_shared<std::vector<Val>>();

  *key = {1, 3, 5};
  *val = {1, 1, 1};

  KVWorker<Val> wk;
  wk.Wait(wk.ZPush(key, val));

  std::vector<Val> recv_val;
  wk.Wait(wk.ZPull(key, &recv_val));

  std::cout << "values pulled at " << MyNodeID() << ": "
            << Blob<const Val>(recv_val) << std::endl;
  return 0;
}
