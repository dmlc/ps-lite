#include "ps/ps.h"
#include <math.h>
using namespace ps;

void StartServer() {
  if (!IsServer()) return;
  auto server = new KVServer<float>(0);
  server->set_request_handle(KVServerDefaultHandle<float>());
  RegisterExitCallback([server](){ delete server; });
}

void RunWorker(int customer_id) {
  Start(customer_id);
  if (!IsWorker()) {
    return;
  }
  KVWorker<float> kv(0, customer_id);
  // init
  int num = 10000;
  std::vector<Key> keys(num);
  std::vector<float> vals(num);

  int rank = MyRank();
  srand(rank + 7);
  for (int i = 0; i < num; ++i) {
    keys[i] = kMaxKey / num * i + customer_id;
    vals[i] = (rand() % 1000);
  }
  // push
  int repeat = 50;
  std::vector<int> ts;
  for (int i = 0; i < repeat; ++i) {
    ts.push_back(kv.Push(keys, vals));

    // to avoid too frequency push, which leads huge memory usage
    if (i > 10) kv.Wait(ts[ts.size()-10]);
  }
  for (int t : ts) kv.Wait(t);

  // pull
  std::vector<float> rets;
  kv.Wait(kv.Pull(keys, &rets));

  float res = 0;
  for (int i = 0; i < num; ++i) {
    res += fabs(rets[i] - vals[i] * repeat);
  }
  CHECK_LT(res / repeat, 1e-5);
  LL << "error: " << res / repeat;
  // stop system
  Finalize(customer_id, true);
}

int main(int argc, char *argv[]) {
  // start system
  bool isWorker = (strcmp(argv[1], "worker") == 0);
  if (!isWorker) {
    Start(0);
    // setup server nodes
    StartServer();
    Finalize(0, true);
    return 0;
  }
  // run worker nodes
  std::thread t0(RunWorker, 0);
  std::thread t1(RunWorker, 1);

  t0.join();
  t1.join();
  return 0;
}
