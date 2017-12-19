#include "ps/ps.h"
using namespace ps;

void StartServer() {
  if (!IsServer()) return;
  auto server = new KVServer<float>(0);
  server->set_request_handle(KVServerDefaultHandle<float>());
  RegisterExitCallback([server](){ delete server; });
}

void RunWorker(int customer_id) {
  if (!IsWorker()) return;
  Start(customer_id);
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
  std::cout<< "start pushing\n";
  // push
  int repeat = 50;
  std::vector<int> ts;
  for (int i = 0; i < repeat; ++i) {
    ts.push_back(kv.Push(keys, vals));

    // to avoid too frequency push, which leads huge memory usage
    if (i > 10) kv.Wait(ts[ts.size()-10]);
  }
  std::cout<< "finished push\n";
  for (int t : ts) kv.Wait(t);
  std::cout<< "start pulling\n";

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
  // setup server nodes
  StartServer();
  // start system
  if (!IsWorker()) {
    Start(0);
    Finalize(0, true);
  }
  // run worker nodes
  std::thread t0(RunWorker, 0);
  std::thread t1(RunWorker, 1);

  t0.join();
  t1.join();
  return 0;
}
