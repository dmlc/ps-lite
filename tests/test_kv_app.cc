#include "ps/ps.h"
#include <math.h>
#include <vector>
#include <chrono>

using namespace ps;

void StartServer()
{
  if (!IsServer())
  {
    return;
  }
  auto server = new KVServer<float>(0);
  server->set_request_handle(KVServerDefaultHandle<float>());
  RegisterExitCallback([server]() { delete server; });
}

void RunWorker()
{
  if (!IsWorker())
    return;
  KVWorker<float> kv(0, 0);

  // init
  int num = 400;
  int keySize = 65536;
  std::vector<Key> keys(num);
  std::vector<float> vals(num * keySize);
  std::vector<int> lens(num, 65536);

  int rank = MyRank();
  srand(rank + 7);
  for (int i = 0; i < num; ++i)
  {
    keys[i] = kMaxKey / num * i + rank;
    //vals[i] = (rand() % 1000);
  }

  // push

  int repeat = 50;
  std::vector<int> ts;
  std::vector<uint64_t> times;
  for (int i = 0; i < repeat; ++i)
  {
    ts.clear();
    uint64_t ms = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();

    ts.push_back(kv.Push(keys, vals, lens));
    for (int t : ts)
      kv.Wait(t);

    // pull
    std::vector<int> retLens;
    kv.Wait(kv.Pull(keys, &vals, &retLens));

    uint64_t end = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
    times.push_back(end - ms);
  }

  std::sort(times.begin(), times.end());
  printf("median: (us)\r\n");
}

int main(int argc, char *argv[])
{
  // start system
  Start(0);
  // setup server nodes
  StartServer();
  // run worker nodes
  RunWorker();
  // stop system
  Finalize(0, true);
  return 0;
}
