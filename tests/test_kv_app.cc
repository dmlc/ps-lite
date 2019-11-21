#include <cmath>
#include "ps/ps.h"

using namespace ps;

void StartServer() {
  if (!IsServer()) {
    return;
  }
  auto server = new KVServer<float>(0);
  server->set_request_handle(KVServerNewHandle<float>());
  RegisterExitCallback([server](){ delete server; });
}

void RunWorker() {

  if (!IsWorker()) return;
  KVWorker<float> kv(0, 0);


  int num = 100;
  std::vector<Key> keys(2,0);
  keys[1]=1;


  std::vector<float> vals(num);

 
 std::vector<int> lens (2);
 lens[0] = 45;
 lens[1] = 55; 


 for (int i = 0; i < num; ++i) {

//    keys[i] = i;
      vals[i] = i;

  }

 
  for(auto e: keys) std::cout<< e << std::endl;
  std::vector<float> outs(num,0);
   
  std::vector<int> lenss (2,0);
  kv.Wait(kv.Push(keys, vals,lens));
  kv.Wait(kv.Pull(keys, &outs,&lenss));

  for(auto e: lenss) std::cout<< e << std::endl;




/*

  // init
  int num = 10000;
  std::vector<Key> keys(num);
  std::vector<float> vals(num);


  int rank = MyRank();
  srand(rank + 7);
  for (int i = 0; i < num; ++i) {
    keys[i] = kMaxKey / num * i + rank;
    vals[i] = (rand() % 1000);
  }

  std::vector<float> outs;
  kv.Wait(kv.PushPull(keys, vals, &outs));



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

  // pushpull
  std::vector<float> outs;
  for (int i = 0; i < repeat; ++i) {
    // PushPull on the same keys should be called serially
    kv.Wait(kv.PushPull(keys, vals, &outs));
  }

  float res = 0;
  float res2 = 0;
  for (int i = 0; i < num; ++i) {
    res += std::fabs(rets[i] - vals[i] * repeat);
    res2 += std::fabs(outs[i] - vals[i] * 2 * repeat);
  }
  CHECK_LT(res / repeat, 1e-5);
  CHECK_LT(res2 / (2 * repeat), 1e-5);
  LL << "error: " << res / repeat << ", " << res2 / (2 * repeat);
*/

}

int main(int argc, char *argv[]) {
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
