# Tutorial of the Parameter Server

Here we show several examples on how to use the simplified parameter server API [ps.h](../src/ps.h).

## Worker APIs

### Simple `Push` and `Pull`
On the first [example](example_a.cc), we define worker nodes and server nodes by
`CreateServerNode` and `WorkerNodeMain`, respectively. Next ask workers to
push a list of key-value pairs into servers and then pull the new values back.

```c++
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
  wk.Wait(ts);

  ts = wk.Pull(key, &recv_val);
  wk.Wait(ts);

  std::cout << "values pulled at " << MyNodeID() << ": " <<
      Blob<const Val>(recv_val) << std::endl;
  return 0;
}
```

This example can be compiled by `make -C .. guide` and run using 4 worker nodes
and 1 server node in local machine by `./local.sh 1 4 ./example_a`. A possible
output is
```
values pulled at W3: [3]: 2 2 2
values pulled at W0: [3]: 2 2 2
values pulled at W2: [3]: 4 4 4
values pulled at W1: [3]: 4 4 4
```
Other information is logged in the `log/` directory.


### Time dependency and callback

Note that we called `Wait` after each `Push` and `Pull` to wait these two
asynchronous functions actually finished the data communication. Besides
`Wait` we can move the dependency that "My pulled results should at least have
the data I pushed previously" into servers node by specifying the `deps`
options.

![deps](deps.png)

Furthermore, we can execute `std::cout` in a callback function. The following
codes ([example_b](example_b.cc)) are equal to above.


```c++
  KVWorker<Val> wk;
  int ts = wk.Push(key, val);

  SyncOpts opts;
  opts.deps = {ts};
  opts.callback = [&recv_val]() {
    std::cout << "values pulled at " << MyNodeID() << ": " <<
    Blob<const Val>(recv_val) << std::endl;
  };
  ts = wk.Pull(key, &recv_val, opts);
  wk.Wait(ts);
```

### Zero-copy communication

In default, both `Push` and `Pull` will first copy the data so that the user
program can write or delete data immediately. In some situation, the memcpy
overhead is expensive, we can then use `ZPush` and `ZPull` to do zero-copy data
communication ([example_c](example_c.cc)):

```c++
  std::shared_ptr<std::vector<Key>> key(new std::vector<Key>({1, 3, 5}));
  std::shared_ptr<std::vector<Val>> val(new std::vector<Val>({1, 1, 1}));
  std::vector<Val> recv_val(3);

  KVWorker<Val> wk;
  int ts = wk.ZPush(key, val);
  wk.Wait(ts);

  ts = wk.ZPull(key, &recv_val);
  wk.Wait(ts);
```

The system will maintain a copy of `key` and `val` to prevent release the memory
before the `Push` and `Pull` are finished. It's safe to destroy `key` and `val`
on the user codes. However, change the content of `key` and `val` may affect the
actualy data sent out.

### Filters

We can apply filters to reduce the data communication volume. In the following
example ([example_d](example_d.cc)), we first let both worker and server cache
the keys list to avoid sending the same key list twice, and then apply lossness
compression on values.

```c++
  int n = 1000000;
  std::shared_ptr<std::vector<Key>> key(new std::vector<Key>(n));
  for (int i = 0; i < n; ++i) (*key)[i] = kMaxKey / n * i;
  std::shared_ptr<std::vector<Val>> val(new std::vector<Val>(n, 1.0));
  std::vector<Val> recv_val(n);

  KVWorker<Val> wk;
  int m = 100;
  for (int i = 0; i < m; ++i) {
    SyncOpts opts;
    opts.AddFilter(Filter::KEY_CACHING);
    opts.AddFilter(Filter::COMPRESSING);
    int ts = wk.ZPush(key, val, opts);
    wk.Wait(ts);

    ts = wk.ZPull(key, &recv_val, opts);
    wk.Wait(ts);
  }
```

Using 4 workers and 4 servers (`./local.sh 4 4 ./example_d -logtostderr`), these
two filters can reduce the total number of data sent by a worker from 2GB to
20MB.

## Server APIs

### Simple handle summing the data

Example [e](example_e.cc) is similar to Example a, where servers sum the data
pushed by workers. The main difference is we
let the handle print some debug information to clearly see how the handle is
called:

```c++
class MyHandle {
 public:
  void SetCaller(void *obj) { obj_ = (Customer*)obj; }

  inline void Start(bool push, int timestamp, const std::string& worker) {
    std::cout << "accept " << (push ? "push" : "pull") << " from " << worker
              << " with timestamp " << timestamp << std::endl;
    ts_ = timestamp;
  }

  inline void Finish() {
    std::cout << "finished " << obj_->NumDoneReceivedRequest(ts_, kWorkerGroup)
              << " / " << FLAGS_num_workers << " on timestamp " << ts_ << std::endl;
  }

  inline void Init(Blob<const Key> keys,
                   Blob<Val> vals) {
    memset(vals.data, 0, vals.size*sizeof(Val));
    std::cout << "init key " << keys << " val " << vals << std::endl;
  }

  inline void Push(Blob<const Key> recv_keys,
                   Blob<const Val> recv_vals,
                   Blob<Val> my_vals) {
    for (size_t i = 0; i < recv_vals.size; ++i)
      my_vals[i] += recv_vals[i];
    std::cout << "handle push: key " << recv_keys << " val " << recv_vals << std::endl;
  }

  inline void Pull(Blob<const Key> recv_keys,
                   Blob<const Val> my_vals,
                   Blob<Val> send_vals) {
    for (size_t i = 0; i < my_vals.size; ++i)
      send_vals[i] = my_vals[i];
    std::cout << "handle pull: key " << recv_keys << std::endl;
  }
 private:
  Customer* obj_ = nullptr;
  int ts_;
};
```

A sample output after running `./local.sh 1 2 ./example_e`

```
accept push from W1 with timestamp 0
init key [1]: 1  val [1]: 0
handle push: key [1]: 1  val [1]: 1
init key [1]: 3  val [1]: 0
handle push: key [1]: 3  val [1]: 1
init key [1]: 5  val [1]: 0
handle push: key [1]: 5  val [1]: 1
finished 1 / 2 on timestamp 0
accept pull from W1 with timestamp 1
handle pull: key [1]: 1
handle pull: key [1]: 3
handle pull: key [1]: 5
finished 1 / 2 on timestamp 1
accept push from W0 with timestamp 0
handle push: key [1]: 1  val [1]: 1
handle push: key [1]: 3  val [1]: 1
handle push: key [1]: 5  val [1]: 1
finished 2 / 2 on timestamp 0
accept pull from W0 with timestamp 1
handle pull: key [1]: 1
handle pull: key [1]: 3
handle pull: key [1]: 5
finished 2 / 2 on timestamp 1
```

See more online handles in [sgd_server_handle.h](https://github.com/dmlc/wormhole/blob/master/learn/linear/sgd/sgd_server_handle.h)

### Batch Model

TODO

### Data Consistency

TODO
