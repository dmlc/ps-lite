# Tutorial of the Parameter Server

Here we show several examples of how to use the simplified parameter server API [ps.h](../src/ps.h).

## Worker APIs

### Simple `Push` and `Pull`
In the first [example](example_a.cc), we define worker nodes and server nodes by
`CreateServerNode` and `WorkerNodeMain`, respectively. We next ask workers to
push a list of key-value pairs into servers and then pull the new values back.

```c++
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
```

Tis example can be compiled by executing `make -C .. guide` from inside the guide directory.
It can be run using 4 worker nodes and 1 server node on the local machine by using `./local.sh 1 4 ./example_a`. 
A possible output is
```
values pulled at W3: [3]: 2 2 2
values pulled at W0: [3]: 2 2 2
values pulled at W2: [3]: 4 4 4
values pulled at W1: [3]: 4 4 4
```
Other information is logged in the `log/` directory.


### Time dependency and callback

Note that we called `Wait` after each `Push` and `Pull` to wait for these
asynchronous functions to complete their data communication. Besides
`Wait` we can add the dependency that "My pulled results should at least have
the data I pushed previously" into servers node by specifying the `deps`
options.

![deps](deps.png)

Furthermore, we can execute `std::cout` in a callback function. The following
codes ([example_b](example_b.cc)) shows the above steps.


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
program can write or delete data immediately. In situations where the overhead of
memcpy is expensive, we can then use `ZPush` and `ZPull` to do zero-copy data
communication ([example_c](example_c.cc)):

```c++
  auto key = std::make_shared<std::vector<Key>>();
  auto val = std::make_shared<std::vector<Val>>();

  *key = {1, 3, 5};
  *val = {1, 1, 1};

  KVWorker<Val> wk;
  wk.Wait(wk.ZPush(key, val));

  std::vector<Val> recv_val;
  wk.Wait(wk.ZPull(key, &recv_val));
```

The system will maintain a copy of `key` and `val` to prevent the release of memory
before the `Push` and `Pull` are finished. It's safe to destroy the `key` and `val`
shared pointers in user codes. However, changing the content of `key` and `val` may
affect the actualy data sent out.

### Filters

We can apply filters to reduce the data communication volume. In the following
example ([example_d](example_d.cc)), we first let both worker and server cache
the keys list to avoid sending the same key list twice, and then apply lossless
compression on values.

```c++
  int n = 1000000;
  auto key = std::make_shared<std::vector<Key>>(n);
  for (int i = 0; i < n; ++i) (*key)[i] = kMaxKey / n * i;
  auto val = std::make_shared<std::vector<Val>>(n, 1.0);

  KVWorker<Val> wk;
  std::vector<Val> recv_val;
  for (int i = 0; i < 100; ++i) {
    SyncOpts opts;
    opts.AddFilter(Filter::KEY_CACHING);
    opts.AddFilter(Filter::COMPRESSING);
    wk.Wait(wk.ZPush(key, val, opts));
    wk.Wait(wk.ZPull(key, &recv_val, opts));
  }

```

Using 4 workers and 4 servers (`./local.sh 4 4 ./example_d -logtostderr`), these
two filters can reduce the total number of data sent by a worker from 2GB to
20MB.

### Vector value

A value could be a vector rather than a scalar. For example, we can associate
each key with a fixed length 2 value vector.

```
  std::vector<Key> key = {1,    3,    5};
  std::vector<Val> val = {1, 2, 3, 4, 5, 6};
  wk.Push(key, val);
```
Note that when using the default `OnlineServer`, multiple pushed values with the same key are summed. 
For the example above, the resulting keys and values would be `1 => 3, 3 => 7, 5 => 11`. However,
([example_e](example_e)) shows an server with a custom handler that stores the individual vectors
associated with each key (e.g. `1 => [1,2], 3 => [3,4], 5 => [5,6]`). More about this below.


Dynamic length values may also be passed to the server ([example_e](example_e))
```
  std::vector<Key> key = {1, 3,       8    };
  std::vector<Val> val = {1, 3, 4, 5, 9, 10};
  std::vector<int> siz = {1, 3,       2    };
  wk.VPush(key, val);
```

## Server APIs

### User defined handle

The server nodes accepts a user defined handle. In example [e](example_e.cc), we
show how to accept and store variable length values at server nodes.

```c++
struct MyVal {
  std::vector<Val> w;
  inline void Load(dmlc::Stream *fi) { fi->Read(&w); }
  inline void Save(dmlc::Stream *fo) const { fo->Write(w); }
};

class MyHandle {
 public:
  ...
  void Push(Key recv_key, ps::Blob<const Val> recv_val, MyVal& my_val) {
    size_t n = recv_val.size;
    auto& w = my_val.w;
    if (w.empty()) w.resize(n);
    for (size_t i = 0; i < n; ++i) w[i] += recv_val[i];
  }

  void Pull(Key recv_key, MyVal& my_val, ps::Blob<Val>& send_val) {
    send_val.data = my_val.w.data();
    send_val.size = my_val.w.size();
  }
  ...
};

```

A sample output after running `./local.sh 1 2 ./example_e`

```
-------
accepts push from W0 with timestamp 0 and command -1
init key1
handle push: key 1, val [1]: 1
init key3
handle push: key 3, val [3]: 3 4 5
init key8
handle push: key 8, val [2]: 9 10
finished 1 / 2 on timestamp 0
-------
-------
accepts push from W1 with timestamp 0 and command -1
handle push: key 1, val [1]: 1
handle push: key 3, val [3]: 3 4 5
handle push: key 8, val [2]: 9 10
finished 2 / 2 on timestamp 0
-------
-------
accepts pull from W0 with timestamp 1 and command -1
handle pull: key 1
handle pull: key 3
handle pull: key 8
finished 1 / 2 on timestamp 1
-------
-------
accepts pull from W1 with timestamp 1 and command -1
handle pull: key 1
handle pull: key 3
handle pull: key 8
finished 2 / 2 on timestamp 1
-------
values pulled at W0: [6]: 2 6 8 10 18 20
[3]: 1 3 2
values pulled at W1: [6]: 2 6 8 10 18 20
[3]: 1 3 2
```

More examples:
 - fixed length values: [linear/async_sgd.h](https://github.com/dmlc/wormhole/blob/master/learn/linear/async_sgd.h)
 - dynamic length values: [factorization_machine/fm_server.h](https://github.com/dmlc/wormhole/blob/master/learn/factorization_machine/fm_server.h)

### Batch Model

TODO

### Data Consistency

TODO
