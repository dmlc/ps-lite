# Introduction                        {#mainpage}

The parameter server aims for high-performance distributed machine learning
applications. In this framework, multiple nodes runs over multiple machines to
solve machine learning problems. The role of a node can be server, worker, or
scheduler, which can be queried via \ref ps::NodeInfo.

![ps arch](https://raw.githubusercontent.com/dmlc/dmlc.github.io/master/img/ps-arch.png)

### Worker node
 A worker node performs the main computations such as reading the data and
  computing the gradient. It communicates with the server nodes via `push` and
  `pull`. For example, it pushes the computed gradient to the servers, or pulls
  the recent model from them. The data communicated are presented as key-value
  pairs, where the key might be the `uint64_t` (defined by `ps::Key`) feature
  index and the value might be the according `float` gradient.
  1. Basic synchronization functions: \ref ps::KVWorker::Push, \ref
  ps::KVWorker::Pull, and \ref ps::KVWorker::Wait
  3. Zero-copy versions: \ref ps::KVWorker::ZPush, \ref
     ps::KVWorker::ZPull

### Server node

 A server node maintains and updates the model. Each node maintains only a part
  of the model, often server i handles the keys (feature indices) within the i-th
  segment of <em>[0, uint64_max]</em>. The server node allows user-defined handles to
  process the `push` and `pull` requests from the workers.
  1. Online key-value store \ref ps::OnlineServer
  2. Example user-defined value: \ref ps::IVal
  3. Example user-defined handle: \ref ps::IOnlineHandle

### Scheduler node
There is an optional scheduler node, which is often used to monitor and control the
  progress of the machine learning application. It also can be used to deal with node
  failures. See an example in [asynchronous SGD](https://github.com/dmlc/wormhole/blob/master/learn/solver/async_sgd.h#L27).

### More
The source codes are available at
[github.com/dmlc/ps-lite](https://github.com/dmlc/ps-lite), which are licensed
under Apache 2.0.
