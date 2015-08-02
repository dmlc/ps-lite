# Introduction                         {#mainpage}


The parameter server is a distributed system aims for efficient machine learning
applications. In this framework, there are three kinds of nodes: server, worker,
and scheduler.

![Caption text](../../doc/ps-arch.png)

* A worker node does the main computations such as reading the data and
  computing the gradient. It communicates with the server nodes via `push` and
  `pull`. For example, it can push the computed gradient to the servers, or
  pull the recent model from them. The data communicated are presented as
  key-value pairs, where the key might be the `uint64_t` feature index and the value
  might be the according `float` gradient.

  The function `Push` and `Pull`, together with dynamic value length and
  zero-copy variants, are defined in class \ref ps::KVWorker

* A server node maintains and updates the model. Each node maintains only a part
  of the model, often server *i* handles the keys (feature indices) within the *i*-th
  segment of *[0, uint64_max]*. The server node allows user-defined handles to
  process the `push` and `pull` requests from the workers. See more details in
  \ref ps::OnlineServer

* There is an optional scheduler node, which is often used to monitor and control the
  progress of the machine learning application. It also can be used to deal with node
  failures. See an example in [asynchronous SGD](https://github.com/dmlc/wormhole/blob/master/learn/solver/async_sgd.h#L27).


There is helper class \ref ps::NodeInfo for query info about the node.
