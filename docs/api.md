# APIs


The data communicated are presented as key-value
  pairs, where the key might be the `uint64_t` (defined by `ps::Key`) feature
  index and the value might be the according `float` gradient.
  1. Basic synchronization functions: \ref ps::KVWorker::Push, \ref
  ps::KVWorker::Pull, and \ref ps::KVWorker::Wait
  2. Zero-copy versions: \ref ps::KVWorker::ZPush, \ref
     ps::KVWorker::ZPull

To support dynamic length, pull operations(`Pull` and `ZPull`), do not require the buffer(`vals`) to be the same size as the total data size of pulling down. Larger buffer is allowed while `lens` records the actual size of each key. So the reliable way to read a valid message is to read `lens` bytes. If you ensure that the data size of a key does not change during push or pull, you can verify it by checking whether `lens` of the key is equal to the fixed size.  

often server *i* handles the keys (feature indices) within the i-th
  segment of <em>[0, uint64_max]</em>. The server node allows user-defined handles to
  process the `push` and `pull` requests from the workers.
  1. Online key-value store \ref ps::OnlineServer
  2. Example user-defined value: \ref ps::IVal
  3. Example user-defined handle: \ref ps::IOnlineHandle



  also We can
  also implement

, which is often used to monitor and control the
  progress of the machine learning application. It also can be used to deal with node
  failures. See an example in [asynchronous SGD](https://github.com/dmlc/wormhole/blob/master/learn/solver/async_sgd.h#L27).

```eval_rst
.. automodule:: ps::KVWorker
    :members:
```

```eval_rst
.. doxygenstruct:: ps::KVPairs
   :members:
```
