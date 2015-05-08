/*!
 * @file   ps.h
 * \brief  The parameter server interface
 */
#pragma once
#include <functional>
#include <memory>
#include "ps/base.h"
#include "ps/blob.h"
#include "proto/task.pb.h"
#include "ps/shared_array.h"
#include "kv/kv_cache.h"
#include "kv/kv_store_sparse.h"
#include "kv/kv_store_sparse_dynamic.h"
#include "proto/filter.pb.h"

///////////////////////////////////////////////////////////////////////////////
///                              Worker node APIs                           ///
///////////////////////////////////////////////////////////////////////////////
namespace ps {

/** \brief Push and Pull options */
struct SyncOpts {
  /**
   * \brief the timestamp of the depended requests. This request will be
   * processed by the parameter servers only after the depended requests have
   * been processed.
   */
  std::vector<int> deps;
  /**
   * \brief the function will be executed after received the
   * response from the parameter server
   */
  std::function<void()> callback;

  /**
   * \brief key-value filters to reduce communication cost
   */
  std::vector<Filter> filters;

  /**
   * \brief Sample usage: AddFilter(FilterConfig::COMPRESSING);
   */
  Filter* AddFilter(Filter::Type type);

  /** \brief Convert to a task */
  void GetTask(Task* task) const;
};

/*!
 * \brief key-value cache for sending (receiving) key-value pairs to (from) servers
 *
 * @tparam V the type of value
 */
template<typename V>
class KVWorker {
 public:
  /**
   * @param id the unique identity which is used to find the KVStore at the
   * parameter server. Negative IDs is preserved by system.
   */
  explicit KVWorker(int id = 0) {
    cache_ = CHECK_NOTNULL((new KVCache<Key, V>(id)));
  }
  ~KVWorker() {
    delete cache_;
  }

  /*!
   * \brief Pushes a list of key-value pairs into the parameter server
   *
   * It's a non-blocking call, which returns immediately once the message is
   * queued in the system's sending buffer. The actual push is finished only
   * after Wait(the_returned_timestamp) returns or the provided callback is called.
   *
   * Both keys and values will be copied, using ZPush for zero-copy
   * pushing.
   *
   * Sample usage: assume we have two key-value pairs {1, (1.1, 1.2)}, {3,
   * (3.1,3.2)}, where the value is a 2-length float vector. We then can push these
   * two pairs into the server nodes:
   \code
     KVWorker<float> ps(0);
     std::vector<Key> keys = {1, 3};
     std::vector<float> vals = {1.1, 1.2, 3.1, 3.2};
     ps.Push(keys, vals);
   \endcode
   *
   * @param keys a list of keys
   * @param vals a list of values, whose size should be an integer multiple
   * the key size
   *
   * @return the timestamp of this request.
   */
  int Push(const std::vector<Key>& keys, const std::vector<V>& vals,
           const SyncOpts& opts = SyncOpts()) {
    return Push(Blob<const Key>(keys), Blob<const V>(vals), opts);
  }

  /*!
   * \brief Pulls the values associated with the keys from the parameter server
   *
   * It's a non-blocking call, which returns immediately once the message is
   * queued in the system's sending buffer. The actual push is finished only
   * after Wait(the_returned_timestamp) returns or the provided callback is called.
   *
   * Keys will be copied, using ZPull for zero-copy pull.
   *
   * Sample usage: again assume each key is associated with a 2-length float
   * vector value. We then can pull the newest value from the parameter server:
   \code
     KVWorker<float> ps(0);
     std::vector<Key> keys = {1, 3};
     std::vector<float> vals(4);
     ps.Pull(keys, &vals);
   \endcode
   *
   * @param keys a list of keys
   * @param vals the buffer for the pulled values, which should be
   * pre-allocated and not changed before the pulling is finished.
   *
   * @return the timestamp of this request
   */
  int Pull(const std::vector<Key>& keys, std::vector<V>* vals,
           const SyncOpts& opts = SyncOpts()) {
    return Pull(Blob<const Key>(keys), Blob<V>(vals), opts);
  }

  /*!
   * \brief Waits until a request has been finished
   *
   * Sample usage:
   \code
     int ts = ps.Pull(keys, &vals);
     Wait(ts);
     // now vals is ready for use
   \endcode
   */
  void Wait(int timestamp) {
    cache_->Wait(timestamp);
  }

  /**
   * \brief zero-copy synchronization. Keys (and values for ZPush) will not be
   * copied to reduce the communication delay. Therefore, it is the user's
   * responsibility to keep the keys and values unchanged until the request is
   * finished, namely Wait(ts) returns or the callback is called.
   */
  int ZPush(const std::shared_ptr<std::vector<Key> >& keys,
            const std::shared_ptr<std::vector<V> >& vals,
            const SyncOpts& opts = SyncOpts()) {
    return ZPush(SArray<Key>(keys), SArray<V>(vals), opts);
  }

  int ZPull(const std::shared_ptr<std::vector<Key> >& keys, std::vector<V>* vals,
            const SyncOpts& opts = SyncOpts()) {
    return ZPull(SArray<Key>(keys),
                 SArray<V>(vals->data(), vals->size(), EmptyDel<V>()), opts);
  }

  /*! \brief C-style Push and Pull */
  int Push(Blob<const Key> keys, Blob<const V> vals, const SyncOpts& opts) {
    // copy data
    SArray<Key> s_keys; s_keys.CopyFrom(keys.data, keys.size);
    SArray<V> s_vals; s_vals.CopyFrom(vals.data, vals.size);
    return ZPush(s_keys, s_vals, opts);
  }

  int Pull(Blob<const Key> keys, Blob<V> vals, const SyncOpts& opts) {
    // copy data
    SArray<Key> s_keys; s_keys.CopyFrom(keys.data, keys.size);
    return ZPull(s_keys,
                 SArray<V>(vals.data, vals.size, EmptyDel<V>()), opts);
  }

  int ZPush(const SArray<Key>& keys, const SArray<V>& vals,
            const SyncOpts& opts) {
    Task req; opts.GetTask(&req);
    return cache_->Push(req, keys, vals, opts.callback);
  }


  int ZPull(const SArray<Key>& keys, const SArray<V>& vals,
            const SyncOpts& opts) {
    Task req; opts.GetTask(&req);
    return cache_->Pull(req, keys, vals, opts.callback);
  }

  /*!
   * \brief Increases the clock by delta
   */
  void IncrClock(int delta = 1) {
    cache_->exector()->IncrClock(delta);
  }
 private:
  KVCache<Key, V>* cache_;
};
}  // namespace ps


/**
 * \brief The main function for a worker node
 *
 * All flags and their arguments (e.g. -logtostderr 1) has been parsed and removed
 * from argc and argv, but commandline arguments are remained such as data=my_data.txt
 */
int WorkerNodeMain(int argc, char *argv[]);

///////////////////////////////////////////////////////////////////////////////
///                             Server node APIs                            ///
///////////////////////////////////////////////////////////////////////////////
namespace ps {
/**
 * \brief An example of user-defineable handle.
 * \tparam V the value type
 */
template <typename V>
class IHandle {
 public:
  IHandle() { }
  virtual ~IHandle() { }

  /** \brief Accepts the caller */
  inline void SetCaller(void *obj) { }

  /**
   * \brief Start to handle a request from a worker
   *
   * @param push true if this is a push request
   * @param timestamp the timestamp of this request
   * @param worker the worker id
   */
  inline void Start(bool push, int timestamp, const std::string& worker) { }

  /**
   * \brief The request has been handled
   */
  inline void Finish() { }

  /**
   * \brief Handle initialization, which will be only called once when
   * allocating these key-value paris
   */
  inline void Init(Blob<const Key> keys,
                   Blob<V> vals) {
    memset(vals.data, 0, vals.size*sizeof(V));
  }

  /**
   * \brief Handle PUSH requests from worker nodes
   *
   * @param recv_keys the keys received from a worker node
   * @param recv_vals the corresponding values received from the worker node
   * @param my_vals the corresponding local values
   */
  inline void Push(Blob<const Key> recv_keys,
                   Blob<const V> recv_vals,
                   Blob<V> my_vals) {
    for (size_t i = 0; i < recv_vals.size; ++i)
      my_vals[i] += recv_vals[i];
  }
  /**
   * \brief Handle PUSH requests from worker nod
   *
   * @param recv_keys the keys received from a worker node
   * @param my_vals the corresponding local values
   * @param sent_vals the corresponding values will send to the worker node
   */
  inline void Pull(Blob<const Key> recv_keys,
                   Blob<const V> my_vals,
                   Blob<V> send_vals) {
    for (size_t i = 0; i < my_vals.size; ++i)
      send_vals[i] = my_vals[i];
  }
};


static const int kDynamicValue = -1;
/*!
 * \brief key-value store for server nodes
 *
 * @tparam V the value type
 * @Handle User-defined handles
 * @tparam val_len the length of a value (= val_len * sizeof(V)) that stored in
 * local. It could be a dynamic length DYNAMIC_LEN
 * @tparam sync_val_len the length of value will be synchronized
 */
template <typename V, typename Handle = IHandle<V>, int val_len = 1>
class KVServer {
 public:
  /**
   * \brief Process key-value pairs in online or batch style
   *
   * - ONLINE: individual key-value pairs received from workers are feed into
   *   user-defined writer/reader one by one.
   *
   * - BATCH: all key-value pairs received from a worker in a Push/Pull request
   *   are feed into writer/reader togeter
   *
   * Implementation & Performance
   *
   * - ONLINE: use unordered_map or other equivalence data structure to store KV
   *   pairs. It is suitable when new keys appears during running, such as
   *   SGD/online learning algorithms. However, both read and write could be 5x
   *   slower comparing to BATCH
   *
   * - BATCH: use array to store KV pairs. Suitable for the keys set is fixed at
   *   the beginning, such as batch algorithm.
   */
  enum Type { ONLINE, BATCH };

  /**
   * @param type which affects how key-value pairs are feed into updater and
   *  initializer, see comments below
   * @param id the unique identity. Negative IDs is preserved by system.
   */
  KVServer(int id = 0, Type type = ONLINE)
      : id_(id), type_(type), sync_val_len_(val_len) { }
  ~KVServer() { }

  void set_sync_val_len(int len) { sync_val_len_ = len; }
  Handle& handle() { return handle_; }

  KVStore* Run() {
    KVStore* server = NULL;
    if (type_ == ONLINE) {
      if (val_len != kDynamicValue) {
        server = new KVStoreSparse<Key, V, Handle, val_len>(
            id_, handle_, sync_val_len_);
      } else {
        server = new KVStoreSparseDynamic<Key, V, Handle>(id_, handle_);
      }
    }
    CHECK_NOTNULL(server);
    // let the system to delete server when finished
    Postoffice::instance().manager().TransferCustomer(server);
    return server;
  }

 private:
  int id_;
  Type type_;
  int sync_val_len_;
  Handle handle_;
};
}  // namespace ps

/**
 * \brief The main function for a server node
 *
 * All flags and their arguments (e.g. -logtostderr 1) has been parsed and removed
 * from argc and argv, but commandline arguments are remained such as data=my_data.txt
 */
int CreateServerNode(int argc, char *argv[]);


///////////////////////////////////////////////////////////////////////////////
///                            Scheduler Node APIs                          ///
///////////////////////////////////////////////////////////////////////////////

// TODO

///////////////////////////////////////////////////////////////////////////////
///                            More Advanced APIs                           ///
///////////////////////////////////////////////////////////////////////////////
#include "ps/app.h"
namespace ps {

// The app this node runs
inline App* MyApp() { return Postoffice::instance().manager().app(); }

/*! \brief The global unique string ID of this node */
inline Node MyNode() { return Postoffice::instance().manager().van().my_node(); }
// Each unique string id of my node
inline std::string MyNodeID() { return MyNode().id(); }
/*! \brief Return true if this node is a worker node. */
inline int IsWorkerNode() { return MyNode().role() == Node::WORKER; }
/*! \brief Return true if this node is a server node. */
inline int IsServerNode() { return MyNode().role() == Node::SERVER; }
/*! \brief Return true if this node is a scheduler node. */
inline int IsSchedulerNode() { return MyNode().role() == Node::SCHEDULER; }

inline Range<Key> MyKeyRange() { return Range<Key>(MyNode().key()); }
inline std::string SchedulerID() {
  return Postoffice::instance().manager().van().scheduler().id();
}

inline int NextCustomerID() {
  return Postoffice::instance().manager().NextCustomerID();
}

// The rank ID of this node in its group. Assume this a worker node in a worker
// group with N workers. Then this node will be assigned an unique ID from 0,
// ..., N. Similarly for server and scheduler.
inline int MyRank() { return MyNode().rank(); }
// Total nodes in this node group.
inline int RankSize() {
  auto& mng = Postoffice::instance().manager();
  return IsWorkerNode() ? mng.num_workers() : (IsServerNode() ? mng.num_servers() : 1);
}

// Wait until all FLAGS_num_servers servers are ready.
inline void WaitServersReady() {
  ps::Postoffice::instance().manager().WaitServersReady();
}

// Wait until all FLAGS_num_workers workers are ready.
inline void WaitWorkersReady() {
  ps::Postoffice::instance().manager().WaitWorkersReady();
}

inline void StartSystem(int* argc, char ***argv) {
  ps::Postoffice::instance().Run(argc, argv);
}

inline void StopSystem() {
  ps::Postoffice::instance().Stop();
}

// inline int RunSystem(int argc, char *argv[]) {
//   StartSystem(argc, argv); StopSystem();
//   return 0;
// }

}  // namespace ps

// Implementation

#include <ps/ps-inl.h>
