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
   * \brief Sample usage: AddFilter(Filter::COMPRESSING);
   */
  Filter* AddFilter(Filter::Type type);
  /**
   * \brief The command sent to the server. -1 means no command.
   */
  int cmd = -1;
};

/*!
 * \brief key-value cache for sending (receiving) key-value pairs to (from) servers
 *
 * @tparam Val the type of value
 */
template<typename Val>
class KVWorker {
 public:
  /**
   * @param id the unique identity which is used to find the KVStore at the
   * parameter server. Negative IDs is preserved by system.
   */
  explicit KVWorker(int id = 0) {
    cache_ = CHECK_NOTNULL((new KVCache<Key, Val>(id)));
  }
  ~KVWorker() {
    delete cache_;
  }

  /**************************************************************************
   *                          Basic Push and Pull
   **************************************************************************/

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
  int Push(const std::vector<Key>& keys,
           const std::vector<Val>& vals,
           const SyncOpts& opts = SyncOpts()) {
    // copy the data, then use the zero copy push
    return ZPush(std::make_shared<std::vector<Key>>(keys),
                 std::make_shared<std::vector<Val>>(vals), opts);
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
  int Pull(const std::vector<Key>& keys,
           std::vector<Val>* vals,
           const SyncOpts& opts = SyncOpts()) {
    // copy the data, then use the zero copy pull
    return ZPull(std::make_shared<std::vector<Key>>(keys), vals, opts);
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

  /**************************************************************************
   *                       Zero-copy Push and Pull
   **************************************************************************/

  /**
   * \brief zero-copy synchronization. Keys (and values for ZPush) will not be
   * copied to reduce the communication delay. Therefore, it is the user's
   * responsibility to keep the keys and values unchanged until the request is
   * finished, namely Wait(ts) returns or the callback is called.
   */
  int ZPush(const std::shared_ptr<std::vector<Key> >& keys,
            const std::shared_ptr<std::vector<Val> >& vals,
            const SyncOpts& opts = SyncOpts()) {
    return cache_->Push(GetTask(opts), SArray<Key>(keys),
                        SArray<Val>(vals), SArray<int>(), opts.callback);
  }

  int ZPull(const std::shared_ptr<std::vector<Key> >& keys,
            std::vector<Val>* vals,
            const SyncOpts& opts = SyncOpts()) {
    return cache_->Pull(GetTask(opts), SArray<Key>(keys),
                        CHECK_NOTNULL(vals), NULL, opts.callback);
  }

  /**************************************************************************
   *                Push and Pull with variable length values
   **************************************************************************/
  /**
   * @brief Pushes a list of key-value pairs where value can be arbitary
   * length.
   *
   * @param keys
   * @param vals
   * @param vals_size vals_size[i] stores the length of the i-th value
   * @param opts
   *
   * @return
   */
  int VPush(const std::vector<Key>& keys,
            const std::vector<Val>& vals,
            const std::vector<int>& vals_size,
            const SyncOpts& opts = SyncOpts()) {
    return ZVPush(std::make_shared<std::vector<Key>>(keys),
                  std::make_shared<std::vector<Val>>(vals),
                  std::make_shared<std::vector<int>>(vals_size), opts);
  }

  /**
   * @brief Pulls a list of key-value pairs where value can be arbitary
   * length.
   *
   * @param keys
   * @param vals
   * @param vals_size
   * @param opts
   *
   * @return
   */
  int VPull(const std::vector<Key>& keys,
            std::vector<Val>* vals,
            std::vector<int>* vals_size,
            const SyncOpts& opts = SyncOpts()) {
    return ZVPull(std::make_shared<std::vector<Key>>(keys),
                  CHECK_NOTNULL(vals), CHECK_NOTNULL(vals_size), opts);
  }

  /**************************************************************************
   *          Zero-copy Push and Pull with variable length values
   **************************************************************************/

  int ZVPush(const std::shared_ptr<std::vector<Key> >& keys,
             const std::shared_ptr<std::vector<Val> >& vals,
             const std::shared_ptr<std::vector<int> >& vals_size,
             const SyncOpts& opts = SyncOpts()) {
    return cache_->Push(GetTask(opts), SArray<Key>(keys), SArray<Val>(vals),
                        SArray<int>(vals_size), opts.callback);
  }

  int ZVPull(const std::vector<Key>& keys,
             std::vector<Val>* vals,
             std::vector<int>* vals_size,
             const SyncOpts& opts = SyncOpts()) {
    return cache_->Pull(GetTask(opts), SArray<Key>(keys), CHECK_NOTNULL(vals),
                        CHECK_NOTNULL(vals_size), opts.callback);
  }

 private:
  // /*!
  //  * \brief Increases the clock by delta
  //  */
  // void IncrClock(int delta = 1) {
  //   cache_->exector()->IncrClock(delta);
  // }

  Task GetTask(const SyncOpts& opts);
  KVCache<Key, Val>* cache_;
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
 * \brief An example of user-defined online handle.
 */
template <typename Val, typename SyncVal>
class IOnlineHandle {
 public:
  IOnlineHandle() { }
  virtual ~IOnlineHandle() { }

  /**
   * \brief Start to handle a request from a worker
   *
   * @param push true if this is a push request
   * @param timestamp the timestamp of this request
   * @param cmd the cmd specified in SyncOpts
   * @param msg the received message
   */
  inline void Start(bool push, int timestamp, int cmd, void* msg) { }

  /**
   * \brief The request has been handled
   */
  inline void Finish() { }

  /**
   * \brief Handle initialization, which only called once when allocating these
   * key-value pairs
   */
  inline void Init(Key key, Val& val) { }

  /**
   * \brief Handle PUSH requests from worker nodes
   *
   * @param recv_keys the keys received from a worker node
   * @param recv_vals the corresponding values received from the worker node
   * @param my_vals the corresponding local values
   */
  inline void Push(Key recv_key, Blob<const SyncVal> recv_val, Val& my_val) {
    for (const Val& v : recv_val) my_val += v;
  }

  /**
   * \brief Handle PUSH requests from worker nod
   *
   * @param recv_keys the keys received from a worker node
   * @param my_vals the corresponding local values
   * @param sent_vals the corresponding values will send to the worker node
   */
  inline void Pull(Key recv_key, const Val& my_val, Blob<SyncVal>& send_val) {
    for (Val& v : send_val) v = my_val;
  }

  /** \brief Accepts the caller for advanced usage */
  inline void SetCaller(void *obj) { }
};


/*!
 * \brief key-value store for server nodes in online mode.
 *
 * Individual key-value pairs received from workers are feed into the
 * user-defined handle one by one. It users unordered_map or other equivalence
 * data structure to store <Key,Val> pairs. It is suitable when new keys appears
 * during running, such as SGD/online learning algorithms. However, both read
 * and write could be 5x slower comparing to BatchServer
 *
 * @tparam Val the value type
 * @tparam SyncVal the data type for synchronization, which should be primitive
 * types such as int, float, ...
 * @Handle User-defined handles (or model updater)
 */
template <typename Val, typename SyncVal = Val,
          typename Handle = IOnlineHandle<Val, SyncVal> >
class OnlineServer {
 public:
  /**
   * @param pull_val_len the length of value pulled from server for each key. If
   * positive, then a fixed length SyncVal[pull_val_len] will be
   * pulled. Otherwise, the length is dynamic, and abs(pull_val_len) is the hint
   * length.
   * @param handle
   * @param id the unique identity. Negative IDs is preserved by system.
   */
  OnlineServer(
      const Handle& handle = Handle(), int pull_val_len = 1, int id = 0) {
    server_ = new KVStoreSparse<Key, Val, SyncVal, Handle>(
        id, handle, pull_val_len);
    Postoffice::instance().manager().TransferCustomer(CHECK_NOTNULL(server_));
  }
  ~OnlineServer() { }

  KVStore* server() { return server_; }
 private:
  KVStore* server_ = NULL;
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

DECLARE_int32(num_workers);
DECLARE_int32(num_servers);

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

// The rank ID of this node in its group. Assume this a worker node in a worker
// group with N workers. Then this node will be assigned an unique ID from 0,
// ..., N. Similarly for server and scheduler.
inline int MyRank() { return MyNode().rank(); }
// Total nodes in this node group.
inline int RankSize() {
  auto& mng = Postoffice::instance().manager();
  return IsWorkerNode() ? mng.num_workers() : (IsServerNode() ? mng.num_servers() : 1);
}

inline int NumWorkers() { return FLAGS_num_workers; }
inline int NumServers() { return FLAGS_num_servers; }

inline void StartSystem(int* argc, char ***argv) {
  ps::Postoffice::instance().Run(argc, argv);
}

inline void StopSystem() {
  ps::Postoffice::instance().Stop();
}

inline int RunSystem(int* argc, char ***argv) {
  StartSystem(argc, argv); StopSystem();
  return 0;
}

}  // namespace ps

// Implementation

#include <ps/ps-inl.h>
