/*!
 * @file   ps.h
 * \brief  The parameter server interface
 */
#pragma once
#include "base/base.h"
#include "base/blob.h"
#include "proto/filter.pb.h"
///////////////////////////////////////////////////////////////////////////////
///                              Worker node APIs                           ///
///////////////////////////////////////////////////////////////////////////////

/**
 * \brief The main function for a worker node
 *
 * All flags and their arguments (e.g. -logtostderr 1) has been parsed and removed
 * from argc and argv, but commandline arguments are remained such as data=my_data.txt
 */
int WorkerNodeMain(int argc, char *argv[]);

namespace ps {
/**
 * \brief Options for Push and Pull
 */
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
   * \brief A helper function. Sample usage: AddFilter(FilterConfig::COMPRESSING);
   */
  Filter& AddFilter(Filter::Type type) {
    filters.push_back(Filter());
    filters.back().set_type(type);
    return filters.back();
  }
};

template<typename K, typename V> class KVCache;
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
  explicit KVWorker(int id = 0);
  ~KVWorker();

  /*!
   * \brief Pushes a list of key-value pairs into the parameter server
   *
   * It's a non-blocking call, which returns immediately once the message is
   * queued in the system's sending buffer. The actual push is finished only
   * after Wait(returned_timestamp) returns or the provided callback is called.
   *
   * Both keys and values will be copied, using the SArray version for zero-copy
   * pushing.
   *
   * Sample usage: assume we have two key-value pairs {1, (1.1, 1.2)}, {3,
   * (3.1,3.2)}, where the value is a 2-length float vector. We then can push these
   * two pairs into the parameter server:
   \code
     KVWorker<float> cache(0);
     std::vector<Key> keys = {1, 3};
     std::vector<float> vals = {1.1, 1.2, 3.1, 3.2};
     cache.Push(keys, vals);
   \endcode
   *
   * @param keys a list of keys
   * @param values a list of values, whose size should be an integer multiple
   * the key size
   *
   * @return the timestamp of this request.
   */
  int Push(const std::vector<Key>& keys, const std::vector<V>& values,
           const SyncOpts& opts = SyncOpts()) {
    return Push(CBlob<Key>(keys), CBlob<V>(values), opts);
  }

  /*!
   * \brief Pulls the values associated with the keys from the parameter server
   *
   * It's a non-blocking call, which returns immediately once the message is
   * queued in the system's sending buffer. The actual push is finished only
   * after Wait(returned_timestamp) returns or the provided callback is called.
   *
   * Keys will be copied, using the SArray version for zero-copy pushing.
   *
   * @param keys a list of keys
   * @param values the buffer for the pulled values, which should be pre-allocated
   *
   * Sample usage: again assume each key is associated with a 2-length float
   * vector value. We then can pull the newest value from the parameter server:
   \code
     KVWorker<float> cache(0);
     std::vector<Key> keys = {1, 3};
     std::vector<float> vals(4);
     cache.Pull(keys, &vals);
   \endcode
   * @return the timestamp of this request
   */
  int Pull(const std::vector<Key>& keys, std::vector<V>* values,
           const SyncOpts& opts = SyncOpts()) {
    return Pull(CBlob<Key>(keys), Blob<V>(*values), opts);
  }

  /*!
   * \brief Waits until a request has been finished
   *
   * Sample usage:
   \code
     int ts = cache.Pull(keys, &vals);
     Wait(ts);
     // now vals is ready for use
   \endcode
   */
  void Wait(int timestamp);

  /*! \brief Blob style Push and Pull */

  int Push(CBlob<Key> keys, CBlob<V> values, const SyncOpts& opts = SyncOpts());
  int Pull(CBlob<Key> keys, Blob<V> values, const SyncOpts& opts = SyncOpts());

  /**
   * \brief zero-copy synchronization. Keys (and values) will not be copied to
   * reduce the communication delay. Therefore, it is the user's responsibility
   * to keep the keys and values unchanged until the request is finished, namely
   * Wait(ts) returns or the callback is called.
   */
  int ZPush(const SBlob<Key>& keys, const SBlob<V>& values,
           const SyncOpts& opts = SyncOpts());

  int ZPull(const SBlob<Key>& keys, SBlob<V>* values,
           const SyncOpts& opts = SyncOpts());

  /*!
   * \brief Increases the clock by delta
   */
  void IncrClock(int delta = 1);
 private:
  KVCache<Key, V>* cache_;
};
}  // namespace ps

///////////////////////////////////////////////////////////////////////////////
///                             Server node APIs                            ///
///////////////////////////////////////////////////////////////////////////////

/**
 * \brief The main function for a server node
 *
 * All flags and their arguments (e.g. -logtostderr 1) has been parsed and removed
 * from argc and argv, but commandline arguments are remained such as data=my_data.txt
 */
int CreateServerNode(int argc, char *argv[]);

namespace ps {
/**
 * \brief An example of user-defineable handle. See more handle examples in
 * ps_server_handle.h
 * \tparam V the value type
 */
template <typename V>
class IHandle {
 public:
  IHandle() { }
  virtual ~IHandle() { }

  /**
   * \brief Handle PUSH requests from worker nodes
   *
   * @param recv_keys the keys received from a worker node
   * @param recv_vals the corresponding values received from the worker node
   * @param my_vals the corresponding local values
   */
  inline void HandlePush(int ts, CBlob<Key> recv_keys, CBlob<V> recv_vals,
                         Blob<V>* my_vals) {
    for (size_t i = 0; i < recv_vals.size; ++i)
      (*my_vals)[i] += recv_vals[i];
  }
  /**
   * \brief Handle PUSH requests from worker nod
   *
   * @param recv_keys the keys received from a worker node
   * @param my_vals the corresponding local values
   * @param sent_vals the corresponding values will send to the worker node
   */
  inline void HandlePull(int ts, CBlob<Key> recv_keys, CBlob<V> my_vals,
                         Blob<V>* send_vals) {
    for (size_t i = 0; i < my_vals.size; ++i)
      (*send_vals)[i] = my_vals[i];
  }

  /**
   * \brief Initialize local values
   */
  inline void HandleInit(int ts, CBlob<Key> keys, Blob<V>* vals) {
    memset(vals->data, 0, vals->size*sizeof(V));
  }
};


static const int kDynamicValue = -1;
class KVStore;
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

  KVStore* Run();
 private:
  int id_;
  Type type_;
  int sync_val_len_;
  Handle handle_;
};



///////////////////////////////////////////////////////////////////////////////
///                            Scheduler Node APIs                          ///
///////////////////////////////////////////////////////////////////////////////
// TODO
}  // namespace ps

/// implementation
#include "ps-inl.h"

///////////////////////////////////////////////////////////////////////////////
///                            More Advanced APIs                           ///
///////////////////////////////////////////////////////////////////////////////
#include "system/customer.h"
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
