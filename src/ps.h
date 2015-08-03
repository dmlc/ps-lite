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
#include "dmlc/io.h"

///////////////////////////////////////////////////////////////////////////////
///                              Worker node APIs                           ///
///////////////////////////////////////////////////////////////////////////////
namespace ps {

/// \brief  Advanced synchronization options for a worker request (push or pull)
struct SyncOpts {

  /// \brief Depended timestamps.
  ///
  /// \a deps places an execution order. This request will be executed on the
  /// server nodes only after all requests sent by this worker with timestamps
  /// specified in \a deps have been executed.

  std::vector<int> deps;

  /// \brief The callback for this request is finished
  ///
  /// The callback will be called after this request is actually finished. In
  /// other words, all ack messages have been received for a push or all values
  /// have been pulled back for a pull.  Semantically, it is almost the same
  /// between
  ///
  /// \code
  ///    Wait(Push(keys, vals)); func();
  /// \endcode
  /// and
  /// \code
  ///   SyncOpts opt; opt.callback = func;
  ///   Wait(Push(keys, vals, opt));
  /// \endcode
  ///
  /// The subtle difference is that, in the latter, \a func is executed before
  /// \ref Wait returns, and it is executed by a different thread (KVworker's
  /// data receiving thread).

  std::function<void()> callback;

  /// \brief Filters to compression data
  /// \sa ps::IFilter AddFilter

  std::vector<Filter> filters;

  /// \brief Add a filter to this request
  ///
  /// For example, use LZ4 to compress the values:
  /// \code AddFilter(Filter::COMPRESSING); \endcode

  Filter* AddFilter(Filter::Type type);

  /// \brief The command that will be passed to the server handle.
  /// \sa ps::IOnlineHandle::Start

  int cmd = 0;
};

/// \brief Provides Push and Pull for worker nodes
///
/// This class provides \ref Push and \ref Pull with several variants for worker
/// nodes.
///
/// \tparam Val the type of value, which should be primitive types such as int32_t and float
/// are supported
template<typename Val>
class KVWorker {
 public:
  /// \brief Creates with an unique identity
  ///
  /// A worker node can have multiple \ref KVWorker. They are distinguished with
  /// each other via the identities. Furthermore, it communicates with the
  /// object (such as \ref OnlineServer) on a remote node with the same
  /// identity. If such object does not exist on the receiver node, the system
  /// will produce a fatal message.
  ///
  /// \param id the unique identity, negative IDs are preserved by system.
  explicit KVWorker(int id = 0) {
    cache_ = CHECK_NOTNULL((new KVCache<Key, Val>(id)));
  }

  ~KVWorker() { delete cache_; }

  /// \brief Pushes a list of key-value pairs to all server nodes.
  ///
  /// This function pushes a KV list specified by \a keys and \a vals to all
  /// server nodes. The length of a value,  which is
  /// determined by `k=vals.size()/keys.size()`, can be more than one but should
  /// be a constant. The \a i-th KV pair is
  ///
  /// \verbatim {keys[i], (vals[i*k], ..., vals[(i+1)*k-1])} \endverbatim
  ///
  /// This list may be sliced according to the key ranges server nodes
  /// handling and be pushed to multiple servers
  ///
  /// This function is a non-blocking call, which returns immediately once the
  /// message is queued in the system's sending buffer. The actual pushing is
  /// finished only if \ref Wait returns or the callback specified in \ref
  /// opts is called.
  ///
  ///
  /// Sample usage: the following codes push two KV pairs `{1, (1.1, 1.2)}` and `{3,
  /// (3.1,3.2)}` to server nodes, where the value is a length-2 float vector
  /// \code
  ///   KVWorker<float> w;
  ///   std::vector<Key> keys = {1, 3};
  ///   std::vector<float> vals = {1.1, 1.2, 3.1, 3.2};
  ///   w.Push(keys, vals);
  /// \endcode
  ///
  /// @param keys a list of keys, which must be sorted
  /// @param vals the according values
  /// @param opts push options
  /// @return the timestamp of this request
  ///
  /// \note Both keys and values will be copied into a system buffer, the
  /// zero-copy version \ref ZPush might be more efficient
  /// \note Use \ref VPush to push dynamic length values

  int Push(const std::vector<Key>& keys,
           const std::vector<Val>& vals,
           const SyncOpts& opts = SyncOpts()) {
    // copy the data, then call the zero-copy push
    return ZPush(std::make_shared<std::vector<Key>>(keys),
                 std::make_shared<std::vector<Val>>(vals), opts);
  }

  /// \brief Pulls the values associated with the keys from the server nodes
  ///
  /// This function pulls the values of the keys specified in \a keys from the
  /// server nodes.
  ///
  /// It's a non-blocking call, which returns immediately once the message is
  /// queued in the system's sending buffer. The actual pulling is finished,
  /// namely \a vals is filled with pulled values, only
  /// if \ref Wait returns or the callback specified in opts is called.
  ///
  /// Sample usage: the following codes pull the values of keys \a 1 and \a 3
  /// from the server nodes.
  /// \code
  ///   KVWorker<float> w;
  ///   std::vector<Key> keys = {1, 3};
  ///   std::vector<float> vals;
  ///   ps.Pull(keys, &vals);
  /// \endcode
  ///
  /// @param keys a list of keys, which must be sorted
  /// @param vals the buffer for the pulled values. It can be empty.
  /// @param opts pull options
  /// @return the timestamp of this request
  ///
  /// \note Both keys and values will be copied into a system buffer, the
  /// zero-copy version \ref ZPull might be more efficient
  /// \note Use \ref VPull to pull dynamic length values

  int Pull(const std::vector<Key>& keys,
           std::vector<Val>* vals,
           const SyncOpts& opts = SyncOpts()) {
    // copy the data, then use the zero-copy pull
    return ZPull(std::make_shared<std::vector<Key>>(keys), vals, opts);
  }

  /// \brief Waits until a push or pull has been finished
  ///
  /// Sample usage:
  /// \code
  ///   int ts = w.Pull(keys, &vals);
  ///   Wait(ts);
  ///   // now vals is ready for use
  /// \endcode
  ///
  /// \param timestamp the timestamp of the request waiting for

  void Wait(int timestamp) { cache_->Wait(timestamp); }

  /// @brief Extends \ref Push to dynamic length values
  ///
  /// This function is similar to \ref Push except that there is additional \a
  /// vals_size where `vals_size[i]` stores the value length of the \a i-th KV
  /// pair. In other words, assume `n = vals_size[0] + .. + vals_size[i-1]`,
  /// then the \a i-th KV pair is presented as
  ///
  /// \verbatim {keys[i], (vals[n], ..., vals[vals_size[i]+n-1])} \endverbatim
  ///
  /// @param keys a list of keys, which must be sorted
  /// @param vals the according values
  /// @param vals_size vals_size[i] stores the value length of the \a i-th KV pair
  /// @param opts push options
  /// @return the timestamp of this request
  ///
  /// \note Both keys and values will be copied into a system buffer, the
  /// zero-copy version \ref ZVPush might be more efficient

  int VPush(const std::vector<Key>& keys,
            const std::vector<Val>& vals,
            const std::vector<int>& vals_size,
            const SyncOpts& opts = SyncOpts()) {
    // copy data, then call the zero-copy push
    return ZVPush(std::make_shared<std::vector<Key>>(keys),
                  std::make_shared<std::vector<Val>>(vals),
                  std::make_shared<std::vector<int>>(vals_size), opts);
  }

  /// @brief Extends \ref Pull to dynamic length values
  ///
  /// This function is similar to \ref Pull except that an additional \a
  /// vals_size is pulled, where `vals_size[i]` stores the value length of the
  /// \a i-th KV pair
  ///
  /// @param keys a list of keys, which must be sorted
  /// @param vals the buffer for the pulled values. It can be empty.
  /// @param vals_size the buffer for the pulled value lengths. It can be empty.
  /// @param opts pull options
  /// @return the timestamp of this request
  ///
  /// \note Both keys and values will be copied into a system buffer, the
  /// zero-copy version \ref ZVPull might be more efficient

  int VPull(const std::vector<Key>& keys,
            std::vector<Val>* vals,
            std::vector<int>* vals_size,
            const SyncOpts& opts = SyncOpts()) {
    // copy data, then call the zero-copy pull
    return ZVPull(std::make_shared<std::vector<Key>>(keys),
                  CHECK_NOTNULL(vals), CHECK_NOTNULL(vals_size), opts);
  }

  /// \brief The zero-copy version of \ref Push
  ///
  /// This function is similar to \ref Push except that both \a keys and \a vals
  /// will not be copied to avoid the possible overhead of `memcpy`. It is
  /// achieved via 'std::shared_ptr'. The system will store a copy of the shared
  /// points until the push has been finished. Before that, it is the caller's
  /// responsibility to keep the content to be not changed.
  ///
  /// Sample usage:
  /// \code
  /// using namespace std;
  /// auto keys = new vector<Key>{1, 3};
  /// auto vals = new vector<float>{1.1, 1.2, 3.1, 3.2};
  /// KVWorker<float> w;
  /// w.ZPush(shared_ptr<vector<Key>>(keys), shared_ptr<vector<float>>(vals));
  /// // the system will delete keys and vals
  /// \endcode
  ///
  /// @param keys a list of keys, which must be sorted
  /// @param vals the according values
  /// @param opts push options
  /// @return the timestamp of this request

  int ZPush(const std::shared_ptr<std::vector<Key> >& keys,
            const std::shared_ptr<std::vector<Val> >& vals,
            const SyncOpts& opts = SyncOpts()) {
    return cache_->Push(GetTask(opts), SArray<Key>(keys),
                        SArray<Val>(vals), SArray<int>(), opts.callback);
  }

  /// \brief The zero-copy version of \ref Pull
  ///
  /// This function is similar to \ref Pull except that \a keys
  /// will not be copied to avoid the possible overhead of `memcpy`. It is
  /// achieved via 'std::shared_ptr'. The system will store a copy of the shared
  /// points until the pull has been finished. Before that, it is the caller's
  /// responsibility to keep the content to be not changed.
  ///
  /// Sample usage:
  /// \code
  /// using namespace std;
  /// auto keys = new vector<Key>{1, 3};
  /// auto vals = new vector<float>();
  /// KVWorker<float> w;
  /// w.ZPush(shared_ptr<vector<Key>>(keys), vals);
  /// // the system will delete keys
  /// \endcode
  ///
  /// @param keys a list of keys, which must be sorted
  /// @param vals the buffer for the pulled values. It can be empty.
  /// @param opts pull options
  /// @return the timestamp of this request
  ///
  int ZPull(const std::shared_ptr<std::vector<Key> >& keys,
            std::vector<Val>* vals,
            const SyncOpts& opts = SyncOpts()) {
    return cache_->Pull(GetTask(opts), SArray<Key>(keys),
                        CHECK_NOTNULL(vals), NULL, opts.callback);
  }


  /// \brief The zero-copy version of \ref VPush
  int ZVPush(const std::shared_ptr<std::vector<Key> >& keys,
             const std::shared_ptr<std::vector<Val> >& vals,
             const std::shared_ptr<std::vector<int> >& vals_size,
             const SyncOpts& opts = SyncOpts()) {
    return cache_->Push(GetTask(opts), SArray<Key>(keys), SArray<Val>(vals),
                        SArray<int>(vals_size), opts.callback);
  }

  /// \brief The zero-copy version of \ref VPull
  int ZVPull(const std::shared_ptr<std::vector<Key> >& keys,
             std::vector<Val>* vals,
             std::vector<int>* vals_size,
             const SyncOpts& opts = SyncOpts()) {
    return cache_->Pull(GetTask(opts), SArray<Key>(keys), CHECK_NOTNULL(vals),
                        CHECK_NOTNULL(vals_size), opts.callback);
  }

 private:
  Task GetTask(const SyncOpts& opts);
  KVCache<Key, Val>* cache_;
};
}  // namespace ps


/**
 * \brief The main function for a worker node
 *
 * All flags and their arguments (e.g. -logtostderr 1) has been parsed and
 * removed from argc and argv using GFlags, but commandline arguments are remained
 */
int WorkerNodeMain(int argc, char *argv[]);

///////////////////////////////////////////////////////////////////////////////
///                             Server node APIs                            ///
///////////////////////////////////////////////////////////////////////////////
namespace ps {


/// \brief An example of the user-defined value for \ref OnlineServer
///
/// It must implement \ref Load and \ref Save
template <typename Val>
struct IVal {

  /** \brief value */
  Val w = 0;

  /** \brief Load from disk, return false if load failed (end of the file) */
  inline bool Load(dmlc::Stream *fi) {
    return fi->Read(&w, sizeof(Val)) == sizeof(Val);
  }

  /** \brief Save to disk, return false if it can be skipped  */
  inline bool Save(dmlc::Stream *fo) const {
    if (w == 0) return false;
    fo->Write(&w, sizeof(Val)); return true;
  }
};

/// \brief An example of user-defined handle for \ref OnlineServer
template <typename SyncV>
class IOnlineHandle {
 public:
  IOnlineHandle() { }
  virtual ~IOnlineHandle() { }

  /**
   * \brief Start to handle a request from a worker
   *
   * @param push true if this is a push request
   * @param timestamp the timestamp of this request
   * @param cmd the cmd specified in \ref SyncOpts
   * @param msg the received message
   */
  inline void Start(bool push, int timestamp, int cmd, void* msg) { }

  /**
   * \brief The request has been handled
   */
  inline void Finish() { }

  /**
   * \brief Handle a push request from a worker node
   *
   * @param recv_key the key received from a worker node
   * @param recv_vals the corresponding values received from the worker node
   * @param my_vals the corresponding local stored value
   */
  inline void Push(
      Key recv_key, Blob<const SyncV> recv_val, IVal<SyncV>& my_val) {
    for (const SyncV& v : recv_val) my_val.w += v;
  }

  /**
   * \brief Handle a pull requests from a worker node
   *
   * @param recv_key the key received from a worker node
   * @param my_vals the corresponding local value
   * @param sent_vals the corresponding values will send to the worker node
   */
  inline void Pull(
      Key recv_key, const IVal<SyncV>& my_val, Blob<SyncV>& send_val) {
    for (SyncV& v : send_val) v = my_val.w;
  }

  /** \brief Load from disk */
  inline void Load(dmlc::Stream *fi) { }

  /** \brief Save to disk */
  inline void Save(dmlc::Stream *fo) const { }
};

/// \brief The online key-value store for server nodes
///
/// A server node maintains KV pairs in a particular key range, and responses the
/// push and pull requests from worker nodes. It allows user-defined value type
/// and handle.
///
/// @tparam SyncV the value type used for synchronization, which should be
/// primitive types such as int, float. It should by the same as the value type
/// defined in \ref ps::KVWorker
/// @tparam Val the value type stored in server, which could be a complex
/// user-defined type, see \ref IVal for an example
/// @tparam Handle User-defined handle for processing push and pull request from
/// workers, see \ref IOnlineHandle for an example
template <typename SyncV,
          typename Val = IVal<SyncV>,
          typename Handle = IOnlineHandle<SyncV> >
class OnlineServer {
 public:
  /// \brief Creates a KV store.
  ///
  /// @param handle the user-defined handle
  /// @param pull_val_len the hint of the length of value pulled from server for each
  /// key.
  /// @param id the unique identity. It should matches the according id of \ref
  /// KVWorker
  OnlineServer(const Handle& handle = Handle(),
               int pull_val_len = 1,
               int id = 0) {
    server_ = new KVStoreSparse<Key, Val, SyncV, Handle>(
        id, handle, pull_val_len);
    Postoffice::instance().manager().TransferCustomer(CHECK_NOTNULL(server_));
  }

  ~OnlineServer() { }

  /// \brief Returns the pointer of the actual KV store
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
///                            Helper class                                 ///
///////////////////////////////////////////////////////////////////////////////
#include "ps/app.h"
namespace ps {

DECLARE_int32(num_workers);
DECLARE_int32(num_servers);

/// A helper class to query info about the node
class NodeInfo {
 public:
  static inline int MyRank() { return MyNode().rank(); }

  static inline int NumWorkers() { return FLAGS_num_workers; }

  static inline int NumServers() { return FLAGS_num_servers; }

  static inline int RankSize() {
    return IsWorker() ? NumWorkers() : (IsServer() ? NumServers() : 1);
  }

  static bool IsWorker() { return MyNode().role() == Node::WORKER; }

  /// \brief Return true if this node is a server node.
  static inline int IsServer() { return MyNode().role() == Node::SERVER; }

  /*! \brief Return true if this node is a scheduler node. */
  static inline int IsScheduler() { return MyNode().role() == Node::SCHEDULER; }

  static inline Node MyNode() { return Postoffice::instance().manager().van().my_node(); }
};

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
