/**
 * @file   server.h
 * @brief  server node apis
 */
#pragma once
#include <memory>
#include "ps/base.h"
#include "ps/blob.h"
#include "proto/task.pb.h"
#include "kv/kv_store_sparse.h"
#include "kv/kv_store_sparse_st.h"
// #include "kv/kv_store_cuckoo.h"
namespace ps {

/**
 * @brief An example of the user-defined value for \ref OnlineServer
 *
 * The constructor function is called when the according key does not
 * exist. This class must be copyable and assignable (movable), which are only
 * called once with the constructor function, e.g. ``std::unordered_map::insert(IVal<Val>())``. It
 * also must implement the following three functions \ref Load, \ref Save and
 * \ref Empty
 */
template <typename Val>
struct IVal {

  /// \brief value
  Val w = 0;

  /// \brief Load from disk
  inline void Load(dmlc::Stream *fi) { fi->Read(&w, sizeof(Val)); }

  /// \brief Save to disk
  inline void Save(dmlc::Stream *fo) const { fo->Write(&w, sizeof(Val)); }

  /// \brief Return true if this value can be skipped from saving to disk
  inline bool Empty() const { return w == 0; }
};

/**
 * \brief An example of user-defined handle for \ref OnlineServer
 *
 * Assume OnlineServer stores the KV pairs in `map<Key, Val> store_`. It
 * processes a push request with \a keys and \a vals as following
 *
 * \code
 * handle_.Start(true, ...);
 * for (k : keys) handle_.Push(k, vals[k], store_[k]);
 * handle_.Finish();
 * \endcode
 *
 * A server node processes one request (either push or pull) at a time, the
 * above codes can be almost treated as a transaction. But the middle for loop
 * may run in parallel with the number of threads specified in \ref
 * OnlineServer.
 *
 * The procedure for a pull request is similar.
 *
 * \code
 * handle_.Start(true, ...);
 * for (k : keys) {
 *   handle_.Pull(k, store_[k], v);
 *   vals.push_back(v);
 * }
 * handle_.Finish();
 * // send back vals
 * \endcode
 *
 * \a v consists of a `SyncVal` pointer `v.data` with length `v.size =
 * pull_val_len`. One can reuse this buffer is if the actually value length for
 * sending is not larger than `v.size` (change `v.size` if less than). One can
 * also modify `v.data`, the system will `memcpy` it to it's internal
 * buffer. A sample usage:
 *
 * \code
 * struct MyVal { std::vector<Val> w; }
 * void Pull(Key recv_key, MyVal& my_val, ps::Blob<Val>& send_val) {
 *   send_val.data = my_val.w.data();
 *   send_val.size = my_val.w.size();
 * }
 * \endcode
 */
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

/**
 * \brief The online key-value store for server nodes
 *
 * A server node maintains KV pairs in a particular key range, and responses the
 * push and pull requests from worker nodes. It allows user-defined value type
 * and handle.
 *
 * @tparam SyncV the value type used for synchronization, which should be
 * primitive types such as int, float. It should by the same as the value type
 * defined in \ref ps::KVWorker
 * @tparam Val the value type stored in server, which could be a
 * user-defined type, see \ref IVal for more details
 * @tparam Handle User-defined handle for processing push and pull request from
 * workers, see \ref IOnlineHandle for more details
 */
template <typename SyncV,
          typename Val = IVal<SyncV>,
          typename Handle = IOnlineHandle<SyncV> >
class OnlineServer {
 public:
  /**
   * \brief Creates a KV store.
   *
   * @param handle the user-defined handle
   * @param pull_val_len the hint of the length of value pulled from server for each
   * key.
   * @param num_threads the number of threads can be used to process a worker
   * request
   * @param id the unique identity. It should match the according id of \ref
   * KVWorker
   */
  OnlineServer(const Handle& handle = Handle(),
               int pull_val_len = 1,
               int num_threads = 1,
               int id = NextID()) {
    if (num_threads == 1) {
      server_ = new KVStoreSparseST<Key, Val, SyncV, Handle>(
          id, handle, pull_val_len);
    } else {
      server_ = new KVStoreSparse<Key, Val, SyncV, Handle>(
          id, handle, pull_val_len, num_threads);
    }
    // server_ = new KVStoreCuckoo<Key, Val, SyncV, Handle>(
    //     id, handle, pull_val_len, num_threads);
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
 * All flags and their arguments (e.g. -logtostderr 1) has been parsed and
 * removed from argc and argv, but commandline arguments are remained such as
 * data=my_data.txt
 */
int CreateServerNode(int argc, char *argv[]);
