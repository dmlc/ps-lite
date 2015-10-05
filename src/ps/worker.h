/**
 * @file   worker.h
 * @brief  worker node APIs
 */
#pragma once
#include <functional>
#include <memory>
#include "ps/base.h"
#include "ps/blob.h"
#include "kv/kv_cache.h"
#include "proto/task.pb.h"
#include "proto/filter.pb.h"
#include "ps/shared_array.h"

namespace ps {

/// \brief  Advanced synchronization options for a worker request (push or pull)
struct SyncOpts {
  /**
   * \brief Depended timestamps.
   *
   * \a deps places an execution order. This request will be executed on the
   * server nodes only after all requests sent by this worker with timestamps
   * specified in \a deps have been executed.
   */
  std::vector<int> deps;

  /**
   * \brief The callback for this request is finished
   *
   * The callback will be called after this request is actually finished. In
   * other words, all ack messages have been received for a push or all values
   * have been pulled back for a pull.  Semantically, it is almost the same
   * between
   *
   * \code
   *    Wait(Push(keys, vals)); func();
   * \endcode
   * and
   * \code
   *   SyncOpts opt; opt.callback = func;
   *   Wait(Push(keys, vals, opt));
   * \endcode
   *
   * The subtle difference is that, in the latter, \a func is executed before
   * \ref Wait returns, and it is executed by a different thread (KVworker's
   * data receiving thread).
   */
  std::function<void()> callback;

  /**
   * \brief Filters to compression data
   * \sa ps::IFilter AddFilter
   */
  std::vector<Filter> filters;

  /**
   * \brief Add a filter to this request
   *
   * For example, use LZ4 to compress the values:
   * \code AddFilter(Filter::COMPRESSING); \endcode
   */
  Filter* AddFilter(Filter::Type type);

  /**
   * \brief The command that will be passed to the server handle.
   * \sa ps::IOnlineHandle::Start
   */
  int cmd = 0;

  /**
   * \brief Returns the according system Task
   */
  Task GetTask() const;
};

/**
 * \brief Communicate server nodes with key-value pairs
 *
 * This class provides \ref Push and \ref Pull with several variants for worker
 * nodes. See the \ref OnlineServer of server APIs
 *
 * \tparam Val the type of value, which should be primitive types such as
 * int32_t and float
 */
template<typename Val>
class KVWorker {
 public:
  /**
   * \brief Creates with an unique identity
   *
   * A worker node can have multiple \ref KVWorker. They are distinguished with
   * each other via the identities. Furthermore, it communicates with the
   * object (such as \ref OnlineServer) on a remote node with the same
   * identity. If such object does not exist on the receiver node, the system
   * will produce a fatal message.
   *
   * \param id the unique identity, negative IDs are preserved by system.
   */
  explicit KVWorker(int id = NextID()) {
    cache_ = CHECK_NOTNULL((new KVCache<Key, Val>(id)));
  }

  ~KVWorker() { delete cache_; }

  /**
   * \brief Pushes a list of key-value pairs to all server nodes.
   *
   * This function pushes a KV list specified by \a keys and \a vals to all
   * server nodes. The length of a value,  which is
   * determined by `k=vals.size()/keys.size()`, can be more than one but should
   * be a constant. The \a i-th KV pair is
   *
   * \verbatim {keys[i], (vals[i*k], ..., vals[(i+1)*k-1])} \endverbatim
   *
   * This list may be sliced according to the key ranges server nodes
   * handling and be pushed to multiple servers
   *
   * This function is a non-blocking call, which returns immediately once the
   * message is queued in the system's sending buffer. The actual pushing is
   * finished only if \ref Wait returns or the callback specified in \ref
   * opts is called.
   *
   *
   * Sample usage: the following codes push two KV pairs `{1, (1.1, 1.2)}` and `{3,
   * (3.1,3.2)}` to server nodes, where the value is a length-2 float vector
   * \code
   *   KVWorker<float> w;
   *   std::vector<Key> keys = {1, 3};
   *   std::vector<float> vals = {1.1, 1.2, 3.1, 3.2};
   *   w.Push(keys, vals);
   * \endcode
   *
   * @param keys a list of keys, which must be sorted
   * @param vals the according values
   * @param opts push options
   * @return the timestamp of this request
   *
   * \note Both keys and values will be copied into a system buffer, the
   * zero-copy version \ref ZPush might be more efficient
   * \note Use \ref VPush to push dynamic length values
   */
  int Push(const std::vector<Key>& keys,
           const std::vector<Val>& vals,
           const SyncOpts& opts = SyncOpts()) {
    // copy the data, then call the zero-copy push
    return ZPush(std::make_shared<std::vector<Key>>(keys),
                 std::make_shared<std::vector<Val>>(vals), opts);
  }

  /**
   * \brief Pulls the values associated with the keys from the server nodes
   *
   * This function pulls the values of the keys specified in \a keys from the
   * server nodes.
   *
   * It's a non-blocking call, which returns immediately once the message is
   * queued in the system's sending buffer. The actual pulling is finished,
   * namely \a vals is filled with pulled values, only
   * if \ref Wait returns or the callback specified in opts is called.
   *
   * Sample usage: the following codes pull the values of keys \a 1 and \a 3
   * from the server nodes.
   * \code
   *   KVWorker<float> w;
   *   std::vector<Key> keys = {1, 3};
   *   std::vector<float> vals;
   *   ps.Pull(keys, &vals);
   * \endcode
   *
   * @param keys a list of keys, which must be sorted
   * @param vals the buffer for the pulled values. It can be empty.
   * @param opts pull options
   * @return the timestamp of this request
   *
   * \note Both keys and values will be copied into a system buffer, the
   * zero-copy version \ref ZPull might be more efficient
   * \note Use \ref VPull to pull dynamic length values
   */
  int Pull(const std::vector<Key>& keys,
           std::vector<Val>* vals,
           const SyncOpts& opts = SyncOpts()) {
    // copy the data, then use the zero-copy pull
    return ZPull(std::make_shared<std::vector<Key>>(keys), vals, opts);
  }

  /**
   * \brief Waits until a push or pull has been finished
   *
   * Sample usage:
   * \code
   *   int ts = w.Pull(keys, &vals);
   *   Wait(ts);
   *   // now vals is ready for use
   * \endcode
   *
   * \param timestamp the timestamp of the request waiting for
   */
  void Wait(int timestamp) { cache_->Wait(timestamp); }

  /**
   * @brief Extends \ref Push to dynamic length values
   *
   * This function is similar to \ref Push except that there is additional \a
   * vals_size where `vals_size[i]` stores the value length of the \a i-th KV
   * pair. In other words, assume `n = vals_size[0] + .. + vals_size[i-1]`,
   * then the \a i-th KV pair is presented as
   *
   * \verbatim {keys[i], (vals[n], ..., vals[vals_size[i]+n-1])} \endverbatim
   *
   * @param keys a list of keys, which must be sorted
   * @param vals the according values
   * @param vals_size vals_size[i] stores the value length of the \a i-th KV pair
   * @param opts push options
   * @return the timestamp of this request
   *
   * \note Both keys and values will be copied into a system buffer, the
   * zero-copy version \ref ZVPush might be more efficient
   */
  int VPush(const std::vector<Key>& keys,
            const std::vector<Val>& vals,
            const std::vector<int>& vals_size,
            const SyncOpts& opts = SyncOpts()) {
    // copy data, then call the zero-copy push
    return ZVPush(std::make_shared<std::vector<Key>>(keys),
                  std::make_shared<std::vector<Val>>(vals),
                  std::make_shared<std::vector<int>>(vals_size), opts);
  }

  /**
   * @brief Extends \ref Pull to dynamic length values
   *
   * This function is similar to \ref Pull except that an additional \a
   * vals_size is pulled, where `vals_size[i]` stores the value length of the
   * \a i-th KV pair
   *
   * @param keys a list of keys, which must be sorted
   * @param vals the buffer for the pulled values. It can be empty.
   * @param vals_size the buffer for the pulled value lengths. It can be empty.
   * @param opts pull options
   * @return the timestamp of this request
   *
   * \note Both keys and values will be copied into a system buffer, the
   * zero-copy version \ref ZVPull might be more efficient
   */
  int VPull(const std::vector<Key>& keys,
            std::vector<Val>* vals,
            std::vector<int>* vals_size,
            const SyncOpts& opts = SyncOpts()) {
    // copy data, then call the zero-copy pull
    return ZVPull(std::make_shared<std::vector<Key>>(keys),
                  CHECK_NOTNULL(vals), CHECK_NOTNULL(vals_size), opts);
  }
  /**
   * \brief The zero-copy version of \ref Push
   *
   * This function is similar to \ref Push except that both \a keys and \a vals
   * will not be copied to avoid the possible overhead of `memcpy`. It is
   * achieved via 'std::shared_ptr'. The system will store a copy of the shared
   * points until the push has been finished. Before that, it is the caller's
   * responsibility to keep the content to be not changed.
   *
   * Sample usage:
   * \code
   * using namespace std;
   * auto keys = new vector<Key>{1, 3};
   * auto vals = new vector<float>{1.1, 1.2, 3.1, 3.2};
   * KVWorker<float> w;
   * w.ZPush(shared_ptr<vector<Key>>(keys), shared_ptr<vector<float>>(vals));
   * // the system will delete keys and vals
   * \endcode
   *
   * @param keys a list of keys, which must be sorted
   * @param vals the according values
   * @param opts push options
   * @return the timestamp of this request
   */
  int ZPush(const std::shared_ptr<std::vector<Key> >& keys,
            const std::shared_ptr<std::vector<Val> >& vals,
            const SyncOpts& opts = SyncOpts()) {
    return cache_->Push(opts.GetTask(), SArray<Key>(keys),
                        SArray<Val>(vals), SArray<int>(), opts.callback);
  }
  /**
   * \brief The zero-copy version of \ref Pull
   *
   * This function is similar to \ref Pull except that \a keys
   * will not be copied to avoid the possible overhead of `memcpy`. It is
   * achieved via 'std::shared_ptr'. The system will store a copy of the shared
   * points until the pull has been finished. Before that, it is the caller's
   * responsibility to keep the content to be not changed.
   *
   * Sample usage:
   * \code
   * using namespace std;
   * auto keys = new vector<Key>{1, 3};
   * auto vals = new vector<float>();
   * KVWorker<float> w;
   * w.ZPush(shared_ptr<vector<Key>>(keys), vals);
   * // the system will delete keys
   * \endcode
   *
   * @param keys a list of keys, which must be sorted
   * @param vals the buffer for the pulled values. It can be empty.
   * @param opts pull options
   * @return the timestamp of this request
   */
  int ZPull(const std::shared_ptr<std::vector<Key> >& keys,
            std::vector<Val>* vals,
            const SyncOpts& opts = SyncOpts()) {
    return cache_->Pull(opts.GetTask(), SArray<Key>(keys), opts.callback,
                        CHECK_NOTNULL(vals), NULL);
  }


  /// \brief The zero-copy version of \ref VPush
  int ZVPush(const std::shared_ptr<std::vector<Key> >& keys,
             const std::shared_ptr<std::vector<Val> >& vals,
             const std::shared_ptr<std::vector<int> >& vals_size,
             const SyncOpts& opts = SyncOpts()) {
    return cache_->Push(opts.GetTask(), SArray<Key>(keys), SArray<Val>(vals),
                        SArray<int>(vals_size), opts.callback);
  }

  ///  \brief The zero-copy version of \ref VPull
  int ZVPull(const std::shared_ptr<std::vector<Key> >& keys,
             std::vector<Val>* vals,
             std::vector<int>* vals_size,
             const SyncOpts& opts = SyncOpts()) {
    return cache_->Pull(opts.GetTask(), SArray<Key>(keys), opts.callback,
                        CHECK_NOTNULL(vals), CHECK_NOTNULL(vals_size));
  }

 private:
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
