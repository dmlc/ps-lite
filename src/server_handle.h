/**
 * @file   ps_server_handle.h
 * @brief  Handles for server nodes
 */
#ifndef DMCL_PS_SERVER_HANDLE_
#define DMCL_PS_SERVER_HANDLE_
#include "./base.h"
#include "./ps.h"
namespace dmlc {
namespace ps {

// TODO

// /**
//  * \brief The default handle, which sums the data pushed from worker nodes.
//  */
// template <typename V>
// class DefaultHandle : public IHandle<V> {
//  public:
//   DefautHandle() { }
//   ~DefaultHandle() { }

//   void HandlePush(const Slice<K> recv_keys, const Slice<V> recv_vals,
//                   V* my_vals, size_t my_size) {
//     CHECK_EQ(recv_vals.size(), my_size);
//     for (size_t i = 0; i < my_size; ++i) {
//       my_vals[i] += recv_vals[i];
//     }
//   }

//   void HandlePull(const Slice<K> recv_keys, const Slice<V> my_vals,
//                   V* send_vals, size_t send_size) {
//     CHECK_EQ(recv_size, recv_vals.size());
//     for (size_t i = 0; i < my_size; ++i) {
//       recv_vals[i] = my_vals[i];
//     }
//   }

//   void HandleInit(const Slice<K> keys, V* vals, size_t vals_size) {
//     for (size_t i = 0; i < my_size; ++i) {
//       my_vals[i] = 0;
//     }
//   }
// };

// #if DMLC_USE_EIGEN
// /**
//  * \brief A wrapper of the handle using Eigen3.
//  * \tparam V the value type
//  */
// template <typename V>
// class EigenHandle : public Handle<V> {
//  public:
//   EigenHandle() { }
//   virtual ~EigenHandle() { }

//   typedef Eigen::Map<Eigen::Array<V, Eigen::Dynamic, 1> > EigenArray;
//   typedef Eigen::Map<
//     const Eigen::Array<V, Eigen::Dynamic, 1> > EigenConstArray;

//   void HandlePush(const EigenConstArray& recv_keys,
//                   const EigenConstArray& recv_vals,
//                   EigenArray& my_vals) {
//     my_vals += recv_vals;
//   }

//   void HandlePull(const EigenConstArray& recv_keys,
//                   const EigenConstArray& my_vals,
//                   EigenArray& send_vals) {

//   }

//   virtual HandleInit(const EigenConstArray& keys,
//                      EigenArray& vals) = 0;


//   void HandlePush(const Slice<K> recv_keys, const Slice<V> recv_vals,
//                   V* my_vals, size_t my_size) {
//     HandlePush(recv_keys.ToEigenArray(), recv_vals.ToEigenArray(),
//                EigenArrary(my_vals, my_size));
//   }

//   void HandlePull(const Slice<K> recv_keys, const Slice<V> my_vals,
//                   V* recv_vals, size_t recv_size) {
//     HandlePull(recv_keys.ToEigenArray(), my_vals.ToEigenArray(),
//                EigenArrary(recv_vals, recv_size));
//   }

//   void HandleInit(const Slice<K> recv_keys, V* my_vals, size_t my_size) {
//     HandleInit(recv_keys.ToEigenArray(), EigenArray(my_vals, my_size));
//   }
// };
// #endif  // DMLC_USE_EIGEN

}  // namespace ps
}  // namespace dmlc

#endif  /* DMCL_PS_SERVER_HANDLE_ */
