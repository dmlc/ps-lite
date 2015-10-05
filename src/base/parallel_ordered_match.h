#pragma once
#include "ps/shared_array.h"
#include "base/assign_op.h"
namespace ps {

// the implementation, see comments bellow
template <typename K, typename V>
void ParallelOrderedMatch(
    const K* src_key, const K* src_key_end, const V* src_val,
    const K* dst_key, const K* dst_key_end, V* dst_val,
    int k, AsOp op, size_t grainsize, size_t* n) {
  size_t src_len = std::distance(src_key, src_key_end);
  size_t dst_len = std::distance(dst_key, dst_key_end);
  if (dst_len == 0 || src_len == 0) return;

  // drop the unmatched tail of src
  src_key = std::lower_bound(src_key, src_key_end, *dst_key);
  src_val += (src_key - (src_key_end - src_len)) * k;

  if (dst_len <= grainsize) {
    while (dst_key != dst_key_end && src_key != src_key_end) {
      if (*src_key < *dst_key) {
        ++ src_key; src_val += k;
      } else {
        if (!(*dst_key < *src_key)) {
          for (int i = 0; i < k; ++i) {
            AssignOp(dst_val[i], src_val[i], op);
          }
          ++ src_key; src_val += k;
          *n += k;
        }
        ++ dst_key; dst_val += k;
      }
    }
  } else {
    std::thread thr(
        ParallelOrderedMatch<K,V>, src_key, src_key_end, src_val,
        dst_key, dst_key + dst_len / 2, dst_val,
        k, op, grainsize, n);
    size_t m = 0;
    ParallelOrderedMatch<K,V>(
        src_key, src_key_end, src_val,
        dst_key + dst_len / 2, dst_key_end, dst_val + ( dst_len / 2 ) * k,
        k, op, grainsize, &m);
    thr.join();
    *n += m;
  }
}


// Merge "src_val" into "dst_val" according to the keys by:
//
//   if (src_key[i] == dst_key[j]) then dst_val[j] op= src_val[i]
//
// If dst_val is empty, then message will be allocated. Here we assume both
// src_key and dst_val are ordered.
template <typename K, typename V>
size_t ParallelOrderedMatch(
    const SArray<K>& src_key,  // source keys
    const SArray<V>& src_val,  // source values
    const SArray<K>& dst_key,  // destination keys
    SArray<V>* dst_val,        // destination values
    int k = 1,                 // the size of a value entry = k * sizeof(V)
    AsOp op = AsOp::ASSIGN, // assignment operator
    int num_threads = 2) {
  if (dst_key.empty()) return 0;
  // do check
  CHECK_GT(num_threads, 0);
  CHECK_EQ(src_key.size() * k, src_val.size());
  if (dst_val->empty()) {
    dst_val->resize(dst_key.size()*k);
    dst_val->SetZero();
  } else {
    CHECK_EQ(dst_val->size(), dst_key.size()*k);
  }
  SizeR range = dst_key.FindRange(src_key.range());
  size_t grainsize = std::max(range.size() * k / num_threads + 5, (size_t)1024*1024);
  size_t n = 0;
  ParallelOrderedMatch<K, V>(
      src_key.begin(), src_key.end(), src_val.begin(),
      dst_key.begin() + range.begin(), dst_key.begin() + range.end(),
      dst_val->begin() + range.begin()*k, k, op, grainsize, &n);
  return n;
}

// similar to above, but use std::vector as the container
template <typename K, typename V>
size_t ParallelOrderedMatch(
    const SArray<K>& src_key,  // source keys
    const SArray<V>& src_val,  // source values
    const SArray<K>& dst_key,  // destination keys
    std::vector<V>* dst_val,   // destination values
    int k = 1,                 // the size of a value entry = k * sizeof(V)
    AsOp op = AsOp::ASSIGN, // assignment operator
    int num_threads = 2) {
  if (CHECK_NOTNULL(dst_val)->empty()) {
    dst_val->resize(dst_key.size()*k);
  }
  SArray<V> val(dst_val->data(), dst_val->size(), EmptyDel<V>());
  return ParallelOrderedMatch(src_key, src_val, dst_key, &val, k, op, num_threads);
}


// join key-value pairs. use the assigement operator "op" to solve conflicts. it
// assumes both key1 and key2 are orderd.
template <typename K, typename V>
void ParallelUnion(
    const SArray<K>& key1,  // keys from source 1
    const SArray<V>& val1,  // values from source 1
    const SArray<K>& key2,  // keys from source 2
    const SArray<V>& val2,  // values from source 2
    SArray<K>* joined_key,  // = key1 U key2
    SArray<V>* joined_val,  // = val1 U val2
    int k = 1,              // the size of a value entry = k * sizeof(V)
    AsOp op = AsOp::PLUS,  // assignment operator
    int num_threads = 2) {

  // join keys
  *CHECK_NOTNULL(joined_key) = key1.setUnion(key2);
  CHECK_NOTNULL(joined_val)->resize(0);

  // merge val1
  auto n1 = ParallelOrderedMatch<K,V>(
      key1, val1, *joined_key, joined_val, num_threads);
  CHECK_EQ(n1, key1.size());

  // merge val2
  auto n2 = ParallelOrderedMatch<K,V>(
      key2, val2, *joined_key, joined_val, num_threads);
  CHECK_EQ(n2, key2.size());
}

} // namespace ps
