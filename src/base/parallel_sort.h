/**
 * @file   parallel_sort.h
 * @date   Tue Mar 31 17:01:58 2015
 *
 * @brief  Parallel sort
 */
#pragma once
#include "ps/shared_array.h"
namespace ps {

namespace  {
/// @brief the thread function
template<typename T, class Fn>
void ParallelSort(T* data, size_t len, size_t grainsize, const Fn& cmp) {
  if (len <= grainsize) {
    std::sort(data, data + len, cmp);
  } else {
    std::thread thr(ParallelSort<T, Fn>, data, len/2, grainsize, cmp);
    ParallelSort(data + len/2, len - len/2, grainsize, cmp);
    thr.join();

    std::inplace_merge(data, data + len/2, data + len, cmp);
  }
}
}  // namespace


/**
 * @brief Parallel Sort
 *
 * @param arr array
 * @param num_threads
 * @param cmp the comparision function, such as [](const T& a, const T& b) {
 * return a < b; } or an even simplier version: std::less<T>()
 */
template<typename T, class Fn>
void ParallelSort(SArray<T>* arr, int num_threads, const Fn& cmp) {
  CHECK_GT(num_threads, 0);
  size_t grainsize = std::max(arr->size() / num_threads + 5, (size_t)1024*16);
  ParallelSort(arr->data(), arr->size(), grainsize, cmp);
}

} // namespace ps
