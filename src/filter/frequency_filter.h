#pragma once
#include "base/countmin.h"
#include "base/shared_array_inl.h"
namespace ps {

/**
 * @brief Remove infrequent keys via the countmin sketch
 * @tparam K key type
 * @tparam V counter type
 */
template<typename K, typename V>
class FreqencyFilter {
 public:
  /**
   * @brief Add keys with their key count
   *
   * @param key the list of keys
   * @param count the according frequency count
   */
  void InsertKeys(const SArray<K>& key, const SArray<V>& count);

  /**
   * @brief IFilters infrequency keys
   *
   * @param key the list of keys
   * @param freq_thr the frequency threshold
   *
   * @return the keys whose frequency is greater than freq_thr
   */
  SArray<K> QueryKeys(const SArray<K>& key, int freq_thr);

  bool Empty() { return count_.empty(); }

  /**
   * @brief resize the countmin sketch
   *
   */
  void Resize(int n, int k) { count_.resize(n, k, 254); }

  void Clear() { count_.clear(); }

 private:
  CountMin<K, V> count_;
};

// countmin implementation
template<typename K, typename V>
SArray<K> FreqencyFilter<K,V>::QueryKeys(const SArray<K>& key, int freqency) {
  CHECK_LT(freqency, kuint8max) << "change to uint16 or uint32...";
  SArray<K> filtered_key;
  for (auto k : key) {
    if ((int)count_.query(k) > freqency) {
     filtered_key.push_back(k);
    }
  }
  return filtered_key;
}

template<typename K, typename V>
void FreqencyFilter<K,V>::InsertKeys(const SArray<K>& key, const SArray<V>& count) {
  CHECK_EQ(key.size(), count.size());
  for (size_t i = 0; i < key.size(); ++i) {
    count_.insert(key[i], count[i]);
  }
}

// DEPRECATED hash implementation
// std::unordered_map<K, V> map_;

// template<typename K>
// SArray<K> FreqencyIFilter<K>::QueryKeys(const SArray<K>& key, int freqency) {
//   SArray<K> filtered_key;
//   for (K k : key) {
//     if (map_[k] > freqency) filtered_key.push_back(k);
//   }
//   return filtered_key;
// }

// template<typename K>
// void FreqencyIFilter<K>::InsertKeys(const SArray<K>& key, const SArray<uint32>& count) {
//   CHECK_EQ(key.size(), count.size());
//   for (size_t i = 0; i < key.size(); ++i) {
//     map_[key[i]] += count[i];
//   }
// }

}
