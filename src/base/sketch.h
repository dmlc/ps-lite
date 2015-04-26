#pragma once
#include "base/common.h"
// #include <nmmintrin.h>
namespace ps {

// the basc class for bloom filters, countmin, etc...
class Sketch {
 public:
 protected:

  // ver 1 is faster than ver 2, but is comparable to the murmurhash version
  // need add -msse4.2 in CFLAGS
  // uint64 crc32(uint64 key) const {
  //   return _mm_crc32_u64(0, key);
  // }
  // uint32 crc32(uint64 key) const {
  //   return _mm_crc32_u32((uint32)(key<<32), (uint32)key);
  // }

  uint32 hash(const uint64& key) const {
    // similar to murmurhash
    const uint32 seed = 0xbc9f1d34;
    const uint32 m = 0xc6a4a793;
    const uint32 n = 8;  // sizeof uint64
    uint32 h = seed ^ (n * m);

    uint32 w = (uint32) key;
    h += w; h *= m; h ^= (h >> 16);

    w = (uint32) (key >> 32);
    h += w; h *= m; h ^= (h >> 16);
    return h;
  }
};
} // namespace ps
