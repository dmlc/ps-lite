#pragma once

#include "base/common.h"

namespace ps {
class Bitmap;
typedef std::shared_ptr<Bitmap> BitmapPtr;

#define BITCOUNT_(x) (((BX_(x)+(BX_(x)>>4)) & 0x0F0F0F0F) % 255)
#define BX_(x) ((x) - (((x)>>1)&0x77777777)     \
                - (((x)>>2)&0x33333333)         \
                - (((x)>>3)&0x11111111))
class Bitmap {
 public:
  Bitmap() { }
  Bitmap(uint32 size, bool value = false) { resize(size, value); }
  ~Bitmap() { clear(); }

  void resize(uint32 size, bool value = false) {
    CHECK_EQ(size_, 0)
        << "TODO didn't support resize non-empty bitmap... clear() first ";
    size_ = size;
    map_size_ = (size >> kBitmapShift) + 1;
    map_ = new uint16[map_size_];
    fill(value);
  }

  void clear() {
    delete [] map_;
    map_ = nullptr;
    map_size_ = 0;
    size_ = 0;
  }

  void set(uint32 i) {
    map_[i>>kBitmapShift] |= (uint16) (1 << (i&kBitmapMask));
  }
  void clear(uint32 i) {
    map_[i>>kBitmapShift] &= ~((uint16) (1 << (i&kBitmapMask)));
  }

  bool test(uint32 i) const {
    return static_cast<bool>((map_[i>>kBitmapShift] >> (i&kBitmapMask)) & 1);
  }
  bool operator[] (uint32 i) const {
    return test(i);
  }

  void fill(bool value) {
    if (value)
      memset(map_, 0xFF, map_size_*sizeof(uint16));
    else
      memset(map_, 0, map_size_*sizeof(uint16));
  }

  // TODO flip all bits
  void flip() { }

  uint32 size() const { return size_; }
  size_t memSize() const { return map_size_*sizeof(uint16); }

  // number of bit == true
  uint32 nnz() {
    if (!init_nnz_) {
      for(int i=0; i<65536; i++)
        LUT_[i] = (unsigned char)BITCOUNT_(i);
      init_nnz_ = true;
    }

    uint32 bn = size_ >> kBitmapShift;
    uint32 v = 0;
    for (uint32_t i = 0; i < bn; i++)
      v += LUT_[map_[i]];
    return v + nnz(bn << kBitmapShift, size_);
  }

 private:
  uint32 nnz(uint32 start, uint32 end) {
    CHECK_LE(end, size_);
    uint32 v = 0;
    for (uint32 i = start; i < end; ++i)
      v += (*this)[i];
    return v;
  }

 private:
  uint16* map_ = nullptr;
  uint32 map_size_ = 0;
  uint32 size_ = 0;

  static const uint32 kBitmapShift = 4;
  static const uint32 kBitmapMask = 0x0F;

  unsigned char LUT_[65536];
  bool init_nnz_ = false;

};

} // namespace ps
