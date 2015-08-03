#pragma once
#include "filter/filter.h"
#include <lz4.h>

#if __LZ4_VERSION_MINOR__ < 7
#define LZ4_compress_default LZ4_compress_limitedOutput
#endif

namespace ps {

/// \brief Compress value using LZ4
class CompressingFilter : public IFilter {
 public:
  void Encode(Message* msg) {
    auto conf = Find(Filter::COMPRESSING, msg);
    if (!conf) return;
    conf->clear_uncompressed_size();
    // if (msg->has_key()) {
    //   conf->add_uncompressed_size(msg->key.size());
    //   msg->key = Compress(msg->key);
    // }
    for (auto& v : msg->value) {
      conf->add_uncompressed_size(v.size());
      v = Compress(v);
    }
  }
  void Decode(Message* msg) {
    auto conf = Find(Filter::COMPRESSING, msg);
    if (!conf) return;
    int has_key = 0; //msg->has_key();
    CHECK_EQ((size_t)conf->uncompressed_size_size(), msg->value.size() + has_key);

    // if (has_key) {
    //   msg->key = Decompress(msg->key, conf->uncompressed_size(0));
    // }
    for (size_t i = 0; i < msg->value.size(); ++i) {
      msg->value[i] = Decompress(msg->value[i], conf->uncompressed_size(i+has_key));
    }
  }
 private:
  // based on lz4
  SArray<char> Compress(const SArray<char>& src) {
    int dst_size = LZ4_compressBound(src.size());
    SArray<char> dst(dst_size);
    int actual_size = LZ4_compress_default(src.data(), dst.data(), src.size(),
      dst_size);
    CHECK_GT(actual_size, 0);
    dst.resize(actual_size);
    return dst;
  }

  SArray<char> Decompress(const SArray<char>& src, size_t orig_size) {
    SArray<char> dst(orig_size);
    CHECK_EQ((size_t)LZ4_decompress_safe(src.data(), dst.data(), src.size(), orig_size), orig_size);
    return dst;
  }
};

} // namespace ps
