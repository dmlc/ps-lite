#pragma once
#include "filter/filter.h"

namespace ps {

class CompressingIFilter : public IFilter {
 public:
  void Encode(Message* msg) {
    // auto conf = Find(Filter::COMPRESSING, msg);
    // if (!conf) return;
    // conf->clear_uncompressed_size();
    // if (msg->has_key()) {
    //   conf->add_uncompressed_size(msg->key.size());
    //   msg->key = msg->key.CompressTo();
    // }
    // for (auto& v : msg->value) {
    //   conf->add_uncompressed_size(v.size());
    //   v = v.CompressTo();
    // }
  }
  void Decode(Message* msg) {
    // auto conf = Find(Filter::COMPRESSING, msg);
    // if (!conf) return;
    // int has_key = msg->has_key();
    // CHECK_EQ(conf->uncompressed_size_size(), msg->value.size() + has_key);

    // if (has_key) {
    //   SArray<char> raw(conf->uncompressed_size(0));
    //   raw.UncompressFrom(msg->key);
    //   msg->key = raw;
    // }
    // for (int i = 0; i < msg->value.size(); ++i) {
    //   SArray<char> raw(conf->uncompressed_size(i+has_key));
    //   raw.UncompressFrom(msg->value[i]);
    //   msg->value[i] = raw;
    // }
  }
};

} // namespace ps
