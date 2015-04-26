#include "filter/filter.h"
#include "filter/compressing.h"
#include "filter/key_caching.h"
#include "filter/fixing_float.h"
#include "filter/add_noise.h"

namespace ps {

IFilter* IFilter::create(const Filter& conf) {
  switch (conf.type()) {
    case Filter::KEY_CACHING:
      return new KeyCachingIFilter();
    case Filter::COMPRESSING:
      return new CompressingIFilter();
    case Filter::FIXING_FLOAT:
      return new FixingFloatIFilter();
    case Filter::NOISE:
      return new AddNoiseIFilter();
    default:
      CHECK(false) << "unknow filter type";
  }
  return nullptr;
}


Filter* IFilter::Find(Filter::Type type, Task* task) {
  for (int i = 0; i < task->filter_size(); ++i) {
    if (task->filter(i).type() == type) return task->mutable_filter(i);
  }
  return nullptr;
}

} // namespace ps
