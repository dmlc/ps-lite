#include "filter/filter.h"
#include "filter/compressing.h"
#include "filter/key_caching.h"
#include "filter/fixing_float.h"
#include "filter/add_noise.h"
#include "filter/delta_key.h"
#include "filter/truncate_float.h"

namespace ps {

IFilter* IFilter::create(const Filter& conf) {
  switch (conf.type()) {
    case Filter::KEY_CACHING:
      return new KeyCachingFilter();
    case Filter::COMPRESSING:
      return new CompressingFilter();
    case Filter::FIXING_FLOAT:
      return new FixingFloatFilter();
    case Filter::NOISE:
      return new AddNoiseFilter();
    case Filter::DELTA_KEY:
      return new DeltaKeyFilter();
    case Filter::TRUNCATE_FLOAT:
      return new TruncateFloatFilter();
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
