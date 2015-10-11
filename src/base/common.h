// some utility functions
#pragma once
#include <stdio.h>
#include <stdlib.h>
#ifdef _MSC_VER
#include "../windows/unistd.h"
#else
#include <unistd.h>
#endif
#include <time.h>
#ifdef _MSC_VER
#include <sys/timeb.h>
#include <sys/types.h>
#else
#include <sys/time.h>
#include <libgen.h>
#endif


// concurrency
#include <future>
#include <thread>
#include <mutex>
// smart pointers
#include <memory>
// stream
#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <streambuf>
// containers
#include <vector>
#include <list>
#include <queue>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <algorithm>

#include <functional>


// google staff
#include "gflags/gflags.h"
#include "glog/logging.h"

// util
#include "base/resource_usage.h"
#include "ps/base.h"

// base
#include <google/protobuf/stubs/common.h>
#include "google/protobuf/text_format.h"

//const int MAX_NUM_LEN = 1000;

namespace ps {

// uint64 is the default key size. We can change it into uint32 to reduce the
// spaces for storing the keys. Howerver, if we want a larger key size, say
// uint128, we need to change proto/range.proto to string type, because uint64
// is the largest integer type supported by protobuf
// typedef uint64 Key;
// static const Key kMaxKey = kuint64max;

typedef std::string NodeID;

typedef std::lock_guard<std::mutex> Lock;
using std::string;

#define LL LOG(ERROR)
#define LI LOG(INFO)


} // namespace ps


// basename(__FILE__)
