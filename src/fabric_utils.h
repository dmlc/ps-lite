// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =============================================================================

#ifndef PS_FABRIC_UTILS_H_
#define PS_FABRIC_UTILS_H_

#ifdef DMLC_USE_FABRIC

namespace ps {

#define check_err(ret, msg)                           \
  do {                                                \
    if (ret != 0) {                                   \
      LOG(FATAL) << msg << ". Return Code: " << ret   \
                 << ". ERROR: " << fi_strerror(-ret); \
    }                                                 \
  } while (false)

static const uint64_t kRendezvousStartMask = 0x8000000000000000ULL;
static const uint64_t kRendezvousReplyMask = 0x4000000000000000ULL;
static const uint64_t kDataMask = 0x3FFFFFFFFFFFFFFFULL;

// 1010101010101010101010101010101010101010101010101010101010101010
static const uint64_t kEFAMemTagFormat = 12297829382473034410ULL;

struct FabricEndpoint;

enum FabricWRContextType {
  // send rendezvous start
  kSendRendStart,
  // send rendezvous reply
  kSendRendReply,
  // recv rendezvous msgs
  kReceiveRend,
  // send with data msg
  kSendWithData,
  // recv with data msg
  kReceiveWithData
};

struct FabricBufferContext {
  // the buffer for meta
  char *meta_buffer;
  // the buffer for data
  char *data_buffer;
  // size of msg.meta
  size_t meta_len;
  // msg.data.size()
  size_t data_num;
  // msg.data[i].size()
  size_t data_len[kMaxDataFields];
  // the remote endpoint
  FabricEndpoint *endpoint;
};

struct FabricWRContext {
  // type of the WR context
  FabricWRContextType type;
  // libfabric tag
  uint64_t tag;
  // send/recv buffers:
  // buffers[0] for meta, buffers[1] for data
  struct iovec buffers[2];
  // number of send/recv buffers
  size_t num_buffers;
  // private msg_buf point
  void *private_data;
};

struct FabricMessageBuffer {
  // inline meta data size
  size_t inline_len;
  // inline meta data
  char *inline_buf;
  // WR context
  FabricWRContext *reserved_context;
  // msg.data
  std::vector<SArray<char>> data;
};

struct RendezvousMsg {
  // length of the meta data
  uint64_t meta_len;
  // msg.data.size()
  uint64_t data_num;
  // msg.data[i].size()
  uint64_t data_len[kMaxDataFields];
  // the original address of the message buffer
  uint64_t origin_addr;
  // the tag for tsend / trecv. Used for Rendezvous reply
  uint64_t tag;
  // whether it is for pull response
  bool pull_response;
  // the original key of the pull request
  uint64_t key;
};

std::string RendezvousDebugStr(const RendezvousMsg &msg) {
  std::stringstream ss;
  ss << "meta_len = " << msg.meta_len << ", data_num = " << msg.data_num
     << ", data_len = " << msg.data_len[0]
     << ", origin_addr = " << msg.origin_addr << ", tag = " << msg.tag
     << ", key = " << msg.key;
  return ss.str();
}

std::string WRContextDebugStr(const FabricWRContext &ctx) {
  std::stringstream ss;
  ss << "type = " << ctx.type << ", tag = " << ctx.tag
     << ", num_buffers = " << ctx.num_buffers;
  for (size_t i = 0; i < ctx.num_buffers; i++) {
    ss << ", buffers[" << i << "].iov_base = " << ctx.buffers[i].iov_base
       << ", buffers[" << i << "].iov_len = " << ctx.buffers[i].iov_len;
  }
  return ss.str();
}

struct FabricAddr {
  // endpoint name
  char name[64] = {};
  // length of endpoint name
  size_t len = sizeof(name);

  std::string DebugStr() const {
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < len; i++) {
      ss << std::to_string(name[i]) << ",";
    }
    ss << "]";
    return ss.str();
  }

  void CopyFrom(const char *ep_name, const size_t ep_name_len) {
    len = ep_name_len;
    memcpy(name, ep_name, sizeof(name));
  }

  void CopyTo(char *ep_name, size_t *ep_name_len) {
    *(ep_name_len) = len;
    memcpy(ep_name, name, sizeof(name));
  }
};

class FabricMemoryAllocator {
 public:
  explicit FabricMemoryAllocator() {
    PS_VLOG(4) << "aligned to pagesize " << pagesize_;
  }

  ~FabricMemoryAllocator() {}

  char *Alloc(size_t size, size_t *actual_size) {
    if (size == 0) {
      return nullptr;
    }
    // align to page size (usually 4KB)
    *actual_size = align_ceil(size, pagesize_);

    char *p;
    aligned_malloc((void **)&p, *actual_size);
    CHECK(p);
    return p;
  }

  size_t pagesize_ = sysconf(_SC_PAGESIZE);
};

void PrepareWRContext(FabricWRContext *context, void *meta_buff,
                      size_t meta_size, void *data_buff, size_t data_size) {
  context->buffers[0].iov_base = meta_buff;
  context->buffers[0].iov_len = meta_size;
  if (data_size != 0) {
    context->num_buffers = 2;
    context->buffers[1].iov_base = data_buff;
    context->buffers[1].iov_len = data_size;
  } else {
    context->num_buffers = 1;
  }
}

};  // namespace ps

#endif  // DMLC_USE_FABRIC
#endif  // PS_FABRIC_UTILS_H_
