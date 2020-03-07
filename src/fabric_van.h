/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_FABRIC_VAN_H_
#define PS_FABRIC_VAN_H_
#include <stdio.h>
#include <cstdlib>
#include <zmq.h>
#include <string>
#include <cstring>
#include <thread>
#include <cmath>
#include <atomic>
#include <tuple>
#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"
#include "zmq_van.h"
#if _MSC_VER
#define rand_r(x) rand()
#endif


#ifdef __GNUC__
#define DMLC_PS_OFI_LIKELY(x)  __builtin_expect((x), 1)
#define DMLC_PS_OFI_UNLIKELY(x)  __builtin_expect((x), 0)
#else
#define DMLC_PS_OFI_LIKELY(x)  (x)
#define DMLC_PS_OFI_UNLIKELY(x)  (x)
#endif

#define DMLC_PS_OFI_MAJOR_VERSION  (1)
#define DMLC_PS_OFI_MINOR_VERSION  (6)
#define dmlc_ps_ofi_version    FI_VERSION(DMLC_PS_OFI_MAJOR_VERSION, \
                                          DMLC_PS_OFI_MINOR_VERSION)
#define DMLC_PS_MAX_PROV_INFO    (15)

// We have a limit of MAX_HANDLE_SIZE = 64 bytes. Therefore, we can only
// support an endpoint name of maximum 56 bytes. We are using remaining
// 8 bytes for tags.
#define DMLC_PS_MAX_EP_ADDR (56)

// We are supporting minimum 2^32 rings per endpoint and reserving 1 bit
// for marking control sends/recvs.
#define MIN_TAG_BITS_FOR_RING_ID  (32 + 1)

// For each tag, we use MSB as control bit and remaining
// for identifying different rings. We look at mem_tag_format for
// an endpoint to determine if provider is reserving any MSBs.
#define OFI_HIGHEST_TAG_BIT    (0x1UL << 63)


#define check_err(ret, msg) do {                          \
        if (DMLC_PS_OFI_UNLIKELY(ret != 0)) {             \
          LOG(FATAL) << msg << ". RC: " << ret            \
                     << ". ERROR: " << fi_strerror(-ret); \
        }                                                 \
} while (false)

/* This is twice the size of maximum inflight requests supported by NCCL */
#define DMLC_PS_OFI_MAX_REQUESTS  256


#include <rdma/fi_errno.h>
#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>

namespace ps {

#define NCCL_OFI_MAX_REQUESTS  256

typedef struct stack {
  int *array;
  int top;
  int size;
} stack_t;

/**
 * \brief be smart on freeing recved data
 */
inline void FreeData2(void* data, void* hint) {
  if (hint == NULL) {
    delete[] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}

/*
 *  * @brief  Allocate stack of free indexes
 *   */
static stack_t *allocate_stack(size_t num_elems)
{
  stack_t *stack = NULL;

  stack = (stack_t *)malloc(sizeof(stack_t));
  if (stack == NULL) {
    LOG(FATAL) << "Unable to allocate stack structure";
    goto exit;
  }

  stack->size = num_elems;
  stack->top = -1;
  stack->array = (int *)malloc(stack->size * sizeof(int));
  if (stack->array == NULL) {
    LOG(FATAL) << "Could not allocate stack array";
    goto error;
  }

  goto exit;

error:
  if (stack)
    free(stack);
  stack = NULL;
exit:
  return stack;
}

/*
 *  * @brief  Free given stack
 *   */

void free_stack(stack_t *stack)
{
  if (stack)
  {
    if(stack->array)
      free(stack->array);
    free(stack);
  }
}

/*
 *  * @brief  Push element to stack
 *   *
 *    * @return  0 on success
 *     *    error on others
 *      */
static inline int stack_push(stack_t *stack, int elem)
{
  //ncclResult_t ret = ncclSuccess;
  int ret = 0;

  if (DMLC_PS_OFI_UNLIKELY(stack == NULL)) {
    //ret = ncclSystemError;
    LOG(FATAL)<<"Invalid stack provided.";
    goto exit;
  }

  /* Check if the element is a valid index */
  if (DMLC_PS_OFI_UNLIKELY(elem >= stack->size)) {
    //ret = ncclSystemError;
    LOG(FATAL) << "Invalid element provided. "; // element: %d, Stack Size: %d",
    //         elem, stack->size);
    goto exit;
  }

  /* Check if stack is full */
  if (DMLC_PS_OFI_UNLIKELY(stack->top == (stack->size - 1))) {
    //ret = ncclSystemError;
    LOG(FATAL) << "Stack is full. Cannot insert element into the stack. "; //Stack Size: %d, Current stack index: %d",
    //         stack->top, stack->size);
    goto exit;
  }

  stack->array[++stack->top] = elem;

exit:
  return ret;
}

/*
 *  * @brief  Pop element out of stack
 *   *
 *    * @return  stack element, on success
 *     *    -1 on error
 *      */

static inline int stack_pop(stack_t *stack)
{
  uint64_t free_index = stack->size;

  if (DMLC_PS_OFI_UNLIKELY(stack == NULL)) {
    LOG(FATAL) << "Invalid stack provided.";
    goto exit;
  }

  if (DMLC_PS_OFI_UNLIKELY(stack->top == -1)) {
    LOG(FATAL) << "Stack is empty. Cannot pop element.";
    goto exit;
  }

  free_index = stack->array[stack->top--];

exit:
  return free_index;
}

typedef struct free_list {
  /* Array of free buffers */
  void *buffers;

  /* Stack of free buffer indexes */
  stack_t *free_index;

  /* Size of buffers array */
  uint64_t size;
} free_list_t;

static free_list_t *allocate_free_list(void)
{
  free_list_t *fl = NULL;

  fl = (free_list_t *)malloc(sizeof(free_list_t));
  if (fl == NULL) {
    LOG(FATAL) << "Unable to allocate free list structure";
    goto exit;
  }

  fl->buffers = NULL;
  fl->free_index = NULL;

exit:
  return fl;
}

static int allocate_ofi_fl(free_list_t **nccl_ofi_req_fl, size_t fl_size,
            size_t buffer_size)
{
  int ret = 0, idx;
  free_list_t *fl = NULL;

  /* Validate free list size and buffer size */
  if (fl_size < 1 || buffer_size < 1) {
    //ret = ncclSystemError;
    LOG(FATAL) << "Invalid free list size and/or buffer size. ";//Provided fl_size: %zu and buffer size: %zu",
    //         fl_size, buffer_size);
    goto error;
  }

  /* Allocate free list structure */
  fl = allocate_free_list();
  if (fl == NULL) {
    //ret = ncclSystemError;
    LOG(FATAL) << "ERROR";
    goto error;
  }
  fl->size = fl_size;

  /* Allocate buffers */
  fl->buffers = calloc(fl->size, buffer_size);
  if (fl->buffers == NULL) {
    //LOG(FATAL)"Unable to allocate NCCL OFI free list buffers");
    //ret = ncclSystemError;
    LOG(FATAL) << "ERROR";
    goto error;
  }

  /* Allocate stack of free indexes */
  fl->free_index = allocate_stack(fl->size);
  if (fl->free_index == NULL) {
    //LOG(FATAL)"Couldn't allocate free index stack");
    //ret = ncclSystemError;
    goto error;
  }

  /* Initialise stack */
  for (idx = 0; idx < fl->free_index->size; idx++) {
    ret = stack_push(fl->free_index, idx);
    if (ret != 0)
      goto error;
  }

  *nccl_ofi_req_fl = fl;

  goto exit;

error:
  LOG(FATAL) << "ERROR";
  if (fl->buffers)
    free(fl->buffers);
  if (fl->free_index)
    free_stack(fl->free_index);
  if (fl)
    free(fl);
exit:
  return ret;
}

struct Stack {
  int *array;
  int top;
  int size;
};

struct FreeList {
  // Array of free buffers
  void *buffers;

  // Stack of free buffer indexes
  Stack *free_index;

  // Size of buffers array
  uint64_t size;
};

struct ListenComm {
  uint64_t tag;
  struct fid_ep *local_ep;
  //int dev;
  bool accepted;
};

struct SendComm {
  uint64_t tag;
  uint64_t num_inflight_reqs;
  fi_addr_t remote_ep;
  struct fid_ep *local_ep;
  free_list_t* reqs_fl;
  free_list_t* pending_reqs_fl;
};

struct RecvComm {
  uint64_t tag;
  uint64_t num_inflight_reqs;
  fi_addr_t remote_ep;
  struct fid_ep *local_ep;
  free_list_t *reqs_fl;
};

enum RequestState {
  kRequestCreated = 0,
  kRequestPending,
  kRequestCompleted,
  kRequestError,
};

enum RequestDirection {
  kRequestSend = 1,
  kRequestRecv,
  kRequestUndefined,
};

struct Request {
  // Associated Comm object
  union {
    ListenComm *l_comm;
    SendComm *s_comm;
    RecvComm *r_comm;
  };

  // Buffer index
  uint64_t buffer_index;

  // Associated OFI Context
  struct fi_context ctx;

  // Associated Device ID
  //int dev;

  // Size of completed request
  size_t size;

  // State of request
  RequestState state;

  // Direction of request
  RequestDirection direction;
};

/*
 *  * @brief  Zero out NCCL OFI request
 *   */
static inline void zero_ofi_req(Request *req)
{
  req->l_comm = NULL;
  req->s_comm = NULL;
  req->r_comm = NULL;

  req->buffer_index = 0ULL;
  memset(&req->ctx, 0, sizeof(struct fi_context));

  //req->dev = -1;
  req->size = 0;

  req->state = kRequestCreated;//NCCL_OFI_REQ_CREATED;
  req->direction = kRequestUndefined;
}



struct PendingReq {
  // Associated request
  Request* ofi_req;

  // Send/Recv Metadata
  void *data;
  size_t len;
  int type;
};

struct PendingReqElem {
  PendingReqElem* next;

  // Buffer index
  uint64_t buffer_index;

  // Pending request to retry
  PendingReq pending_req;
};


struct PendingReqQueue {
  PendingReqElem *head;
  PendingReqElem *tail;
};


// TDDO call it an endpoint?
struct Endpoint {
  // Current available tag ID.
  uint64_t tag;

  // Maximum supported tag ID
  uint64_t max_tag;

  // Count of CQEs to read from CQ
  uint64_t num_cqes;

  // Provider name
  char *prov_name;

  // Fabric handle
  struct fid_fabric *fabric;

  // Access Domain handle
  struct fid_domain *domain;

  // Endpoint handle to communicate to
  struct fid_ep *ep;

  // Address vector handle
  struct fid_av *av;

  // Completion Queue handle
  struct fid_cq *cq;

  // Pending requests queue
  PendingReqQueue *pending_req_q;
};

static inline Request *allocate_nccl_ofi_request(free_list_t *fl)
{
  Request *req = NULL;
  uint64_t next_avail_index;

  if (DMLC_PS_OFI_UNLIKELY(fl == NULL || fl->free_index == NULL)) {
    LOG(FATAL) << "Free list is empty or Free Index stack does not exist.";
    goto exit;
  }

  /* Get free index */
  next_avail_index = stack_pop(fl->free_index);
  if (DMLC_PS_OFI_UNLIKELY(next_avail_index >= fl->free_index->size)) {
    LOG(FATAL) << "No pre-allocated buffer is available for use";
    //NCCL_OFI_WARN("No pre-allocated buffer is available for use. next_avail_index: %lu and free_index Size: %d",
    //         next_avail_index, fl->free_index->size);
    goto exit;
  }

  /* Get buffer */
  if (DMLC_PS_OFI_UNLIKELY(fl->buffers == NULL)) {
    LOG(FATAL) <<("No pre-allocated buffers are present.");
    goto exit;
  }

  req = &((Request *)fl->buffers)[next_avail_index];
  req->buffer_index = next_avail_index;

exit:
  return req;
}
/*
 *  * @brief  Prepares NCCL OFI request for reuse
 *   */
static inline int free_nccl_ofi_req(Request *req, bool dec_inflight_cmds)
{
  int ret = 0;//ncclSuccess;
  SendComm *sComm = NULL;
  RecvComm *rComm = NULL;
  uint64_t buffer_index;

  if (DMLC_PS_OFI_UNLIKELY(req == NULL)) {
    //ret = ncclSystemError;
    LOG(FATAL) << ("Provided null request for cleanup");
    goto exit;
  }

  if (req->direction == kRequestSend) {
    sComm = req->s_comm;
    if (DMLC_PS_OFI_UNLIKELY(sComm == NULL)) {
      //ret = ncclSystemError;
      LOG(FATAL) << "Invalid sComm provided for request of device";// %d\n",
              //sComm->dev);
      goto exit;
    }

    /* Update free list */
    if (DMLC_PS_OFI_UNLIKELY(sComm->reqs_fl == NULL)) {
      //ret = ncclSystemError;
      LOG(FATAL) << "sComm for device  does not have valid free list";
              //sComm->dev);
      goto exit;
    }

    buffer_index = req->buffer_index;

    /* Zero out buffer */
    zero_ofi_req(req);

    ret = stack_push(sComm->reqs_fl->free_index,
         buffer_index);
    if (DMLC_PS_OFI_UNLIKELY(ret != 0))
      goto exit;

    /* Reduce inflight commands */
    if (DMLC_PS_OFI_LIKELY(dec_inflight_cmds == true))
      sComm->num_inflight_reqs--;

  }
  else if (req->direction == kRequestRecv) {
    rComm = req->r_comm;
    if (DMLC_PS_OFI_UNLIKELY(rComm == NULL)) {
      //ret = ncclSystemError;
      LOG(FATAL) <<("Invalid rComm provided for request of device");
              //rComm->dev);
      goto exit;
    }

    /* Update free list */
    if (DMLC_PS_OFI_UNLIKELY(rComm->reqs_fl == NULL)) {
      //ret = ncclSystemError;
      LOG(FATAL) << ("rComm for device does not have valid free list");
              //rComm->dev);
      goto exit;
    }

    buffer_index = req->buffer_index;

    /* Zero out buffer */
    zero_ofi_req(req);

    ret = stack_push(rComm->reqs_fl->free_index,
         buffer_index);
    if (DMLC_PS_OFI_UNLIKELY(ret != 0))
      goto exit;

    /* Reduce inflight commands */
    if (DMLC_PS_OFI_LIKELY(dec_inflight_cmds == true))
      rComm->num_inflight_reqs--;
  }
  else {
    //ret = ncclSystemError;
    LOG(FATAL) << ("Unexpected transaction direction. Transaction direction:") <<
             req->direction;
  }

exit:
  return ret;
}




/**
 * \brief ZMQ based implementation
 */
class FabricVan : public Van {


 public:
  FabricVan() {}
  virtual ~FabricVan() {}

 protected:
  void Start(int customer_id, bool standalone) override {
    // start zmq
    start_mu_.lock();
    OfiInit();
    start_mu_.unlock();
    customer_id_ = customer_id;
    ZmqStart();
    Van::Start(customer_id, false);
  }

  void ZmqStart() {
    start_mu_.lock();
    if (context_ == nullptr) {
      context_ = zmq_ctx_new();
      CHECK(context_ != NULL) << "create 0mq context failed";
    }
    start_mu_.unlock();

    auto val1 = Environment::Get()->find("BYTEPS_ZMQ_MAX_SOCKET");
    int byteps_zmq_max_socket = val1 ? atoi(val1) : 1024;
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, byteps_zmq_max_socket);
    PS_VLOG(1) << "BYTEPS_ZMQ_MAX_SOCKET set to " << byteps_zmq_max_socket;

    auto val2 = Environment::Get()->find("BYTEPS_ZMQ_NTHREADS");
    int byteps_zmq_nthreads = val2 ? atoi(val2) : 4;
    zmq_ctx_set(context_, ZMQ_IO_THREADS, byteps_zmq_nthreads);
    PS_VLOG(1) << "BYTEPS_ZMQ_NTHREADS set to " << byteps_zmq_nthreads;
  }

  void OfiInit() {
    // Get a list of fi_info structures for a single provider
    struct fi_info *hints = fi_allocinfo();
    CHECK(hints != nullptr) << "Unable to allocate fi_info";
    
    // Hints to filter providers
    hints->caps = FI_TAGGED | FI_MSG;
    hints->mode = FI_CONTEXT;

    hints->ep_attr->type = FI_EP_RDM;

    hints->domain_attr->av_type = FI_AV_TABLE;
    hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
    hints->domain_attr->data_progress = FI_PROGRESS_AUTO;

    // Indicate that the application support local memory registration
    hints->domain_attr->mr_mode = FI_MR_LOCAL;

    // TODO figure out msg_order
    hints->tx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->msg_order = FI_ORDER_SAS;

    std::lock_guard<std::mutex> lock(mu_);
    int ret = fi_getinfo(dmlc_ps_ofi_version, nullptr, 0, 0, hints, &ofi_provider_);
    if (ret == -FI_ENODATA) {
      LOG(FATAL) << "Could not find any optimal provider.";
    } else {
      check_err(ret, "Could not complete fi_getinfo");
    }
    CHECK(ofi_provider_ != nullptr) << "Failed to get ofi provider";
    // If we detect the Amazon EFA provider, emulate a NIC per GPU
    // so that NCCL will build more rings and achieve better peak BW
    if (strcmp(ofi_provider_->fabric_attr->prov_name, "efa") == 0) {
      ofi_provider_->next = ofi_provider_;
    }
    fi_freeinfo(hints);
    LOG(INFO) << "Selected fabric provider is "
              << ofi_provider_->fabric_attr->prov_name;

    // Check if provider requires local memory registration
    if (ofi_provider_->domain_attr->mr_mode & FI_MR_LOCAL) {
      LOG(FATAL) << "Provider " << ofi_provider_->fabric_attr->prov_name
                 << " required registration of local memory buffers, which"
                 << " is not implemented";
    }
  }


  // Allocates and initialises various libfabric resources like
  // fabric, domain, endpoint, CQ and AV.
  // Returns initialised nccl_ofi_comp structure
  // Should be protected with mu_
  void OfiCreateComponent() {
    // TODO: use smart pointer
    ofi_component_ = (Endpoint*) calloc(1, sizeof(Endpoint));
    CHECK(ofi_component_ != nullptr) << "Failed to allocate Endpoint";

    // Initialize tag and num_cqes
    ofi_component_->tag = 1;
    ofi_component_->num_cqes = DMLC_PS_OFI_MAX_REQUESTS;
    ofi_component_->prov_name = ofi_provider_->fabric_attr->prov_name;

    // Determine if any tag bits are used by provider
    int ofi_tag_leading_zeroes = 0, ofi_tag_bits_for_ring_id = 64;
    while (!((ofi_provider_->ep_attr->mem_tag_format << ofi_tag_leading_zeroes++) &
      (uint64_t) OFI_HIGHEST_TAG_BIT) &&
      (ofi_tag_bits_for_ring_id >= MIN_TAG_BITS_FOR_RING_ID)) {
      ofi_tag_bits_for_ring_id--;
    }

    CHECK_GT(ofi_tag_bits_for_ring_id, MIN_TAG_BITS_FOR_RING_ID)
      << "Provider " << ofi_provider_->fabric_attr->prov_name
      << " does not provide enough tag bits " << ofi_tag_bits_for_ring_id
      << " for ring ID. Minimum required is " << MIN_TAG_BITS_FOR_RING_ID;

    // Set maximum tag information; Reserving 1 bit for control information
    ofi_component_->max_tag = (uint64_t)((1ULL << (ofi_tag_bits_for_ring_id - 1)) - 1);

    // Create fabric
    int ret = fi_fabric(ofi_provider_->fabric_attr, &(ofi_component_->fabric), nullptr);
    check_err(ret, "Couldn't open a fabric provider");

    // Create domain
    ret = fi_domain(ofi_component_->fabric, ofi_provider_,
        &(ofi_component_->domain), nullptr);
    check_err(ret, "Couldn't open a fabric access domain");

    /* Create transport level communication endpoint(s) */
    ret = fi_endpoint(ofi_component_->domain, ofi_provider_, &(ofi_component_->ep), nullptr);
    check_err(ret, "Couldn't allocate endpoint");

    struct fi_cq_attr cq_attr = {};
    cq_attr.format = FI_CQ_FORMAT_TAGGED;
    ret = fi_cq_open(ofi_component_->domain, &cq_attr, &ofi_component_->cq, nullptr);
    check_err(ret, "Couldn't open CQ");

    struct fi_av_attr av_attr = {};
    av_attr.type = FI_AV_TABLE;
    ret = fi_av_open(ofi_component_->domain, &av_attr, &ofi_component_->av, nullptr);
    check_err(ret, "Couldn't open AV");

    // Bind CQ and AV to endpoint
    ret = fi_ep_bind(ofi_component_->ep, (fid_t)ofi_component_->cq, FI_SEND | FI_RECV);
    check_err(ret, "Couldn't bind EP-CQ");
    ret = fi_ep_bind(ofi_component_->ep, (fid_t)ofi_component_->av, 0);
    check_err(ret, "Couldn't bind EP-CQ");

    // Enable endpoint for communication
    ret = fi_enable(ofi_component_->ep);
    check_err(ret, "Couldn't enable endpoint");

    // Get endpoint name
    ret = fi_getname(&(ofi_component_->ep->fid), (void *)&ep_name_, &ep_name_len_);
    check_err(ret, "Call to fi_getname() failed");
  }


  void OfiListen() {
    // Create libfabric components for the given NIC,
    // if not already created, else increase tag ID.
    uint64_t tag = 0;
    std::lock_guard<std::mutex> lock(mu_);
    if (DMLC_PS_OFI_UNLIKELY(ofi_component_ == nullptr)) {
      OfiCreateComponent();
    } else {
      ofi_component_->tag++;
      if (ofi_component_->tag == ofi_component_->max_tag) {
        LOG(FATAL) << "Cannot open more connections. Maximum is "
                   << ofi_component_->max_tag;
      }
    }
    tag = ofi_component_->tag;

    // Build ListenComm
    ListenComm *listen_comm = (ListenComm *) calloc(1, sizeof(ListenComm));
    listen_comm->tag = tag;
    listen_comm->local_ep = ofi_component_->ep;
    listen_comm->accepted = false;
    CHECK(listen_comms_.find(tag) == listen_comms_.end())
      << "Duplicate tag detected";
    listen_comms_[tag] = listen_comm;
  }

  void Connect(const Node& node) override {
    // use zmq to boostrap libfabric
    if ((node.role == my_node_.role) && (node.role == Node::SCHEDULER)) {
      LOG(INFO) << "Skip connecting to myself";
      return;
    }

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      LOG(INFO) << "Skip connecting to peer node " << node.hostname << ":" << node.port;
      return;
    }
    CHECK_NE(node.id, node.kEmpty);
    std::string endpoint;
    uint64_t my_tag;
    std::string host_ip = node.hostname + ":" + std::to_string(node.port);
    {
      std::lock_guard<std::mutex> lock(mu_);
      LOG(INFO) << "Connecting to " << host_ip;
      CHECK(endpoint_map_.find(host_ip) != endpoint_map_.end());
      endpoint = endpoint_map_[host_ip];
      my_tag = my_tag_;
    }
    OfiConnect(endpoint, my_tag, host_ip);
  }

  /*
   *  * @brief  Processes completion entries from CQ
   *   *
   *    * @return  0, on success
   *     *    error, on others
   *      */
  inline int process_completions(
          struct fi_cq_tagged_entry *cq_entry,
          uint64_t num_cqes, uint64_t control_bit_mask)
  {
    int ret = 0; //ncclSuccess;
    Request *req = NULL;
    uint64_t comp_idx = 0, comp_flags = 0;
  
    for (comp_idx = 0; comp_idx < num_cqes; comp_idx++) {
  
      comp_flags = cq_entry[comp_idx].flags;
  
      req = container_of(cq_entry[comp_idx].op_context,
             Request, ctx);
      if (DMLC_PS_OFI_UNLIKELY(req == NULL)) {
        LOG(FATAL) << ("Invalid request context provided");
        //ret = ncclSystemError;
        goto exit;
      }
  
      req->state = kRequestCompleted; //NCCL_OFI_REQ_COMPLETED;
      req->size = cq_entry[comp_idx].len;
  
      /* Determine if this is control message */
      if (DMLC_PS_OFI_UNLIKELY(cq_entry[comp_idx].tag & control_bit_mask)) {
        if (comp_flags & FI_RECV) {
          /* Mark listenComm to accepted state */
          req->l_comm->accepted = true;
        }
      }
    }
  
  exit:
    return ret;
  }


  /*
   *  * @brief  Process completion entries for the given NCCL OFI component.
   *   *    This also updates several request fileds like size, status, etc
   *    *
   *     * @return  0, on success
   *      *    error, on others
   *       */
  int ofi_process_cq() {
    ssize_t rc = 0;
    int ret = 0;
    struct fi_cq_err_entry err_buffer = { 0 };
    uint64_t cqe_burst = ofi_component_->num_cqes;
    struct fi_cq_tagged_entry cqe_tagged_buffers[cqe_burst];
    Request *req = NULL;
    struct fid_cq *cq = ofi_component_->cq;
    uint64_t control_bit_mask = ~(ofi_component_->max_tag);
  
    while (true) {
  
      /* Zero-out buffers */
      memset(&cqe_tagged_buffers, 0, sizeof(cqe_tagged_buffers));
  
      /* Receive completions for the given endpoint */
      rc = fi_cq_read(cq, &cqe_tagged_buffers[0], cqe_burst);
      if (rc > 0) {
        ret = process_completions(
            &cqe_tagged_buffers[0], rc,
            control_bit_mask);
        if (DMLC_PS_OFI_UNLIKELY(ret != 0))
          LOG(FATAL) << "ERROR";
          goto exit;
      }
      else if (DMLC_PS_OFI_UNLIKELY(rc == -FI_EAVAIL)) {
        rc = fi_cq_readerr(cq, &err_buffer, 0);
        if (DMLC_PS_OFI_UNLIKELY(rc < 0)) {
          LOG(FATAL) << "ERROR";
          //NCCL_OFI_WARN("Unable to read from fi_cq_readerr. RC: %zd. Error: %s",
          //       rc,
          //       fi_cq_strerror(cq,
          //    err_buffer.prov_errno,
          //    err_buffer.err_data, NULL, 0));
          //ret = ncclSystemError;
          goto exit;
        }
  
        /* TODO: Add debug log to dump failed request details */
        req = container_of(err_buffer.op_context,
                Request, ctx);
        req->state = kRequestError; // NCCL_OFI_REQ_ERROR;
        req->size = err_buffer.len;
      }
      else if (rc == -FI_EAGAIN) {
        /* No completions to process */
        break;
      }
      else {
        LOG(FATAL) << "ERROR";
        //NCCL_OFI_WARN("Unable to retrieve completion queue entries. RC: %zd, ERROR: %s",
        //       rc, fi_strerror(-ret));
        //ret = ncclSystemError;
        goto exit;
      }
    }
  
  exit:
    return ret;
  }


  void OfiConnect(const std::string remote_endpoint, uint64_t tag, const std::string host_ip) {
    ssize_t rc = 0;
    fi_addr_t remote_addr;
    Request *req = NULL;
    size_t req_size = sizeof(Request);

    // Create libfabric components for the given NIC, if not
    // already created.
    uint64_t max_tag;
    mu_.lock();
    if (DMLC_PS_OFI_UNLIKELY(ofi_component_ == nullptr)) {
      OfiCreateComponent();
    }
    max_tag = ofi_component_->max_tag;
    CHECK(tag >= 1 && tag <= max_tag) << "Received an invalid tag "
                                      << tag << " for device";
    mu_.unlock();

    // Insert remote address into AV
    int ret = fi_av_insert(ofi_component_->av, (void *) remote_endpoint.c_str(), 1,
                           &remote_addr, 0, nullptr);
    if (DMLC_PS_OFI_UNLIKELY(ret != 1)) {
      LOG(FATAL) << "Unable to insert remote address into address vector for device. "
                 << "RC: " << ret << " " << fi_strerror(-ret);
    }

    // Build SendComm
    SendComm *sComm = (SendComm *)calloc(1, sizeof(SendComm));
    CHECK(sComm != nullptr) << "Couldn't allocate SendComm";

    sComm->tag = tag;
    sComm->local_ep = ofi_component_->ep;
    sComm->remote_ep = remote_addr;

    // Pre-allocated buffers for data path
    ret = allocate_ofi_fl(&sComm->reqs_fl, NCCL_OFI_MAX_REQUESTS,
                          req_size);
    if (DMLC_PS_OFI_UNLIKELY(ret != 0)) {
      LOG(FATAL) << "Could not allocate NCCL OFI requests free list for dev";
    }

    req = allocate_nccl_ofi_request(sComm->reqs_fl);
    if (DMLC_PS_OFI_UNLIKELY(req == NULL)) {
        LOG(FATAL) << "Unable to get NCCL OFI request for device";
    }

    req->s_comm = sComm;
    req->direction = kRequestSend;

    /* Send "connect" message to remote EP */
    mu_.lock();
    void *ep_name = &ep_name_;
    uint64_t ep_name_len = ep_name_len_;
    mu_.unlock();
    do {
      rc = fi_tsend(sComm->local_ep, (void *)&ep_name,
              ep_name_len, NULL, sComm->remote_ep,
              sComm->tag | ~max_tag, &req->ctx);
      if (rc == 0) {
        break;
      } else if (rc == -FI_EAGAIN) {
        // Process completions so that you have enough
        // resources for sending connect message
        ret = ofi_process_cq();
        if (DMLC_PS_OFI_UNLIKELY(ret != 0))
          LOG(FATAL) << "ERROR";
      }
      else {
        check_err(rc, "Unable to send connect message");
      }
    } while (true);

    /* Ensure the message is sent. */
    do {
      ret = ofi_process_cq();
      if (DMLC_PS_OFI_UNLIKELY(ret != 0)) LOG(FATAL) << "ERROR";
    } while (req->state != kRequestCompleted);
  
    std::lock_guard<std::mutex> lock(mu_);
    send_comms_[host_ip] = sComm;
    LOG(INFO) << "Connected to node " << host_ip;
    if (req)
      free_nccl_ofi_req(req, false);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    // include/rdma/fabric.h:typedef uint64_t          fi_addr_t;
    Van::Stop();
    // join all threads
    should_stop_ = true;
    for (auto t : thread_list_) t->join();
    PS_VLOG(1) << my_node_.ShortDebugString() << " all threads joined and destroyed";
    // close sockets
    int linger = 0;
    int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
    CHECK(rc == 0 || errno == ETERM);
    CHECK_EQ(zmq_close(receiver_), 0);
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& it : senders_) {
      int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
      CHECK(rc == 0 || errno == ETERM);
      CHECK_EQ(zmq_close(it.second), 0);
    }
    senders_.clear();
    zmq_ctx_destroy(context_);
    context_ = nullptr;
  }

  struct pair_hash {
  public:
    template <typename T, typename U>
    std::size_t operator()(const std::pair<T, U> &x) const
    {
      return std::hash<T>()(x.first) ^ std::hash<U>()(x.second);
    }
  };

  int Bind(const Node& node, int max_retry) override {
    is_worker_ = (node.role == Node::WORKER ? true : false);
    bool is_server = (node.role == Node::SERVER ? true : false);
    int num_workers = Postoffice::Get()->num_workers();
    int num_servers = Postoffice::Get()->num_servers();
    int num_connections = 0;
    if (is_worker_) {
      num_connections = num_servers;
    } else if (is_server) {
      num_connections = num_workers;
    } else {
      num_connections = num_workers + num_servers;
    }
    mu_.lock();
    num_connections_ = num_connections;
    mu_.unlock();

    for (int i = 0; i < num_connections; i++) {
      // listen for num_connections times
      OfiListen();
    }

    int my_port = ZmqBind(node, max_retry);
    if (node.role == Node::SCHEDULER) {
      BootstrapScheduler(node, my_port);
    } else {
      BootstrapNonScheduler(node, my_port);
    }
    //auto t = new std::thread(&FabricVan::OfiAcceptThread, this);
    //thread_list_.push_back(t);
    return my_port;
  }

  void BootstrapScheduler(const Node& node, int my_port) {
    int num_workers = Postoffice::Get()->num_workers();
    int num_servers = Postoffice::Get()->num_servers();
    int total_num_instances = 1 + num_workers + num_servers;

    std::unordered_map<std::string, std::string> endpoint_map;
    // hostname:ip to tag
    std::unordered_map<std::string, int> tag_map;
    std::vector<Node> connected_nodes;

    for (int i = 0; i < num_workers + num_servers; i++) {
      Message msg_recv;
      int bytes = ZmqRecvMsg(&msg_recv);

      Node key = msg_recv.meta.control.node[0];
      std::string hostname = key.hostname;
      int port = key.port;
      std::string host_ip = hostname + ":" + std::to_string(port);
      LOG(INFO) << "Received " << bytes << " bytes in scheduler from "
                << host_ip << ". " << i  << "/" << num_workers + num_servers;

      Node value = msg_recv.meta.control.node[1];
      std::string ep_name = value.hostname;
      endpoint_map[host_ip] = ep_name;
      connected_nodes.push_back(key);
    }
    CHECK_EQ(static_cast<int>(endpoint_map.size()), num_workers + num_servers);
    CHECK_EQ(static_cast<int>(connected_nodes.size()), num_workers + num_servers);

    int server_count = 0;
    int worker_count = 0;
    int customer_id = -1;
    {
      std::lock_guard<std::mutex> lock(mu_);
      customer_id = customer_id_;
      // sort the nodes according their ip and port,
      std::sort(connected_nodes.begin(), connected_nodes.end(),
                [](const Node &a, const Node &b) {
                  return (a.hostname.compare(b.hostname) | (a.port < b.port)) > 0;
                });
      // assign node tags
      for (auto &node : connected_nodes) {
        std::string node_host_ip = node.hostname + ":" + std::to_string(node.port);
        if (node.role == Node::SERVER) {
          tag_map[node_host_ip] = ++server_count;
        } else {
          tag_map[node_host_ip] = ++worker_count;
        }
      }
    }

    Message req;
    req.meta.control.cmd = Control::BOOTSTRAP;
    req.meta.customer_id = customer_id;

    int fake_id = 0;
    {
      // key
      Node key;
      key.id = fake_id;
      key.role = Node::SCHEDULER;
      key.hostname = my_node_.hostname;
      key.port = my_port;
      // value
      Node value;
      std::string ep_name(ep_name_, ep_name_len_);
      value.hostname = ep_name;
      value.port = 1;
      req.meta.control.node.push_back(key);
      req.meta.control.node.push_back(value);
    }
    for (auto node: connected_nodes) {
      // key
      Node key;
      // fake id for creating zmq senders
      key.id = ++fake_id;
      key.role = node.role;
      key.hostname = node.hostname;
      key.port = node.port;
      ZmqConnect(key);
      // value
      Node value;
      std::string node_host_ip = node.hostname + ":" + std::to_string(node.port);
      value.hostname = endpoint_map[node_host_ip];
      value.port = tag_map[node_host_ip];
      req.meta.control.node.push_back(key);
      req.meta.control.node.push_back(value);
    }
    CHECK_EQ(fake_id, num_workers + num_servers);
    // Broadcast mapping to all nodes
    for (int i = 1; i <= num_workers + num_servers; i++) {
      req.meta.recver = i;
      int bytes = ZmqSendMsg(req);
      LOG(INFO) << "Send " << bytes << " to node " << i;
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      endpoint_map_ = endpoint_map;
      tag_map_ = tag_map;
      connected_nodes_ = connected_nodes;
    }
  }

  void BootstrapNonScheduler(const Node& node, int my_port) {
    // connect to scheduler
    Node sched_node;
    sched_node.id = kScheduler;
    sched_node.port = atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_PORT")));
    sched_node.hostname = std::string(CHECK_NOTNULL(Environment::Get()->find("DMLC_PS_ROOT_URI")));
    sched_node.role = Node::SCHEDULER;
    ZmqConnect(sched_node);

    // send my endpoint
    std::string ep_name;
    int customer_id;
    {
      std::lock_guard<std::mutex> lock(mu_);
      ep_name = std::string(ep_name_, ep_name_len_);
      customer_id = customer_id_;
    }
    std::string hostname(node.hostname);
    {
      // key
      Node key;
      key.role = node.role;
      key.hostname = hostname;
      key.port = my_port;
      // value
      Node value;
      value.role = node.role;
      value.hostname = ep_name;

      Message req;
      req.meta.recver = kScheduler;
      req.meta.control.cmd = Control::BOOTSTRAP;
      req.meta.control.node.push_back(key);
      req.meta.control.node.push_back(value);

      int bytes = ZmqSendMsg(req);
      LOG(INFO) << "Send " << bytes << " to the scheduler";
    }
    {
      // receive all endpoints info
      Message msg_recv;
      int bytes = ZmqRecvMsg(&msg_recv);
      LOG(INFO) << "Received " << bytes << " bytes from the scheduler";

      size_t kv_size = msg_recv.meta.control.node.size();
      int num_workers = Postoffice::Get()->num_workers();
      int num_servers = Postoffice::Get()->num_servers();
      int total_num_instances = 1 + num_workers + num_servers;
      CHECK_EQ(static_cast<int>(kv_size / 2), total_num_instances) << "Unexpected number of node infos";
      CHECK_EQ(msg_recv.meta.control.node.size() % 2, 0) << "Invalid number of key value pairs";

      bool contains_my_endpoint = false;
      for (int i = 0; i < total_num_instances; i++) {
        Node recv_key = msg_recv.meta.control.node[2 * i];
        std::string recv_hostname = recv_key.hostname;
        int recv_port = recv_key.port;
        std::string node_host_ip = recv_hostname + ":" + std::to_string(recv_port);
        Node recv_value = msg_recv.meta.control.node[2 * i + 1];
        std::string recv_ep_name = recv_value.hostname;

        std::lock_guard<std::mutex> lock(mu_);
        if (recv_port == my_port && recv_hostname.compare(my_node_.hostname) == 0) {
          if (std::string(ep_name_, ep_name_len_).compare(recv_ep_name) != 0) {
            char readable_ep_name[DMLC_PS_MAX_EP_ADDR] = {};
            size_t readable_ep_name_len = sizeof(readable_ep_name);
            fi_av_straddr(ofi_component_->av, recv_ep_name.c_str(), readable_ep_name, &readable_ep_name_len);
            LOG(FATAL) << i << ": recv_readable_ep_name = " << std::string(readable_ep_name, readable_ep_name + readable_ep_name_len);
          }
          my_tag_ = recv_value.port;
          contains_my_endpoint = true;
        }
        endpoint_map_[node_host_ip] = recv_ep_name;
      }
      CHECK(contains_my_endpoint) << "My endpoint is missing " << node.hostname << ":" << my_port;
    }
  }

  // scheduler binds
  int ZmqBind(const Node& node, int max_retry) {
    receiver_ = zmq_socket(context_, ZMQ_ROUTER);
    int option = 1;
    CHECK(!zmq_setsockopt(receiver_, ZMQ_ROUTER_MANDATORY, &option, sizeof(option)))
        << zmq_strerror(errno);
    CHECK(receiver_ != NULL)
        << "create receiver socket failed: " << zmq_strerror(errno);
    int local = GetEnv("DMLC_LOCAL", 0);
    std::string hostname = node.hostname.empty() ? "*" : node.hostname;
    int use_kubernetes = GetEnv("DMLC_USE_KUBERNETES", 0);
    if (use_kubernetes > 0 && node.role == Node::SCHEDULER) {
      hostname = "0.0.0.0";
    }
    std::string addr = local ? "ipc:///tmp/" : "tcp://" + hostname + ":";
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL) + port);
    for (int i = 0; i < max_retry + 1; ++i) {
      auto address = addr + std::to_string(port);
      if (zmq_bind(receiver_, address.c_str()) == 0) break;
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    std::lock_guard<std::mutex> lk(mu_);
    auto t = new std::thread(&FabricVan::ZmqRecvThread, this, (void*) receiver_);
    thread_list_.push_back(t);
    LOG(INFO) << node.hostname << " bound to port " << port;
    return port;
  }

  void ZmqConnect(const Node& node) {
    // use zmq to boostrap libfabric
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    int id = node.id;
    mu_.lock();
    auto it = senders_.find(id);
    if (it != senders_.end()) {
      zmq_close(it->second);
    }
    mu_.unlock();
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      LOG(INFO) << "skip connecting to my peers";
      return;
    }
    void* sender = zmq_socket(context_, ZMQ_DEALER);
    CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
    if (my_node_.id != Node::kEmpty) {
      std::string my_id = "ps" + std::to_string(my_node_.id);
      zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
      std::lock_guard<std::mutex> lk(mu_);
      if (is_worker_ && (senders_.find(id)==senders_.end())) {
        auto t = new std::thread(&FabricVan::ZmqRecvThread, this, (void*) sender);
        thread_list_.push_back(t);
      }
    }
    // connect
    std::string addr =
        "tcp://" + node.hostname + ":" + std::to_string(node.port);
    if (GetEnv("DMLC_LOCAL", 0)) {
      addr = "ipc:///tmp/" + std::to_string(node.port);
    }
    if (zmq_connect(sender, addr.c_str()) != 0) {
      LOG(FATAL) << "connect to " + addr + " failed: " + zmq_strerror(errno);
    }
    std::lock_guard<std::mutex> lk(mu_);
    senders_[id] = sender;
    if (my_node_.role != Node::SCHEDULER) {
      CHECK_EQ(senders_.size(), 1) << "Unexpected number of senders";
    }
    LOG(INFO) << "ZMQ sender " << id << " connected to " + addr;
  }

  int SendMsg(Message& msg) override {

    return 0;

  }

  //int OfiAcceptThread() {
  //  for (uint64_t i = 1; i <= num_connections_; i++) {
  //    uint64_t tag = (my_tag_ + i) % num_connections_;
  //    int ret = 0; // ncclSuccess;
  //    ssize_t rc = 0;
  //    recvComm_t *rComm = NULL;
  //    listenComm_t *lComm = (listenComm_t *)listenComm;
  //    int dev = lComm->dev;
  //    nccl_ofi_t *nccl_ofi_comp = nccl_ofi_component[dev];
  //    nccl_ofi_req_t *req = NULL;
  //    char remote_ep_addr[MAX_EP_ADDR] = {0};
  //    fi_addr_t remote_ep;
  //    uint64_t max_tag = nccl_ofi_comp->max_tag;
  //    size_t req_size = sizeof(nccl_ofi_req_t);


  //  
  //    if (nccl_ofi_comp == NULL) {
  //      ret = ncclSystemError;
  //      NCCL_OFI_WARN("NCCL OFI component for dev %d is uninitialised",
  //             dev);
  //      goto exit;
  //    }
  //  
  //    if (lComm->accepted == true) {
  //      ret = ncclSystemError;
  //      NCCL_OFI_WARN("listenComm object already has an active connection.");
  //      goto exit;
  //    }
  //  
  //    /* Allocate a NCCL OFI request */
  //    req = (nccl_ofi_req_t *)calloc(1, sizeof(nccl_ofi_req_t));
  //    if (OFI_UNLIKELY(req == NULL)) {
  //      NCCL_OFI_WARN("Unable to allocate nccl_ofi_req_t");
  //      ret = ncclSystemError;
  //      goto exit;
  //    }
  //    req->state = kRequestCreated;
  //  
  //    if (OFI_UNLIKELY(ret != 0))
  //      goto exit;
  //  
  //    req->lComm = lComm;
  //    req->dev = dev;
  //  
  //    /* Post a buffer for receiving connection requests */
  //    do {
  //      rc = fi_trecv(lComm->local_ep, (void *)&remote_ep_addr, MAX_EP_ADDR,
  //              NULL, FI_ADDR_UNSPEC, lComm->tag | ~max_tag,
  //              0, &req->ctx);
  //      if (rc == 0)
  //        break;
  //      else if (rc == -FI_EAGAIN) {
  //        /*
  //   *        * Process completions so that you have enough
  //   *               * resources for posting receive buffer
  //   *                      */
  //        ret = nccl_ofi_progress(nccl_ofi_comp);
  //        if (OFI_UNLIKELY(ret != 0))
  //          goto exit;
  //      }
  //      else {
  //        NCCL_OFI_WARN("Unable to post a buffer for receving connections for dev %d. RC: %zd, ERROR: %s",
  //               dev, rc, fi_strerror(-rc));
  //        ret = ncclSystemError;
  //        goto exit;
  //      }
  //    } while (true);
  //  
  //    /* Progress NCCL_OFI until connection is accepted */
  //    while (lComm->accepted == false) {
  //      ret = nccl_ofi_progress(nccl_ofi_comp);
  //      if (OFI_UNLIKELY(ret != 0))
  //        goto exit;
  //    }
  //  
  //    /* Insert remote EP address to AV */
  //    ret = fi_av_insert(nccl_ofi_comp->av, (void *)remote_ep_addr, 1,
  //           &remote_ep, 0, NULL);
  //    if (OFI_UNLIKELY(ret != 1)) {
  //      NCCL_OFI_WARN("Unable to insert remote address into address vector for device %d. RC: %d",
  //              dev, fi_strerror(-ret));
  //      ret = ncclSystemError;
  //      goto exit;
  //    }
  //  
  //    /* Build recvComm */
  //    rComm = (recvComm_t *)calloc(1, sizeof(recvComm_t));
  //    if (rComm == NULL) {
  //      NCCL_OFI_WARN("Unable to allocate receive Comm object for device %d",
  //             dev);
  //      ret = ncclSystemError;
  //      goto exit;
  //    }
  //  
  //    rComm->tag = lComm->tag;
  //    rComm->local_ep = lComm->local_ep;
  //    rComm->remote_ep = remote_ep;
  //    rComm->dev = dev;
  //  
  //    /* Pre-allocated buffers for data path */
  //    ret = allocate_ofi_fl(&rComm->reqs_fl, NCCL_OFI_MAX_REQUESTS,
  //              req_size);
  //    if (OFI_UNLIKELY(ret != 0)) {
  //      NCCL_OFI_WARN("Could not allocate NCCL OFI requests free list for dev %d",
  //             dev);
  //      goto exit;
  //    }
  //  
  //    *recvComm = rComm;
  //  
  //  exit:
  //    if (req)
  //      free(req);
  //    return ret;
  //  }
  //}


  int ZmqSendMsg(Message& msg) {
    //if (!is_worker_) return NonWorkerSendMsg(msg);

    std::lock_guard<std::mutex> lk(mu_);

    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);

    // find the socket
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "there is no socket to node " << id;
      return -1;
    }

    void* socket = it->second;

    return ZmqSendMsg(socket, msg);
  }

  int RecvMsg(Message* msg) override {
    return -1;
  }

  int ZmqRecvMsg(Message* msg) {
    msg->data.clear();

    ZmqBufferContext notification;
    recv_buffers_.WaitAndPop(&notification);

    size_t recv_bytes = 0;

    msg->meta.sender = notification.sender;
    msg->meta.recver = my_node_.id;

    char* meta_buf = CHECK_NOTNULL((char*)zmq_msg_data(notification.meta_zmsg));
    size_t meta_len = zmq_msg_size(notification.meta_zmsg);

    UnpackMeta(meta_buf, meta_len, &(msg->meta));
    recv_bytes += meta_len;

    //for (size_t i = 0; i < notification.data_zmsg.size(); ++i) {
    //  auto zmsg = notification.data_zmsg[i];
    //  char* buf = CHECK_NOTNULL((char*)zmq_msg_data(zmsg));
    //  size_t size = zmq_msg_size(zmsg);
    //  recv_bytes += size;


    //  SArray<char> data;
    //  // zero copy
    //  data.reset(buf, size, [zmsg, size](void *) {
    //    zmq_msg_close(zmsg);
    //    delete zmsg;
    //  });
    //  msg->data.push_back(data);
    //}

    return recv_bytes;
  }

 private:
   void OfiGetProvider() {
  }

  void PrintEndpointMap() {
    for (auto k_v: endpoint_map_) {
      //char readable_ep_name[DMLC_PS_MAX_EP_ADDR] = {};
      //size_t readable_ep_name_len = sizeof(readable_ep_name);
      //fi_av_straddr(ofi_component_->av, recv_ep_name.c_str(), readable_ep_name, &readable_ep_name_len);
      LOG(INFO) << k_v.first << "->" << k_v.second;
    }
  }

  void ZmqRecvThread(void* socket) {
    // TODO termintate this thread when done
    CHECK(socket);
    LOG(INFO) << "Start ZMQ recv thread";

    while (true) {
      ZmqBufferContext *buf_ctx = new ZmqBufferContext();

      for (int i = 0;; ++i) {
        zmq_msg_t* zmsg = new zmq_msg_t;
        CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
        while (true) {
          std::lock_guard<std::mutex> lk(mu_);
          // the zmq_msg_recv should be non-blocking, otherwise deadlock will happen
          int tag = ZMQ_DONTWAIT;
          if (should_stop_ || zmq_msg_recv(zmsg, socket, tag) != -1) break;
          if (errno == EINTR) {
            std::cout << "interrupted";
            continue;
          } else if (errno == EAGAIN) { // ZMQ_DONTWAIT
            continue;
          }
          CHECK(0) << "failed to receive message. errno: " << errno << " "
                       << zmq_strerror(errno);
        }
        if (should_stop_) break;
        char* buf = CHECK_NOTNULL((char*)zmq_msg_data(zmsg));
        size_t size = zmq_msg_size(zmsg);

        if (i == 0) {
          // identify
          buf_ctx->sender = GetNodeID(buf, size);
          CHECK(zmq_msg_more(zmsg));
          zmq_msg_close(zmsg);
          delete zmsg;
        }
        else if (i == 1) {
          // task
          buf_ctx->meta_zmsg = zmsg;
          bool more = zmq_msg_more(zmsg);
          if (!more) break;
        }
        else {
          buf_ctx->data_zmsg.push_back(zmsg);
          bool more = zmq_msg_more(zmsg);
          if (!more) break;
        }
      } // for
      if (should_stop_) break;
      recv_buffers_.Push(*buf_ctx);
    } // while
  }

  int ZmqSendMsg(void* socket, Message& msg) {
    // send meta
    int meta_size;
    char* meta_buf = nullptr;
    PackMeta(msg.meta, &meta_buf, &meta_size);
    int tag = ZMQ_SNDMORE;
    int n = msg.data.size();
    if (n == 0) tag = 0;
    zmq_msg_t meta_msg;
    zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData2, NULL);
    while (true) {
      if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
      if (errno == EINTR) continue;
      CHECK(0) << zmq_strerror(errno);
    }
    zmq_msg_close(&meta_msg);
    int send_bytes = meta_size;
    return send_bytes;
  }

  /**
   * return the node id given the received identity
   * \return -1 if not find
   */
  int GetNodeID(const char* buf, size_t size) {
    if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
      int id = 0;
      size_t i = 2;
      for (; i < size; ++i) {
        if (buf[i] >= '0' && buf[i] <= '9') {
          id = id * 10 + buf[i] - '0';
        } else {
          break;
        }
      }
      if (i == size) return id;
    }
    return Meta::kEmpty;
  }

  Node::Role GetRoleFromId(int id) {
    if (id < 8) return Node::SCHEDULER;
    if (id % 2) return Node::WORKER;
    return Node::SERVER;
  }

  void* context_ = nullptr;
  // node_id to the socket for sending data to this node
  std::unordered_map<int, void*> senders_;
  std::mutex mu_;
  void* receiver_ = nullptr;

  bool is_worker_;

  // Recv buffer queue
  ThreadsafeQueue<ZmqBufferContext> recv_buffers_;

  std::atomic<bool> should_stop_{false};

  std::vector<std::thread*> thread_list_;

  // NICs info list for a provider
  struct fi_info* ofi_provider_ = nullptr;
  // NCCL OFI component array for all NICs
  Endpoint* ofi_component_ = nullptr;
  // name of the endpoint
  char ep_name_[DMLC_PS_MAX_EP_ADDR] = {};
  // length of the name
  size_t ep_name_len_ = sizeof(ep_name_);

  // listen comms
  // tag -> comms
  std::unordered_map<uint64_t, ListenComm*> listen_comms_;
  std::unordered_map<uint64_t, RecvComm*> receive_comms_;

  // listen comm
  //ListenComm* listen_comm_ = nullptr;

  // * \brief node_id to the socket for sending data to this node
  // */
  //std::unordered_map<int, void*> senders_;
  //ZMQVan* zmq_van_ = new std::unique_ptr<ZMQVan>;
  int customer_id_;
  // hostname:ip to endpoint
  std::unordered_map<std::string, std::string> endpoint_map_;
  // hostname:ip to tag
  std::unordered_map<std::string, int> tag_map_;
  std::vector<Node> connected_nodes_;

  //
  uint64_t my_tag_;
  uint64_t num_connections_;
  std::unordered_map<std::string, SendComm*> send_comms_;

};
}  // namespace ps

#endif  // PS_FABRIC_VAN_H_
