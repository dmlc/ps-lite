#pragma once
#include <limits>
#include "ps/internal/utils.h"
namespace ps {

#if USE_KEY32
/*! \brief Use unsigned 32-bit int as the key type */
using Key = uint32_t;
#else
/*! \brief Use unsigned 64-bit int as the key type */
using Key = uint64_t;
#endif

/*! \brief The maximal allowed key value */
static const Key kMaxKey = std::numeric_limits<Key>::max();

/** \brief node ID for the scheduler */
const static int kScheduler = 1;

/**
 * \brief the server node group ID
 *
 * group id can be combined:
 * - kServerGroup + kScheduler means all server nodes and the scheuduler
 * - kServerGroup + kWorkerGroup means all server and worker nodes
 */
const static int kServerGroup = 2;

/** \brief the worker node group ID */
const static int kWorkerGroup = 4;

}  // namespace ps
