/*!
 * @file   ps.h
 * \brief  The parameter server interface
 */
#pragma once
#include <functional>
#include "dmlc/io.h"
#include "system/postoffice.h"
namespace ps {

inline int NextID() {
  return Postoffice::instance().manager().NextCustomerID();
}

inline void StartSystem(int* argc, char ***argv) {
  Postoffice::instance().Run(argc, argv);
}

inline void StopSystem() {
  Postoffice::instance().Stop();
}

inline int RunSystem(int* argc, char ***argv) {
  StartSystem(argc, argv); StopSystem();
  return 0;
}
}  // namespace ps

/// \brief worker node api
#include "ps/worker.h"

/// \brief server node api
#include "ps/server.h"

/// \brief scheduler node api
#include "ps/scheduler.h"

/// \brief node runtime info
#include "ps/node_info.h"


/// \brief implementation
#include "system/ps-inl.h"
