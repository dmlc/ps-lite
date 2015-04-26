#include "ps.h"
#include "parameter/parameter.h"
namespace ps {

void Parameter::ProcessRequest(Message* request) {
  const auto& call = request->task.param();
  Message* response = nullptr;
  bool push = call.push();
  if (!push) {
    // a pull request, need to reply with the value
    response = new Message(*request);
  }

  if (call.replica()) {
    // a replication request
    if (push) {
      SetReplica(request);
    } else {
      GetReplica(response);
    }
  } else {
    // a normal request
    if (push) {
      SetValue(request);
    } else {
      GetValue(response);
    }
  }

  if (response) Reply(request, response);
}

void Parameter::ProcessResponse(Message* response) {
  const auto& call = response->task.param();
  bool push = call.push();

  if (call.replica()) {
    // a replication response
    if (push) return; // an ACK response
    if (Range<Key>(response->task.key_range()) == MyKeyRange()) {
      Recover(response);
    } else {
      SetReplica(response);
    }
  } else {
    // a normal response
    if (!push) SetValue(response);
  }
}

}  // namespace ps
