#pragma once
#include "ps/internal/message.h"
#include "ps/internal/postoffice.h"
namespace ps {

/**
 * \brief the format for a received request or reponse for \ref SimpleApp
 */
struct SimpleData {
  /** \brief the int head */
  int head;
  /** \brief the string body */
  std::string body;
  /** \brief sender's node id */
  int sender;
  /** \brief the associated timestamp */
  int timestamp;
};

/**
 * \brief a simple app
 *
 * It provides basic communcation with a pair of int (head) and string (body)
 */
class SimpleApp {
 public:
  /**
   * \brief constructor
   * @param app_id the app id, should match with the remote node app with which this app
   * is communicated
   */
  SimpleApp(int app_id);

  /** \brief deconstructor */
  virtual ~SimpleApp() { delete obj_; obj_ = nullptr; }

  /**
   * \brief send a request to a remote node
   *
   * \param req_head request head
   * \param req_body request body
   * \param recv_id remote node id
   *
   * @return the timestamp of this request
   */
  int Request(int req_head, const std::string& req_body, int recv_id);

  /**
   * \brief wait until a request is finished
   *
   * \param timestamp
   */
  void Wait(int timestamp) { obj_->WaitRequest(timestamp); }


  /**
   * \brief send back a response for a request
   * \param recv_req the received request
   * \param the response body
   */
  void Response(const SimpleData& recv_req, const std::string& res_body = "");

  /**
   * \brief the handle to proces a received request/respoonse
   *
   * \param recved the received request or response
   * \param app this pointer
   */
  using Handle = std::function<void(const SimpleData& recved, SimpleApp* app)>;

  /**
   * \brief set the request handle
   * \param request_handle the request handle
   */
  void set_request_handle(const Handle& request_handle) {
    CHECK(request_handle) << "invalid request handle";
    request_handle_ = request_handle;
  }

  /**
   * \brief set the response handle
   * \param response_handle the response handle
   */
  void set_response_handle(const Handle& response_handle) {
    CHECK(response_handle) << "invalid response handle";
    response_handle_ = response_handle;
  }

  /**
   * \brief returns the customer
   */
  Customer* get_customer() { return obj_; }
 protected:
  /** \brief empty construct */
  SimpleApp() : obj_(nullptr) {
    request_handle_ = [](const SimpleData& recved, SimpleApp* app) {
      app->Response(recved);
    };
    response_handle_ = [](const SimpleData& recved, SimpleApp* app) { };
  }

  /** \brief process a received message */
  void Process(const Message& msg);

  /** \brief ps internal object */
  Customer* obj_;

 private:
  /** \brief request handle */
  Handle request_handle_;
  /** \brief request handle */
  Handle response_handle_;
};

////////////////////////////////////////////////////////////////////////////////

SimpleApp::SimpleApp(int app_id) : SimpleApp() {
  using namespace std::placeholders;
  obj_ = new Customer(app_id, std::bind(&SimpleApp::Process, this, _1));
}

int SimpleApp::Request(int req_head, const std::string& req_body, int recv_id) {
  // setup message
  Message msg;
  msg.meta.set_head(req_head);
  if (req_body.size()) msg.meta.set_body(req_body);
  int ts = obj_->NewRequest(recv_id);
  msg.meta.set_timestamp(ts);
  msg.meta.set_request(true);
  msg.meta.set_simple_app(true);
  msg.meta.set_customer_id(obj_->id());

  // send
  for (int r : Postoffice::Get()->GetNodeIDs(recv_id)) {
    msg.recver = r;
    Postoffice::Get()->van()->Send(msg);
  }
  return ts;
}

void SimpleApp::Response(const SimpleData& req, const std::string& res_body) {
  // setup message
  Message msg;
  msg.meta.set_head(req.head);
  if (res_body.size()) msg.meta.set_body(res_body);
  msg.meta.set_timestamp(req.timestamp);
  msg.meta.set_request(false);
  msg.meta.set_simple_app(true);
  msg.meta.set_customer_id(obj_->id());
  msg.recver = req.sender;

  // send
  Postoffice::Get()->van()->Send(msg);
}


void SimpleApp::Process(const Message& msg) {
  SimpleData recv;
  recv.sender    = msg.sender;
  recv.head      = msg.meta.head();
  recv.body      = msg.meta.body();
  recv.timestamp = msg.meta.timestamp();
  if (msg.meta.request()) {
    CHECK(request_handle_);
    request_handle_(recv, this);
  } else {
    CHECK(response_handle_);
    response_handle_(recv, this);
  }
}

}  // namespace ps
