#include "system/env.h"
#include "system/van.h"
#include "base/common.h"
#include "base/local_machine.h"
#include "base/dir.h"
// #include "data/common.h"
#include "proto/node.pb.h"
namespace ps {

DEFINE_int32(num_servers, 0, "number of servers");
DEFINE_int32(num_workers, 0, "number of clients");
DEFINE_int32(num_replicas, 0, "number of replicas per server node");

DEFINE_string(my_node, "role:SCHEDULER,hostname:'127.0.0.1',port:8000,id:'H'", "my node");
DEFINE_string(scheduler, "role:SCHEDULER,hostname:'127.0.0.1',port:8000,id:'H'", "the scheduler node");
DEFINE_int32(my_rank, -1, "my rank among MPI peers");
DEFINE_string(interface, "", "network interface");

void Env::Init(char* argv0) {
  if (getenv("DMLC_PS_ROOT_URI")) {
    InitDMLC();
  }

  InitGlog(argv0);
}

void Env::InitGlog(char* argv0) {
  if (FLAGS_log_dir.empty()) FLAGS_log_dir = "/tmp";
  if (!DirExists(FLAGS_log_dir)) { CreateDir(FLAGS_log_dir); }
  // change the hostname in default log filename to node id
  string logfile = FLAGS_log_dir + "/" + string(basename(argv0)) + "."
                   + Van::ParseNode(FLAGS_my_node).id() + ".log.";
  google::SetLogDestination(google::INFO, (logfile+"INFO.").c_str());
  google::SetLogDestination(google::WARNING, (logfile+"WARNING.").c_str());
  google::SetLogDestination(google::ERROR, (logfile+"ERROR.").c_str());
  google::SetLogDestination(google::FATAL, (logfile+"FATAL.").c_str());
  google::SetLogSymlink(google::INFO, "");
  google::SetLogSymlink(google::WARNING, "");
  google::SetLogSymlink(google::ERROR, "");
  google::SetLogSymlink(google::FATAL, "");
  FLAGS_logbuflevel = -1;
}

void Env::InitDMLC() {
  Node scheduler;
  scheduler.set_hostname(std::string(CHECK_NOTNULL(getenv("DMLC_PS_ROOT_URI"))));
  scheduler.set_port(atoi(CHECK_NOTNULL(getenv("DMLC_PS_ROOT_PORT"))));
  scheduler.set_role(Node::SCHEDULER);
  scheduler.set_id("H");
  CHECK(google::protobuf::TextFormat::PrintToString(scheduler, &FLAGS_scheduler));

  FLAGS_num_workers = atoi(CHECK_NOTNULL(getenv("DMLC_NUM_WORKER")));
  FLAGS_num_servers = atoi(CHECK_NOTNULL(getenv("DMLC_NUM_SERVER")));

  AssembleMyNode();
}

void Env::AssembleMyNode() {
  // get my role
  char* role_str = getenv("DMLC_ROLE");
  Node::Role role;
  string id;
  if (role_str != NULL) {
    if (!strcmp(role_str, "scheduler")) {
      FLAGS_my_node = FLAGS_scheduler;
      return;
    } else if (!strcmp(role_str, "worker")) {
      role = Node::WORKER;
    } else if (!strcmp(role_str, "server")) {
      role = Node::SERVER;
    } else {
      LOG(FATAL) << "unknown role: " << *role_str;
    }
  } else {
    int my_rank;
    char* rank_str = getenv("PMI_RANK");
    if (!rank_str) {
      rank_str = getenv("OMPI_COMM_WORLD_RANK");
    }
    CHECK(rank_str != NULL) << "fail to get rank size";
    my_rank = atoi(rank_str);

    // role and id
    CHECK_LT(my_rank, FLAGS_num_workers + FLAGS_num_servers);
    if (my_rank < FLAGS_num_workers) {
      role = Node::WORKER;
      id = "W" + std::to_string(my_rank);
    } else { // if (my_rank < FLAGS_num_workers + FLAGS_num_servers) {
      role = Node::SERVER;
      id = "S" + std::to_string(my_rank - FLAGS_num_workers);
    }
  }

  Node node; node.set_role(role);
  if (!id.empty()) node.set_id(id);

  // IP, port and interface
  string ip;
  string interface = FLAGS_interface;
  unsigned short port;

  if (interface.empty()) {
    LocalMachine::pickupAvailableInterfaceAndIP(interface, ip);
  } else {
    ip = LocalMachine::IP(interface);
  }
  CHECK(!ip.empty()) << "failed to got ip";
  CHECK(!interface.empty()) << "failed to got the interface";
  port = LocalMachine::pickupAvailablePort();
  CHECK_NE(port, 0) << "failed to get port";
  node.set_hostname(ip);
  node.set_port(static_cast<int32>(port));

  CHECK(google::protobuf::TextFormat::PrintToString(node, &FLAGS_my_node));
}



}  // namespace ps
