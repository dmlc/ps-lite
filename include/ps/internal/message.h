/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_MESSAGE_H_
#define PS_INTERNAL_MESSAGE_H_
#include <array>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "ps/sarray.h"
namespace ps {
/** \brief data type */
enum DataType {
  CHAR,
  INT8,
  INT16,
  INT32,
  INT64,
  UINT8,
  UINT16,
  UINT32,
  UINT64,
  FLOAT,
  DOUBLE,
  OTHER
};

/** \brief data type name */
static const char* DataTypeName[] = {"CHAR",   "INT8",  "INT16",  "INT32",
                                     "INT64",  "UINT8", "UINT16", "UINT32",
                                     "UINT64", "FLOAT", "DOUBLE", "OTHER"};

/**
 * \brief compare if V and W are the same type
 */
template <typename V, typename W>
inline bool SameType() {
  return std::is_same<typename std::remove_cv<V>::type, W>::value;
}
/**
 * \brief return the DataType of V
 */
template <typename V>
DataType GetDataType() {
  if (SameType<V, int8_t>()) {
    return INT8;
  } else if (SameType<V, int16_t>()) {
    return INT16;
  } else if (SameType<V, int32_t>()) {
    return INT32;
  } else if (SameType<V, int64_t>()) {
    return INT64;
  } else if (SameType<V, uint8_t>()) {
    return UINT8;
  } else if (SameType<V, uint16_t>()) {
    return UINT16;
  } else if (SameType<V, uint32_t>()) {
    return UINT32;
  } else if (SameType<V, uint64_t>()) {
    return UINT64;
  } else if (SameType<V, float>()) {
    return FLOAT;
  } else if (SameType<V, double>()) {
    return DOUBLE;
  } else {
    return OTHER;
  }
}
/**
 * \brief information about a node
 */
struct Node {
  /** \brief the empty value */
  static const int kEmpty;
  /** \brief default constructor */
  Node() : id(kEmpty), port(kEmpty), is_recovery(false), aux_id(-1) {}
  /** \brief node roles */
  enum Role { SERVER, WORKER, SCHEDULER, JOINT };
  /** \brief get debug string */
  std::string DebugString() const {
    std::stringstream ss;
    ss << "[role="
       << (role == SERVER ? "server"
                          : (role == WORKER ? "worker" : "scheduler"))
       << (id != kEmpty ? ", id=" + std::to_string(id) : "")
       << ", ip=" << hostname << ", port=" << port
       << ", is_recovery=" << is_recovery << ", aux_id=" << aux_id
       << ", num_ports=" << num_ports;
    if (num_ports > 1) {
      ss << ", ports=[";
      for (int i = 0; i < num_ports; ++i) {
        ss << ports[i] << ",";
      }
      ss << "], devices=[";
      for (int i = 0; i < num_ports; ++i) {
        ss << DeviceTypeName[dev_types[i]] << "[" << dev_ids[i] << "],";
      }
      ss << "]";
    }
    if (endpoint_name_len > 0) {
      ss << ", endpoint_name_len=" << endpoint_name_len << ", endpoint_name={";
      for (size_t i = 0; i < endpoint_name_len; i++) {
        ss << std::to_string(endpoint_name[i]) + ",";
      }
      ss << "}";
    }
    ss << "]";
    return ss.str();
  }
  /** \brief get short debug string */
  std::string ShortDebugString() const {
    std::string str = role == SERVER ? "S" : (role == WORKER ? "W" : "H");
    if (id != kEmpty) str += "[" + std::to_string(id) + "]";
    return str;
  }
  /** \brief the role of this node */
  Role role;
  /** \brief node id */
  int id;
  /** \brief customer id */
  int customer_id;
  /** \brief hostname or ip */
  std::string hostname;
  /** \brief number of ports */
  int num_ports;
  /** \brief the port this node is binding */
  std::array<int, 32> ports;
  /** \brief the types of devices this node is binding */
  std::array<int, 32> dev_types;
  /** \brief the ids of devices this node is binding */
  std::array<int, 32> dev_ids;
  /** \brief the same port as ports[0] */
  int port;
  /** \brief whether this node is created by failover */
  bool is_recovery;
  /** \brief endpoint name */
  char endpoint_name[64];
  /** \brief the length of the endpoint name */
  size_t endpoint_name_len = 0;
  /** \brief the auxilary id. currently used for fabric van communication setup
   */
  /** also used for the rank assignment phase for p2p communication**/
  int aux_id = -1;
};

/**
 * \brief meta info of a system control message
 */
struct Control {
  /** \brief empty constructor */
  Control() : cmd(EMPTY) {}
  /** \brief return true is empty */
  inline bool empty() const { return cmd == EMPTY; }
  /** \brief get debug string */
  std::string DebugString() const {
    if (empty()) return "";
    std::vector<std::string> cmds = {
        "EMPTY",         "TERMINATE",       "ADD_NODE",  "BARRIER",
        "ACK",           "HEARTBEAT",       "BOOTSTRAP", "ADDR_REQUEST",
        "ADDR_RESOLVED", "INSTANCE_BARRIER"};
    std::stringstream ss;
    ss << "cmd=" << cmds[cmd];
    if (node.size()) {
      ss << ", node={";
      for (const Node& n : node) ss << " " << n.DebugString();
      ss << " }";
    }
    if (cmd == BARRIER || cmd == INSTANCE_BARRIER)
      ss << ", barrier_group=" << barrier_group;
    if (cmd == ACK) ss << ", msg_sig=" << msg_sig;
    return ss.str();
  }
  /** \brief all commands */
  enum Command {
    EMPTY,
    TERMINATE,
    ADD_NODE,
    BARRIER,
    ACK,
    HEARTBEAT,
    BOOTSTRAP,
    ADDR_REQUEST,
    ADDR_RESOLVED,
    INSTANCE_BARRIER
  };
  /** \brief the command */
  Command cmd;
  /** \brief node infos */
  std::vector<Node> node;
  /** \brief the node group for a barrier, such as kWorkerGroup */
  int barrier_group;
  /** message signature */
  uint64_t msg_sig;
};
/**
 * \brief meta info of a message
 */
struct Meta {
  /** \brief the empty value */
  static const int kEmpty;
  /** \brief default constructor */
  Meta()
      : head(kEmpty),
        app_id(kEmpty),
        customer_id(kEmpty),
        timestamp(kEmpty),
        sender(kEmpty),
        recver(kEmpty),
        request(false),
        push(false),
        simple_app(false) {}
  std::string DebugString() const {
    std::stringstream ss;
    if (sender == Node::kEmpty) {
      ss << "?";
    } else {
      ss << sender;
    }
    ss << " => " << recver;
    ss << ". Meta: request=" << request;
    if (timestamp != kEmpty) ss << ", timestamp=" << timestamp;
    if (!control.empty()) {
      ss << ", control={ " << control.DebugString() << " }";
    } else {
      ss << ", app_id=" << app_id << ", customer_id=" << customer_id
         << ", simple_app=" << simple_app << ", push=" << push
         << ", sid=" << sid;
    }
    if (head != kEmpty) ss << ", head=" << head;
    if (control.empty() && !simple_app)
      ss << ", key=" << key;  // valid data msg
    if (body.size()) ss << ", body=" << body;
    if (data_type.size()) {
      ss << ", dtype={";
      for (auto d : data_type) ss << " " << DataTypeName[static_cast<int>(d)];
      ss << " }";
    }
    if (!control.empty() || simple_app) ss << ". NOT DATA MSG!";
    return ss.str();
  }
  /** \brief an int head */
  int head;
  /** \brief the unique id of the application of messsage is for*/
  int app_id;
  /** \brief customer id*/
  int customer_id;
  /** \brief the timestamp of this message */
  int timestamp;
  /** \brief the node id of the sender of this message */
  int sender;
  /** \brief the node id of the receiver of this message */
  int recver;
  /** \brief whether or not this is a request message*/
  bool request;
  /** \brief whether or not a push message */
  bool push;
  /** \brief whether or not it's for SimpleApp */
  bool simple_app;
  /** \brief an string body */
  std::string body;
  /** \brief data type of message.data[i] */
  std::vector<DataType> data_type;
  /** \brief src device type of message.data[i] */
  DeviceType src_dev_type = UNK;
  /** \brief src device id of message.data[i] */
  int src_dev_id = -1;
  /** \brief dst device type of message.data[i] */
  DeviceType dst_dev_type = UNK;
  /** \brief dst device id of message.data[i] */
  int dst_dev_id = -1;
  /** \brief system control message */
  Control control;
  /** \brief the byte size */
  int data_size = 0;
  /** \brief the key */
  uint64_t key;
  /** \brief the address */
  uint64_t addr = 0;
  /** \brief the value length */
  int val_len;
  /** \brief the optional 4-bytes field */
  int option;
  /** \brief the sequence id (used by ucx) */
  int sid;
};
/**
 * \brief messages that communicated among nodes.
 */
struct Message {
  /** \brief the meta info of this message */
  Meta meta;
  /** \brief the large chunk of data of this message */
  std::vector<SArray<char> > data;
  /**
   * \brief push array into data, and add the data type
   */
  template <typename V>
  void AddData(const SArray<V>& val) {
    CHECK_EQ(data.size(), meta.data_type.size());
    meta.data_type.push_back(GetDataType<V>());
    SArray<char> bytes(val);
    meta.data_size += bytes.size();
    data.push_back(bytes);
    // the data/value array
    if (data.size() == 2) {
      meta.src_dev_type = val.src_device_type_;
      meta.src_dev_id = val.src_device_id_;
      meta.dst_dev_type = val.dst_device_type_;
      meta.dst_dev_id = val.dst_device_id_;
    }
  }
  std::string DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " Body: { ";
      ss << DeviceTypeName[meta.src_dev_type] << "(" << meta.src_dev_id << ")->"
         << DeviceTypeName[meta.dst_dev_type] << "(" << meta.dst_dev_id << ") "
         << "data_size=[";
      for (const auto& d : data) {
        ss << d.size() << ",";
      }
      ss << "] }";
    }
    return ss.str();
  }
};
}  // namespace ps
#endif  // PS_INTERNAL_MESSAGE_H_
