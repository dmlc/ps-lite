#pragma once
#include <vector>
#include <limits>
#include <string>
#include "ps/sarray.h"
namespace ps {

/**
 * \brief data type
 */
enum DataType {
  CHAR, INT8, INT16, INT32, INT64,
  UINT8, UINT16, UINT32, UINT64,
  FLOAT, DOUBLE, OTHER
};

/**
 * \brief compare if V and W are the same type
 */
template<typename V, typename W>
inline bool SameType() {
  return std::is_same<typename std::remove_cv<V>::type, W>::value;
}

/**
 * \brief return the DataType of V
 */
template<typename V>
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
#if _MSC_VER <1900
    static const int kEmpty;
#else
    static const int kEmpty = std::numeric_limits<int>::max();
#endif
  /** \brief default constructor */
  Node() : id(kEmpty), port(kEmpty) {}
  /** \brief node roles */
  enum Role { SERVER, WORKER, SCHEDULER };
  /** \brief the role of this node */
  Role role;
  /** \brief node id */
  int id;
  /** \brief hostname or ip */
  std::string hostname;
  /** \brief the port this node is binding */
  int port;
};

/**
 * \brief meta info of a system control message
 */
struct Control {
  /** \brief empty constructor */
  Control() : cmd(EMPTY) { }
  /** \brief return true is empty */
  inline bool empty() const { return cmd == EMPTY; }
  /** \brief all commands */
  enum Command { EMPTY, TERMINATE, ADD_NODE, BARRIER };
  /** \brief the command */
  Command cmd;
  /** \brief node infos */
  std::vector<Node> node;
  /** \brief the node group for a barrier, such as kWorkerGroup */
  int barrier_group;
};

/**
 * \brief meta info of a message
 */
struct Meta {
  /** \brief the empty value */
#if _MSC_VER <1900
  static const int kEmpty;
#else
  static const int kEmpty = std::numeric_limits<int>::max();
#endif
  /** \brief default constructor */
  Meta() : head(kEmpty), customer_id(kEmpty), timestamp(kEmpty),
           sender(kEmpty), recver(kEmpty),
           request(false), simple_app(false) {}
  /** \brief an int head */
  int head;
  /** \brief the unique id of the customer is messsage is for*/
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
  /** \brief system control message */
  Control control;
};

/**
 * \brief messages that communicated amaong nodes.
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
    data.push_back(SArray<char>(val));
  }
};
}  // namespace ps
