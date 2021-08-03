/**
 *  Copyright (c) 2018-2019 Bytedance Inc.
 *  Author: zhuyibo@bytedance.com (Yibo Zhu)
 */
#ifndef PS_LITE_META_H_
#define PS_LITE_META_H_

#include <stdint.h>

namespace ps {

struct RawNode {
  // the node role
  int role;
  // node id
  int id;
  // hostname or ip
  char hostname[64];
  // number of ports
  int num_ports;
  // all the ports this node is binding
  int ports[32];
  // the port this node is binding (ports[0])
  int port;
  // the type of devices
  int dev_types[32];
  // the id of devices
  int dev_ids[32];
  // whether this node is created by failover
  bool is_recovery;
  // the locally unique id of an customer
  int customer_id;
  // endpoint name;
  char endpoint_name[64];
  // endpoint name len;
  size_t endpoint_name_len;
  // auxilary id
  int aux_id;
};

// system control info
struct RawControl {
  int cmd;
  int node_size;
  int barrier_group;
  uint64_t msg_sig;
};

// mete information about a message
struct RawMeta {
  // message.head
  int head;
  // message.body
  int body_size;
  // if set, then it is system control task. otherwise, it is for app
  RawControl control;
  // true: a request task
  // false: the response task to the request task with the same *time*
  bool request;
  // the unique id of an application
  int app_id;
  // the timestamp of this message
  int timestamp;
  // data type of message.data[i]
  int data_type_size;
  /** \brief src device type of message.data[i] */
  int src_dev_type;
  /** \brief src device id of message.data[i] */
  int src_dev_id;
  /** \brief dst device type of message.data[i] */
  int dst_dev_type;
  /** \brief dst device id of message.data[i] */
  int dst_dev_id;
  // the locally unique id of an customer
  int customer_id;
  // whether or not a push message
  bool push;
  // whether or not it's for SimpleApp
  bool simple_app;
  // message.data_size
  int data_size;
  // message.key
  uint64_t key;
  // message.addr
  uint64_t addr;
  // the length of the message's value
  int val_len;
  // the option field
  int option;
  // the sequence id
  int sid;

  // body
  // data_type
  // node
};

}  // namespace ps

#endif
