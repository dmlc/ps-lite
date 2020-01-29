/**
 *  Copyright (c) 2018-2019 Bytedance Inc.
 *  Author: zhuyibo@bytedance.com (Yibo Zhu)
*/
#ifndef PS_LITE_META_H_
#define PS_LITE_META_H_

#include<stdint.h>

namespace ps {

struct RawNode {
  // the node role
  int role;
  // node id
  int id;
  // hostname or ip
  char hostname[64];
  // the port this node is binding
  int port;
  // whether this node is created by failover
  bool is_recovery;
  // the locally unique id of an customer
  int customer_id;
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

  // body
  // data_type
  // node
};

} // namespace

#endif
