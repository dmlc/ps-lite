# environment variables

The variables must be set for starting

- `DMLC_NUM_WORKER` : the number of workers
- `DMLC_NUM_SERVER` : the number of servers
- `DMLC_ROLE` : the role of the current node, can be `worker`, `server`, or `scheduler`
- `DMLC_PS_ROOT_URI` : the ip or hostname of the scheduler node
- `DMLC_PS_ROOT_PORT` : the port that the scheduler node is listening

additional variables:

- `DMLC_INTERFACE` : the network interface a node should use. in default choose
  automatically
- `DMLC_LOCAL` : runs in local machines, no network is needed
- `DMLC_PS_WATER_MARK`  : limit on the maximum number of outstanding messages
- `DMLC_PS_VAN_TYPE` : the type of the Van for transport, can be `ibverbs` for RDMA, `zmq` for TCP, `p3` for TCP with [priority based parameter propagation](https://anandj.in/wp-content/uploads/sysml.pdf).