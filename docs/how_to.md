# How To

## Debug PS-Lite

We can let PS-Lite print debug information by specify the environment variable
`PS_VERBOSE`.
- `PS_VERBOSE=1`: print connection information
- `PS_VERBOSE=2`: print all data communication information

For example, first run `make test; cd tests` in the root directory. Then
```bash
export PS_VERBOSE=1; ./local.sh 1 1 ./test_connection
```
Possible outputs are
```bash
[19:57:18] src/van.cc:72: Node Info: role=schedulerid=1, ip=127.0.0.1, port=8000
[19:57:18] src/van.cc:72: Node Info: role=worker, ip=128.2.211.110, port=58442
[19:57:18] src/van.cc:72: Node Info: role=server, ip=128.2.211.110, port=40112
[19:57:18] src/van.cc:336: assign rank=8 to node role=server, ip=128.2.211.110, port=40112
[19:57:18] src/van.cc:336: assign rank=9 to node role=worker, ip=128.2.211.110, port=58442
[19:57:18] src/van.cc:347: the scheduler is connected to 1 workers and 1 servers
[19:57:18] src/van.cc:354: S[8] is connected to others
[19:57:18] src/van.cc:354: W[9] is connected to others
[19:57:18] src/van.cc:296: H[1] is stopped
[19:57:18] src/van.cc:296: S[8] is stopped
[19:57:18] src/van.cc:296: W[9] is stopped
```
where `H`, `S` and `W` stand for scheduler, server, and workers respectively.

### Use a Particular Network Interface ###

PS-Lite often chooses the first available network interface. But for machines
have multiple interface, we can specify which network interface to use for data
communication by the environment variable `DMLC_INTERFACE`. For example, to use
the infinite-band interface `ib0`, we can
```bash
export DMLC_INTERFACE=ib0; commands_to_run
```

If all ps-lite nodes run in the same machine, we can also set `DMLC_LOCAL` to be
true. Then it will use memory copy rather than the local network interface,
which sometimes improve the performance:
```bash
export DMLC_LOCAL=1; commands_to_run
```

## Environment Variables to Start PS-Lite

This section is useful if we want to port PS-Lite to other cluster resource
managers besides the supported one including `ssh`, `mpirun`, `yarn` and `sge`.

To start a PS-Lite node, we need to give proper values to the following
environment variables.
- `DMLC_NUM_WORKER` : the number of workers
- `DMLC_NUM_SERVER` : the number of servers
- `DMLC_ROLE` : the role of the current node, can be `worker`, `server`, or `scheduler`
- `DMLC_PS_ROOT_URI` : the ip or hostname of the scheduler node
- `DMLC_PS_ROOT_PORT` : the port that the scheduler node is listening
