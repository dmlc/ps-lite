# scheduler's port
set -x

export DMLC_NODE_HOST=""
export DMLC_NUM_PORTS=2
export DMLC_PS_ROOT_URI=10.188.139.15
export DMLC_PS_ROOT_PORT=9999
export DMLC_ENABLE_RDMA="multivan"
export BYTEPS_ENABLE_IPC=0
export DMLC_NUM_WORKER=2
export DMLC_NUM_SERVER=2
export PS_VERBOSE=2
export DMLC_INTERFACE=eth2
export NUM_KEY_PER_SERVER=2

ROLE=$1
if [ "$1" == "worker" ]; then
    DMLC_ROLE=$1 ./test_benchmark 409600000 5 1
else
    DMLC_ROLE=$1 ./test_benchmark
fi