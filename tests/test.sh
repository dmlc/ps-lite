export BINARY=${BINARY:-./test_benchmark}


function cleanup() {
    echo "kill all testing process of ps lite for user $USER"
    pkill -9 -f $BINARY
    sleep 1
}
trap cleanup EXIT
# cleanup # cleanup on startup

if [ "$#" != "5" ]; then
    echo "Usage: bash test.sh (local|remote|joint) bytes_per_msg msg_count (push_only|pull_only|push_pull) (cpu2cpu|cpu2gpu|gpu2gpu|gpu2cpu)"
    exit -1
fi

launch_mode=$1
bytes_per_msg=$2
msg_count=$3
op_type=$4
dev_type=$5

if [[ "$launch_mode" != "joint" && "$launch_mode" != "local" && "$launch_mode" != "remote" ]]; then
    echo "Unexpected launch mode. Please choose among: (local|remote|joint)"
    exit -1
fi

if [ "$op_type" == "push_only" ]; then
    op_code="2"
elif [ "$op_type" == "pull_only" ]; then
    op_code="3"
elif [ "$op_type" == "push_pull" ]; then
    op_code="1"
else
    echo "Unexpected op type. Please choose among: (push_only|pull_only|push_pull)"
    exit -1
fi

set -x

if [ "$dev_type" == "cpu2cpu" ]; then
    export TEST_NUM_GPU_WORKER=${TEST_NUM_GPU_WORKER:-0}
    export TEST_NUM_GPU_SERVER=${TEST_NUM_GPU_SERVER:=0}
    export TEST_NUM_CPU_WORKER=${TEST_NUM_CPU_WORKER:-1}
    export TEST_NUM_CPU_SERVER=${TEST_NUM_CPU_SERVER:-1}
    export DMLC_NUM_GPU_DEV=${DMLC_NUM_GPU_DEV:-0}
    export DMLC_NUM_CPU_DEV=${DMLC_NUM_CPU_DEV:-1}
elif [ "$dev_type" == "cpu2gpu" ]; then
    export TEST_NUM_GPU_WORKER=${TEST_NUM_GPU_WORKER:-0}
    export TEST_NUM_GPU_SERVER=${TEST_NUM_GPU_SERVER:=1}
    export TEST_NUM_CPU_WORKER=${TEST_NUM_CPU_WORKER:-1}
    export TEST_NUM_CPU_SERVER=${TEST_NUM_CPU_SERVER:-0}
    export ENABLE_RECV_BUFFER=${ENABLE_RECV_BUFFER:=1}
    export DMLC_NUM_GPU_DEV=${DMLC_NUM_GPU_DEV:-1}
    export DMLC_NUM_CPU_DEV=${DMLC_NUM_CPU_DEV:-0}
elif [ "$dev_type" == "gpu2gpu" ]; then
    export TEST_NUM_GPU_WORKER=${TEST_NUM_GPU_WORKER:-1}
    export TEST_NUM_GPU_SERVER=${TEST_NUM_GPU_SERVER:=1}
    export TEST_NUM_CPU_WORKER=${TEST_NUM_CPU_WORKER:-0}
    export TEST_NUM_CPU_SERVER=${TEST_NUM_CPU_SERVER:-0}
    export ENABLE_RECV_BUFFER=${ENABLE_RECV_BUFFER:=1}
    export DMLC_NUM_GPU_DEV=${DMLC_NUM_GPU_DEV:-1}
    export DMLC_NUM_CPU_DEV=${DMLC_NUM_CPU_DEV:-0}
elif [ "$dev_type" == "gpu2cpu" ]; then
    export TEST_NUM_GPU_WORKER=${TEST_NUM_GPU_WORKER:-1}
    export TEST_NUM_GPU_SERVER=${TEST_NUM_GPU_SERVER:=0}
    export TEST_NUM_CPU_WORKER=${TEST_NUM_CPU_WORKER:-0}
    export TEST_NUM_CPU_SERVER=${TEST_NUM_CPU_SERVER:-1}
    export DMLC_NUM_GPU_DEV=${DMLC_NUM_GPU_DEV:-1}
    export DMLC_NUM_CPU_DEV=${DMLC_NUM_CPU_DEV:-0}
else
    echo "Unexpected device type. Please choose among: (cpu2cpu|cpu2gpu|gpu2gpu|gpu2cpu)"
    exit -1
fi

export ARGS=${ARGS:-$bytes_per_msg 100000 $op_code}
export NUM_KEY_PER_SERVER=$msg_count

export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$UCX_HOME/lib:$LD_LIBRARY_PATH
# the number of workers
export DMLC_NUM_WORKER=${DMLC_NUM_WORKER:-1}
# the number of servers
export DMLC_NUM_SERVER=${DMLC_NUM_SERVER:=$DMLC_NUM_WORKER}
export DMLC_PS_ROOT_URI=${NODE_ONE_IP}
export DMLC_PS_ROOT_PORT=${DMLC_PS_ROOT_PORT:-12278} # scheduler's port (can random choose)

# ==============================
# ======= RDMAVan options ======
# ==============================
# enable rdma
export DMLC_ENABLE_RDMA=${DMLC_ENABLE_RDMA:-1}
# NIC for RDMAVan
export DMLC_INTERFACE=eth2


# =======================================
# ===== preset default UCX vars =========
# =======================================
export UCX_USE_MT_MUTEX=y
export UCX_SOCKADDR_CM_ENABLE=y
export UCX_RNDV_THRESH=8k
export UCX_IB_TRAFFIC_CLASS=236
export UCX_TLS=${UCX_TLS:=rc_x,ib,sm,tcp,cuda}
export UCX_LOG_LEVEL=info
# export UCX_IB_NUM_PATHS=${UCX_IB_NUM_PATHS:=2}
# export UCX_MEMTYPE_CACHE=n
# export UCX_RNDV_SCHEME=put_zcopy
# export UCX_IB_GPU_DIRECT_RDMA=no
# export UCX_MAX_RNDV_RAILS=2

# ==============================
# ======== UCXVan options ======
# ==============================
# enable ucx
export DMLC_ENABLE_UCX=${DMLC_ENABLE_UCX:-1}
export UCX_NET_DEVICES=${UCX_NET_DEVICES:=eth0,eth2,mlx5_2:1}
export BYTEPS_UCX_SHORT_THRESH=4096


export PS_VERBOSE=${PS_VERBOSE:-1}
export CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-0}
# export BYTEPS_ENABLE_IPC=0
# export DMLC_GROUP_SIZE=${DMLC_GROUP_SIZE:=1}

# ==============================
# ======== Test options ========
# ==============================
# export BENCHMARK_NTHREAD=${BENCHMARK_NTHREAD:=$DMLC_GROUP_SIZE}
export SKIP_DEV_ID_CHECK=${SKIP_DEV_ID_CHECK:-1}
export DMLC_RANK=${DMLC_RANK:=0}
export GDB=" gdb -ex run --args "
export GDB=" "

if [ "$launch_mode" == "local" ] # no other args
then
    # launch scheduler
    echo "This is the local node."
    export DMLC_NODE_HOST=${NODE_ONE_IP}
    export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_ONE_IP}

    DMLC_ROLE=scheduler $BINARY $ARGS &
    sleep 2
    DMLC_ROLE=server $GDB $BINARY $ARGS
elif [ "$launch_mode" == "remote" ]
then
    echo "This is the remote node."
    export DMLC_NODE_HOST=${NODE_TWO_IP}
    export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_TWO_IP}

    DMLC_ROLE=worker $GDB $BINARY $ARGS
elif [ "$launch_mode" == "joint" ]
then
    # launch scheduler
    echo "This is a joint node."
    export DMLC_NODE_HOST=${NODE_ONE_IP}
    export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_ONE_IP}

    DMLC_ROLE=scheduler $BINARY $ARGS &> sched.log &
    sleep 2
    DMLC_ROLE=server $GDB $BINARY $ARGS &> server.log &
    sleep 2
    DMLC_ROLE=worker $GDB $BINARY $ARGS 2>&1 | tee worker.log
fi
