export BINARY=${BINARY:-./tests/test_benchmark_stress}
export ARGS=${ARGS:-30000000 1000 0}

set -x

function cleanup() {
    echo "kill all testing process of ps lite for user $USER"
    pkill -9 -f $BINARY
    sleep 1
}
trap cleanup EXIT
cleanup # cleanup on startup

export DMLC_NUM_WORKER=${DMLC_NUM_WORKER:-2}
export DMLC_NUM_SERVER=$DMLC_NUM_WORKER
export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$UCX_HOME/lib:$LD_LIBRARY_PATH

export DMLC_PS_ROOT_URI=${NODE_ONE_IP}
export BYTEPS_ORDERED_HOSTS=${NODE_ONE_IP},${NODE_TWO_IP}

export DMLC_PS_ROOT_PORT=${DMLC_PS_ROOT_PORT:-12278} # scheduler's port (can random choose)
export UCX_IB_TRAFFIC_CLASS=236
export DMLC_INTERFACE=eth2        # my RDMA interface
export UCX_NET_DEVICES=${UCX_NET_DEVICES:-mlx5_2:1}
export UCX_MAX_RNDV_RAILS=${UCX_MAX_RNDV_RAILS:-2}

export DMLC_ENABLE_RDMA=${DMLC_ENABLE_RDMA:-1}        # enable rdma
export DMLC_ENABLE_UCX=${DMLC_ENABLE_UCX:-1}          # enable ucx
export PS_VERBOSE=${PS_VERBOSE:-1}

# export UCX_MEMTYPE_CACHE=n
# export UCX_RNDV_SCHEME=put_zcopy
# export BYTEPS_UCX_SHORT_THRESH=0
# export UCX_IB_GPU_DIRECT_RDMA=no

# ========================================
# NOTE: preset default env vars for UCXVan
# ========================================
export UCX_USE_MT_MUTEX=y
export UCX_IB_NUM_PATHS=2
export UCX_SOCKADDR_CM_ENABLE=y
export UCX_RNDV_THRESH=8k
# ========================================

export BYTEPS_ENABLE_IPC=0
export BENCHMARK_NTHREAD=${BENCHMARK_NTHREAD:-8}

if [ $# -eq 0 ] # no other args
then
    # launch scheduler
    echo "This is a local node."
    export BYTEPS_NODE_ID=0
    export DMLC_NODE_HOST=${NODE_ONE_IP}
    export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_ONE_IP}

    DMLC_ROLE=scheduler $BINARY &
    DMLC_ROLE=joint $BINARY $ARGS
fi

echo "This is a remote node."
export DMLC_NODE_HOST=${NODE_TWO_IP}
export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_TWO_IP}
export BYTEPS_NODE_ID=1

DMLC_ROLE=joint $BINARY $ARGS
