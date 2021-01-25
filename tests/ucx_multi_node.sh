export BINARY=${BINARY:-./test_benchmark_stress}
export ARGS=${ARGS:-30000000 1000000000 0}

function cleanup() {
    echo "kill all testing process of ps lite for user $USER"
    if [[ $EUID -ne 0 ]]; then
        pkill -9 -u $USER -f $BINARY
    else
        pkill -9 -f $BINARY
    fi
    sleep 1
}
trap cleanup EXIT
# cleanup # cleanup on startup

export DMLC_NUM_WORKER=${DMLC_NUM_WORKER:-1}
export DMLC_NUM_SERVER=$DMLC_NUM_WORKER
export CUDA_HOME=
export UCX_HOME=
export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$UCX_HOME/lib

export DMLC_PS_ROOT_URI=${NODE_ONE_IP}
export BYTEPS_ORDERED_HOSTS=${NODE_ONE_IP},${NODE_TWO_IP}

export DMLC_PS_ROOT_PORT=${DMLC_PS_ROOT_PORT:-12279} # scheduler's port (can random choose)
export UCX_IB_TRAFFIC_CLASS=236
export DMLC_INTERFACE=eth2        # my RDMA interface
export DMLC_ENABLE_RDMA=1
export UCX_NET_DEVICES=mlx5_2:1
export UCX_MAX_RNDV_RAILS=${UCX_MAX_RNDV_RAILS:-2}

export DMLC_ENABLE_RDMA=${DMLC_ENABLE_RDMA:-1}        # enable rdma
export DMLC_ENABLE_UCX=${DMLC_ENABLE_UCX:-1}          # enable ucx
# export PS_VERBOSE=2

# export UCX_MEMTYPE_CACHE=n
# export UCX_RNDV_SCHEME=put_zcopy
# export BYTEPS_UCX_SHORT_THRESH=0

export LOCAL_SIZE=${LOCAL_SIZE:-2}               # test ucx gdr
export CUDA_VISIBLE_DEVICES=6,7
# export UCX_IB_GPU_DIRECT_RDMA=no

export BYTEPS_ENABLE_IPC=0
export BENCHMARK_NTHREAD=${BENCHMARK_NTHREAD:-8}

if [ $# -eq 0 ] # no other args
then
    # launch scheduler
    echo "This is scheduler node."
    export BYTEPS_NODE_ID=0
    export DMLC_NODE_HOST=${NODE_ONE_IP}
    export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_ONE_IP}

    DMLC_ROLE=scheduler $BINARY &
    if [ $DMLC_NUM_WORKER == "2" ]; then
      DMLC_ROLE=worker $BINARY $ARGS &
    fi
    # launch server
    DMLC_ROLE=server $BINARY
fi

export DMLC_NODE_HOST=${NODE_TWO_IP}
export UCX_RDMA_CM_SOURCE_ADDRESS=${NODE_TWO_IP}
export BYTEPS_NODE_ID=1

if [ $DMLC_NUM_WORKER == "2" ]; then
  DMLC_ROLE=server $BINARY &
fi
DMLC_ROLE=worker $BINARY $ARGS
