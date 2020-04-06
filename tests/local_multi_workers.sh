#!/bin/bash
# set -x
if [ $# -lt 3 ]; then
    echo "usage: $0 num_servers num_workers bin [args..]"
    exit -1;
fi

#export FI_LOG_LEVEL=Debug
export DMLC_NUM_SERVER=$1
shift
export DMLC_NUM_WORKER=$1
shift
bin=$1
shift
arg="$@"

# start the scheduler
# export FI_LOG_LEVEL=Debug
export DMLC_PS_ROOT_URI=${DMLC_PS_ROOT_URI:-'172.31.93.7'}
export DMLC_PS_ROOT_PORT=${DMLC_PS_ROOT_PORT:-8000}
export DMLC_INTERFACE=${DMLC_INTERFACE:-eth0}

if [ $DMLC_ROLE == 'scheduler' ]; then
    (${bin} ${arg} 2>&1 | tee sched.log)
    # (numactl --physcpubind=0-35 --membind=0 ${bin} ${arg} 2>&1 | tee sched.log)
    # gdb --args ${bin} ${arg}
elif [ $DMLC_ROLE == 'server' ]; then
    for ((i=0; i<${DMLC_NUM_SERVER}; ++i)); do
        export HEAPPROFILE=./S${i}
        # (${bin} ${arg} &)
        # (${bin} ${arg} 2>&1 | tee server.log)
        (numactl --physcpubind=0-35 --membind=0 ${bin} ${arg} 2>&1 | tee server.log)
        # gdb --args ${bin} ${arg}
    done
elif [ $DMLC_ROLE == 'worker' ]; then
    for ((i=0; i<${DMLC_NUM_WORKER}; ++i)); do
        export HEAPPROFILE=./S${i}
        # (${bin} ${arg} &)
        # (${bin} ${arg} 2>&1 | tee worker.log)
        (numactl --physcpubind=0-35 --membind=0 ${bin} ${arg} 2>&1 | tee worker.log)
    done
fi
wait
