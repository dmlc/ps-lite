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
export DMLC_PS_ROOT_URI=${DMLC_PS_ROOT_URI:-'127.0.0.1'}
export DMLC_PS_ROOT_PORT=${DMLC_PS_ROOT_PORT:-8000}
export DMLC_INTERFACE=${DMLC_INTERFACE:-eth0}

if [ $DMLC_ROLE == 'scheduler' ]; then
    (${bin} ${arg} 2>&1 | tee sched.log)
elif [ $DMLC_ROLE == 'server' ]; then
    for ((i=0; i<${DMLC_NUM_SERVER}; ++i)); do
        export HEAPPROFILE=./S${i}
        (${bin} ${arg} 2>&1 | tee server.log &)
    done
fi
wait
