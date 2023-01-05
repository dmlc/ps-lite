#!/bin/bash
# set -x
if [ $# -lt 3 ]; then
    echo "usage: $0 num_servers num_workers bin [args..]"
    exit -1;
fi

export DMLC_NUM_SERVER=$1
shift
export DMLC_NUM_WORKER=$1
shift
bin=$1
shift
arg="$@"

# 首先启动Scheduler节点。这是要固定好Server和Worker数量，Scheduler节点管理所有节点的地址。
# 启动Worker或Server节点。每个节点要知道Scheduler节点的IP、port。启动时连接Scheduler节点，绑定本地端口，并向Scheduler节点注册自己信息（报告自己的IP，port）。
# Scheduler等待所有Worker节点都注册后，给其分配id，并把节点信息传送出去（例如Worker节点要知道Server节点IP和端口，Server节点要知道Worker节点的IP和端口）。此时Scheduler节点已经准备好。
# Worker或Server接收到Scheduler传送的信息后，建立对应节点的连接。此时Worker或Server已经准备好，会正式启动。

# start the scheduler
export DMLC_PS_ROOT_URI='127.0.0.1'
export DMLC_PS_ROOT_PORT=8000
export DMLC_ROLE='scheduler'
${bin} ${arg} &


# start servers
export DMLC_ROLE='server'
for ((i=0; i<${DMLC_NUM_SERVER}; ++i)); do
    export HEAPPROFILE=./S${i}
    ${bin} ${arg} &
done

# start workers
export DMLC_ROLE='worker'
for ((i=0; i<${DMLC_NUM_WORKER}; ++i)); do
    export HEAPPROFILE=./W${i}
    ${bin} ${arg} &
done

wait
