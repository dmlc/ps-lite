#!/usr/bin/env python
"""
DMLC submission script, local machine version
"""

import argparse
import sys
import os
import subprocess
from threading import Thread
import tracker
import signal
import logging

parser = argparse.ArgumentParser(description='DMLC script to submit dmlc jobs as local process')
parser.add_argument('-n', '--nworker', required=True, type=int,
                    help = 'number of worker nodes to be launched')
parser.add_argument('-s', '--server-nodes', default = 0, type=int,
                    help = 'number of server nodes to be launched')
parser.add_argument('--log-level', default='INFO', type=str,
                    choices=['INFO', 'DEBUG'],
                    help = 'logging level')
parser.add_argument('--log-file', type=str,
                    help = 'output log to the specific log file')
parser.add_argument('command', nargs='+',
                    help = 'command for launching the program')
args, unknown = parser.parse_known_args()


keepalive = """
nrep=0
rc=254
while [ $rc -eq 254 ];
do
    export DMLC_NUM_ATTEMPT=$nrep
    %s
    rc=$?;
    nrep=$((nrep+1));
done
"""

def exec_cmd(cmd, role, pass_env):
    env = os.environ.copy()
    for k, v in pass_env.items():
        env[k] = str(v)

    env['DMLC_ROLE'] = role

    ntrial = 0
    while True:
        if os.name == 'nt':
            env['DMLC_NUM_ATTEMPT'] = str(ntrial)
            ret = subprocess.call(cmd, shell=True, env = env)
            if ret == 254:
                ntrial += 1
                continue
        else:
            bash = keepalive % (cmd)
            ret = subprocess.call(bash, shell=True, executable='bash', env = env)
        if ret == 0:
            logging.debug('Thread %d exit with 0')
            return
        else:
            if os.name == 'nt':
                os.exit(-1)
            else:
                raise Exception('Get nonzero return code=%d' % ret)

cmd = ' '.join(args.command) + ' ' + ' '.join(unknown)
#
#  submission script using pyhton multi-threading
#
def mthread_submit(nworker, nserver, envs):
    """
      customized submit script, that submit nslave jobs, each must contain args as parameter
      note this can be a lambda function containing additional parameters in input
      Parameters
         nworker number of slave process to start up
         nserver number of server nodes to start up
         envs enviroment variables to be added to the starting programs
    """
    procs = {}
    for i in range(nworker + nserver):
        if i < nworker:
            role = 'worker'
        else:
            role = 'server'
        procs[i] = Thread(target = exec_cmd, args = (cmd, role, envs))
        procs[i].setDaemon(True)
        procs[i].start()


tracker.config_logger(args)

tracker.submit(args.nworker,
               args.server_nodes,
               fun_submit = mthread_submit,
               pscmd = cmd)
