#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
DMLC submission script, MPI version
"""
import argparse
import sys
import os
import subprocess
import tracker
from threading import Thread

parser = argparse.ArgumentParser(description='DMLC script to submit dmlc job using MPI')
parser.add_argument('-n', '--nworker', required=True, type=int,
                    help = 'number of worker proccess to be launched')
parser.add_argument('-s', '--server-nodes', default = 0, type=int,
                    help = 'number of server nodes to be launched')
parser.add_argument('--log-level', default='INFO', type=str,
                    choices=['INFO', 'DEBUG'],
                    help = 'logging level')
parser.add_argument('--log-file', type=str,
                    help = 'output log to the specific log file')
parser.add_argument('-H', '--hostfile', type=str,
                    help = 'the hostfile of mpi server')
parser.add_argument('command', nargs='+',
                    help = 'command for dmlc program')
parser.add_argument('--host-ip', type=str,
                    help = 'the scheduler ip', default='ip')
args, unknown = parser.parse_known_args()
#
# submission script using MPI
#

def get_mpi_env(envs):
    """get the mpirun command for setting the envornment

    support both openmpi and mpich2
    """
    outfile="/tmp/mpiver"
    os.system("mpirun 1>/tmp/mpiver 2>/tmp/mpiver")
    with open (outfile, "r") as infile:
        mpi_ver = infile.read()
    cmd = ''
    if 'Open MPI' in mpi_ver:
        for k, v in envs.items():
            cmd += ' -x %s=%s' % (k, str(v))
    elif 'mpich' in mpi_ver:
        for k, v in envs.items():
            cmd += ' -env %s %s' % (k, str(v))
    else:
        raise Exception('unknow mpi version %s' % (mpi_ver))

    return cmd

def mpi_submit(nworker, nserver, pass_envs):
    """
      customized submit script, that submit nslave jobs, each must contain args as parameter
      note this can be a lambda function containing additional parameters in input
      Parameters
         nworker number of slave process to start up
         nserver number of server nodes to start up
         pass_envs enviroment variables to be added to the starting programs
    """
    def run(prog):
        """"""
        subprocess.check_call(prog, shell = True)

    cmd = ''
    if args.hostfile is not None:
        cmd = '--hostfile %s' % (args.hostfile)
    cmd += ' ' + ' '.join(args.command) + ' ' + ' '.join(unknown)

    # start servers
    if nserver > 0:
        pass_envs['DMLC_ROLE'] = 'server'
        prog = 'mpirun -n %d %s %s' % (nserver, get_mpi_env(pass_envs), cmd)
        thread = Thread(target = run, args=(prog,))
        thread.setDaemon(True)
        thread.start()

    if nworker > 0:
        pass_envs['DMLC_ROLE'] = 'worker'
        prog = 'mpirun -n %d %s %s' % (nworker, get_mpi_env(pass_envs), cmd)
        thread = Thread(target = run, args=(prog,))
        thread.setDaemon(True)
        thread.start()

tracker.config_logger(args)

tracker.submit(args.nworker, args.server_nodes, fun_submit = mpi_submit,
               hostIP=args.host_ip,
               pscmd=(' '.join(args.command) + ' ' + ' '.join(unknown)))
