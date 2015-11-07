#!/usr/bin/env python
"""
DMLC submission script by ssh

One need to make sure all slaves machines are ssh-able.

It first copy the local_dir on the root machine to remote_dir on all slave
machines by rsync. Then run the command by ssh.
"""

import argparse
import sys
import os
import subprocess
import tracker
import logging
from threading import Thread

parser = argparse.ArgumentParser(description='DMLC script to submit dmlc job using ssh')
parser.add_argument('-n', '--nworker', required=True, type=int,
                    help = 'number of worker nodes to be launched')
parser.add_argument('-s', '--nserver', default = 0, type=int,
                    help = 'number of server nodes to be launched')
parser.add_argument('-H', '--hostfile', type=str,
                    help = 'the hostfile of all slave nodes')
parser.add_argument('-R', '--root-dir', type=str, default=os.getcwd()+'/',
                    help = 'the directory at root node will be synced, default\
                    is the current working directory')
parser.add_argument('-L', '--slave-dir', type=str, default='/tmp/dmlc',
                    help = 'the working directory at slave nodes. default is /tmp/dmlc')
parser.add_argument('command', nargs='+',
                    help = 'command for dmlc program')
parser.add_argument('--log-level', default='INFO', type=str,
                    choices=['INFO', 'DEBUG'],
                    help = 'logging level')
parser.add_argument('--log-file', type=str,
                    help = 'output log to the specific log file')

args, unknown = parser.parse_known_args()
cmd = (' '.join(args.command) + ' ' + ' '.join(unknown))

def sync_dir(slave_node=None):
    """sync the working directory from root node into slave node
    """
    local = args.root_dir
    remote = slave_node + ':' + args.slave_dir

    logging.info('rsync %s -> %s', local, remote)

    prog = 'rsync -az --rsh="ssh -o StrictHostKeyChecking=no" %s %s' % (
        local, remote)
    subprocess.check_call([prog], shell = True)

def ssh_submit(nworker, nserver, pass_envs):
    """
      customized submit script, that submit nslave jobs, each must contain args as parameter
      note this can be a lambda function containing additional parameters in input
      Parameters
         nworker number of slave process to start up
         nserver number of server nodes to start up
         pass_envs enviroment variables to be added to the starting programs
    """

    # sync working dir
    if args.hostfile is not None:
        with open(args.hostfile) as f:
            hosts = f.readlines()
        assert len(hosts) > 0
    else:
        hosts = ['localhost']

    hosts = [h.strip() for h in hosts]
    for h in hosts:
        sync_dir(h)

    # run jobs
    def run(prog):
        subprocess.check_call(prog, shell = True)

    def get_env(pass_envs):
        envs = []
        # get system envs
        keys = ['LD_LIBRARY_PATH', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
        for k in keys:
            v = os.getenv(k)
            if v is not None:
                envs.append('export ' + k + '=' + v + ';')
        # get ass_envs
        for k, v in pass_envs.items():
            envs.append('export ' + str(k) + '=' + str(v) + ';')
        return (' '.join(envs))

    for i in range(nworker + nserver):
        if i < nserver:
            pass_envs['DMLC_ROLE'] = 'server'
        else:
            pass_envs['DMLC_ROLE'] = 'worker'

        node = hosts[i % len(hosts)]
        prog = get_env(pass_envs) + ' cd ' + args.slave_dir + '; ' + cmd
        prog = 'ssh -o StrictHostKeyChecking=no ' + node + ' \'' + prog + '\''

        thread = Thread(target = run, args=(prog,))
        thread.setDaemon(True)
        thread.start()

tracker.config_logger(args)

tracker.submit(args.nworker,
               args.nserver,
               fun_submit = ssh_submit,
               pscmd = cmd)
