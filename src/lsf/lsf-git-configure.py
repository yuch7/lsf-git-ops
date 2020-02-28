#!/usr/bin/env python3

# Copyright International Business Machines Corp, 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import sys
import time
import argparse
import subprocess
import string
import logging
from logging import handlers
import signal

# Rules for lsf.conf parameter based on IBM LSF Knowledge center
# Operations:
#     lim-reconfig: lsadmin reconfig -f
#     res-restart: lsadmin resrestart -f
#     sbd-restart: badmin hrestart -f all
#     mbd-restart: badmin mbdrestart -f
#
# Note:
#     This is not a full rule list.
#     Make sure your concerned parameters has its rule
operation_map = {
        'lsf.conf': {
            'default': set(),
            'LSB_AFS_JOB_SUPPORT': set(['res-restart']),
            'LSB_DEBUG_MBD': set(['mbd-restart']),
            'LSB_DEBUG_SBD': set(['sbd-restart']),
            'LSB_DEBUG_SCH': set(['mbd-reconfig']),
            'LSB_DISABLE_LIMLOCK_EXCL': set(['sbd-restart']),
            'LSB_DISPATCH_CHECK_RESUME_INTVL': set(['mbd-reconfig']),
            'LSB_ENABLE_ESTIMATION': set(['mbd-restart']),
            'LSB_ENABLE_HPC_ALLOCATION': set(['mbd-restart']),
            'LSB_EXCLUDE_HOST_PERIOD': set(['mbd-restart']),
            'LSB_GPU_NEW_SYNTAX': set(['mbd-restart']),
            'LSB_HJOB_PER_SESSION': set(['mbd-restart']),
            'LSB_JOB_CPULIMIT': set(['sbd-restart']),
            'LSB_JOB_MEMLIMIT': set(['sbd-restart']),
            'LSB_JOB_REPORT_MAIL': set(['sbd-restart']),
            'LSB_JOB_TMPDIR': set(['sbd-restart']),
            'LSB_LOCALDIR': set(['mbd-restart', 'sbd-restart']),
            'LSB_LOG_MASK_MBD': set(['mbd-restart']),
            'LSB_LOG_MASK_SBD': set(['sbd-restart']),
            'LSB_LOG_MASK_SCH': set(['mbd-reconfig']),
            'LSB_MAILPROG': set(['sbd-restart']),
            'LSB_MAILSERVER': set(['sbd-restart']),
            'LSB_MAILTO': set(['sbd-restart']),
            'LSB_MAX_FORWARD_PER_SESSION': set(['mbd-reconfig']),
            'LSB_MAX_JOB_DISPATCH_PER_SESSION': set(['mbd-reconfig']),
            'LSB_MAX_PACK_JOBS': set(['mbd-restart']),
            'LSB_MAX_PROBE_SBD': set(['mbd-restart']),
            'LSB_MEMLIMIT_ENFORCE': set(['sbd-restart']),
            'LSB_MEMLIMIT_ENF_CONTROL': set(['sbd-restart']),
            'LSB_MOD_ALL_JOBS': set(['mbd-restart']),
            'LSB_PLAN_KEEP_RESERVE': set(['mbd-restart']),
            'LSB_QUERY_ENH': set(['mbd-restart']),
            'LSB_RC_DEFAULT_HOST_TYPE': set(['mbd-restart']),
            'LSB_RC_EXTERNAL_HOST_FLAG': set(['mbd-restart']),
            'LSB_RC_EXTERNAL_HOST_IDLE_TIME': set(['mbd-restart']),
            'LSB_RC_EXTERNAL_HOST_MAX_TTL': set(['mbd-restart']),
            'LSB_RC_MQTT_ERROR_LIMIT': set(['mbd-restart']),
            'LSB_RC_QUERY_INTERVAL': set(['mbd-restart']),
            'LSB_RC_REQUEUE_BUFFER': set(['mbd-restart']),
            'LSB_RC_TEMPLATE_REQUEST_DELAY': set(['mbd-restart']),
            'LSB_RC_UPDATE_INTERVAL': set(['mbd-restart']),
            'LSB_REQUEUE_TO_BOTTOM': set(['mbd-restart']),
            'LSB_RESOURCE_ENFORCE': set(['mbd-restart']),
            'LSB_SACCT_ONE_UG': set(['mbd-restart']),
            'LSB_SKIP_FULL_HOSTS': set(['mbd-reconfig']),
            'LSB_START_EBROKERD': set(['mbd-restart']),
            'LSB_START_MPS': set(['sbd-restart']),
            'LSB_SUBK_SHOW_EXEC_HOST': set(['sbd-restart']),
            'LSF_DCGM_PORT': set(['sbd-restart', 'res-restart']),
         },
        'lsf.shared': set(['lim-reconfig', 'mbd-restart']),
        'lsf.cluster': set(['lim-restart', 'mbd-restart']),

        'lsb.applications': set(['mbd-reconfig']),
        'lsb.hosts': set(['mbd-reconfig']),
        'lsb.modules': set(['mbd-reconfig']),
        'lsb.queues': set(['mbd-reconfig']),
        'lsb.resources': set(['mbd-reconfig']),
        'lsb.serviceclasses': set(['mbd-reconfig']),
        'lsb.users': set(['mbd-reconfig']),
}


class Logger(object):
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    def __init__(self, filename, level='info', when='D', backCount=3, fmt='%(asctime)s - %(levelname)s - %(process)d: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')
        th.setFormatter(format_str)
        self.logger.addHandler(sh)
        self.logger.addHandler(th)


def signal_fun(signum, frame):
    logging.error('Signal <%d> is received, exit.' % signum)
    exit(1)


def execute(cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    ret = proc.returncode
    return ret, out.decode('utf8'), err.decode('utf8')


def git_manager_shared(shared_envdir, log):
    operations = set()
    # 1. change dir to sub
    os.chdir(shared_envdir)

    # 2. get current commit id
    cmd = ['git', 'log', '--pretty=format:%H', '-1']
    ret, out, err = execute(cmd)
    if ret is not 0:
        logging.error('For shared LSF configuration,executing %s failed.' % cmd)
        return None, operations

    commit_id = out.split('\n')
    if len(commit_id) <= 0:
        logging.warning('For shared LSF configuration,cannot get current commit id from git log output <%s>.' % out)
        return None, operations

    # 3. pull repo to update the directory
    cmd = ['git', 'pull']
    ret, out, err = execute(cmd)
    if ret is not 0:
        return None, operations

    # 4. get operations for changed files
    cmd = ['git', 'diff', '--name-only', commit_id[0], 'HEAD']
    ret, out, err = execute(cmd)
    if ret is not 0:
        return None, operations


    files = out.split('\n')
    if len(files) == 0 or len(files[0]) == 0:
        logging.debug('For shared LSF configuration, there is no diff comparing with previous git status.')
        return None,operations

    message = 'For shared LSF configuration, current commit id is %s and updated files are %s.' %(commit_id[0] , files)
    if log is None:
        logging.info(message)
    else:
        log.logger.info(message)

    for file in files:
        if file == '':
            continue

        name = os.path.basename(file)
        if file.find('lsb.applications'):
            operations = operations.union(operation_map['lsb.applications'])
        elif file.find('lsb.queues'):
             operations = operations.union(operation_map['lsb.queues'])
        elif file.find('lsb.hosts'):
             operations = operations.union(operation_map['lsb.hosts'])
        elif file.find('lsb.resources'):
             operations = operations.union(operation_map['lsb.resources'])
        elif file.find('lsb.users'):
             operations = operations.union(operation_map['lsb.users'])
        elif file.find('lsf.shared'):
             operations = operations.union(operation_map['lsf.shared'])

    message = 'For shared LSF configuration, determined operations are %s.' % operations
    if log is None:
        logging.info(message)
    else:
        log.logger.info(message)

    return commit_id[0], operations


def git_manager_private(lsf_envdir, log):
    operations = set()

    # 0. change dir to lsf_envdir
    os.chdir(lsf_envdir)

    # 1. get current commit id
    cmd = ['git', 'log', '--pretty=format:%H', '-1']
    ret, out, err = execute(cmd)
    if ret is not 0:
        logging.error('Failed executing %s ' % cmd)
        return None, operations

    commit_id = out.split('\n')
    if len(commit_id) <= 0:
        logging.warning('Cannot get current commit id from git log output <%s>.' % out)
        return None, operations

    # 2. pull repo to update the directory
    cmd = ['git', 'pull']
    ret, out, err = execute(cmd)
    if ret is not 0:
        return None, operations

    # 3. get operations for changed files
    cmd = ['git', 'diff', '--name-only', commit_id[0], 'HEAD']
    ret, out, err = execute(cmd)
    if ret is not 0:
        return None, operations

    files = out.split('\n')
    if len(files) == 0 or len(files[0]) == 0:
        logging.debug('There is no diff comparing with previous git status.')
        return None, operations

    message = 'Current commit id is %s and updated files are %s.' %(commit_id[0], files)
    if log is None:
        logging.info(message)
    else:
        log.logger.info(message)

    for file in files:
        if file == '':
            continue

        if file.startswith('lsf.cluster.'):
            file = 'lsf.cluster'

        name = os.path.basename(file)
        if name == 'lsf.conf':
            # for lsf.conf, it needs to be checked parameter based
            cmd = ['git', 'diff', '--unified=0', commit_id[0], 'HEAD']
            ret, out, err = execute(cmd)
            if ret is not 0:
                return None,operations

            lines = out.splitlines()
            for line in lines:
                kv = re.findall(r'^[+-]\w+=', line)
                if len(kv) <= 0:
                    continue
                param = kv[0][1:-1]

                logging.debug('The parameter %s is changed.' % param)

                if param in operation_map[name]:
                    op = operation_map[name][param]
                else:
                    op = operation_map[name]['default']

                operations = operations.union(op)
        else:
            operations = operations.union(operation_map[name])

    message = 'Determined operations are %s.' % operations
    if log is None:
        logging.info(message)
    else:
        log.logger.info(message)

    return commit_id[0],operations


def is_execute_success(log, cmd):
    ret, out, err = execute(cmd)
    if ret is not 0:
        message = 'Failed to run %s, due to %s.' %(cmd, err)
        if log is None:
            logging.error(message)
        else:
            log.logger.error(message)
        return False
    else:
        message = 'Success to run command %s.' % cmd
        if log is None:
            logging.info(message)
        else:
            log.logger.info(message)
        return True


def do_actions(log, operations):
    if 'lim-reconfig' in operations:
        cmd = ['lsadmin', 'reconfig', '-f']
        success = is_execute_success(log, cmd)
        if not success:
            return False

    if 'lim-restart' in operations:
        cmd = ['lsadmin', 'limrestart', '-f', 'all']
        success = is_execute_success(log, cmd)
        if not success:
            return False

    if 'res-restart' in operations:
        cmd = ['lsadmin', 'resrestart', '-f', 'all']
        success = is_execute_success(log, cmd)
        if not success:
            return False

    if 'sbd-restart' in operations:
        cmd = ['badmin', 'hrestart', '-f', 'all']
        success = is_execute_success(log, cmd)
        if not success:
            return False

    if 'mbd-restart' in operations:
        cmd = ['badmin', 'mbdrestart', '-f']
        success = is_execute_success(log, cmd)
        if not success:
            return False

    if 'mbd-reconfig' in operations:
        cmd = ['badmin', 'reconfig', '-f']
        success = is_execute_success(log, cmd)
        if not success:
            return False

    return True

def commit_git_log(lsf_envdir, private_commit_id, shared_commit_id):
    # 1. change dir to lsf_envdir
    os.chdir(lsf_envdir)

    # 2. push the changes for git.configuration.*  to private repo
    cmd = ['git', 'add', 'git-configuration.log*']
    ret, out, err = execute(cmd)
    if ret is not 0:
        logging.error(err)
        return

    if shared_commit_id and private_commit_id:
        comments = 'Update git configuration log based on commit id %s for shared LSF configuration updated and %s for private LSF configuration updated.' %(shared_commit_id, private_commit_id)
    elif private_commit_id:
        comments = 'Update git configuration log based on commit id %s for private LSF configuration updated.' %(private_commit_id)
    else:
        comments = 'Update git configuration log based on commit id %s for shared LSF configuration updated.' %(shared_commit_id)

    cmd = ['git', 'commit', '-m', comments]
    ret, out, err = execute(cmd)
    if ret is not 0:
        logging.error(err)
        return

    cmd = ['git', 'push']
    ret, out, err = execute(cmd)
    if ret is not 0:
        logging.error(err)
        return


def main(argv):
    # the logger is used to print basic output
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(process)d: %(message)s')
    signal.signal(signal.SIGINT, signal_fun)
    signal.signal(signal.SIGHUP, signal_fun)
    signal.signal(signal.SIGTERM, signal_fun)

    parser = argparse.ArgumentParser(description='LSF configuration management by git.')
    parser.add_argument('-d', '--shared_envdir', type=str, default=None, help='set to the full path of shared LSF configuration')
    parser.add_argument('-i', '--interval', type=int, default=5, help='interval in wainting for next pulling')
    parser.add_argument('-n', '--notify', action="store_true", help='push LSF operation log into LSF configuration git repository')
    args = parser.parse_args()

    if args.shared_envdir:
        if not os.path.exists(args.shared_envdir):
            logging.error('No such file or directory: %s.' % args.shared_envdir )
            sys.exit(-1)

    lsf_envdir = os.environ.get('LSF_ENVDIR', None)
    if lsf_envdir is None:
        logging.error('This tool should be run in LSF context. Please source your LSF profile.')
        sys.exit(-1)

    # the logger is used to push LSF operation back to LSF configuration git repository 
    if args.notify:
        log = Logger(lsf_envdir +'/git-configuration.log', level='debug')
    else:
        log = None

    operations = set()
    while True:
        # must run git_manager_private firstly, as we will update git.log to private repo
        private_commit_id, private_operations = git_manager_private(lsf_envdir, log)
        shared_commit_id = None
        shared_operations = set()
        if args.shared_envdir:
            shared_commit_id,shared_operations = git_manager_shared(args.shared_envdir, log)
        operations = private_operations | shared_operations

        # There is no file changed in LSF git configuration for both private repo and shared repo, skip
        if len(operations) == 0:
            logging.debug('No operation needs to be executed. Just continue...')

        else :
            do_actions(log,operations)
            if args.notify:
                commit_git_log(lsf_envdir, private_commit_id , shared_commit_id)
            operations.clear()

        time.sleep(args.interval)


if __name__ == "__main__":
    main(sys.argv[1:])

