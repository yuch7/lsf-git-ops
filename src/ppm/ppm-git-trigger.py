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
import sys
import subprocess
import logging
import time
import argparse
import signal


def signal_fun(signum, frame):
    logging.error('Signal <%d> is received, exit.' % signum)
    exit(1)


def init_submit(args):
    #since there's always value for path, repo will take priority
    if args.repo is not None:
        cmd = ['git', 'clone', args.repo]
        ret, out, err = execute(cmd)
        if ret is not 0:
            logging.error('Cannot clone the repo provided, you can clone it manually and set it with --path.')
            sys.exit(-1)
        target=args.repo.split("/")[-1].split(".")[0]
        os.chdir(target)
    else:
        if not os.path.isdir(args.path):
            logging.error('The path specified does not exist.')
            sys.exit(-1)
        os.chdir(args.path)
        cmd = ['git', 'status']
        ret, out, err = execute(cmd)
        if ret is not 0:
            logging.error('The path must be managed by git.')
            sys.exit(-1)
    
    #check whether there's /workflow directory
    if not os.path.isdir("workflow"):
        logging.error('The workflow directory must exist under the path or repo.')
        sys.exit(-1)
    
    operations = set() 
    #submit the existing flows
    for i in os.listdir("workflow"):
        if os.path.isdir("workflow/" + i):
            for j in os.listdir("workflow/" + i):
                (_, extension) = os.path.splitext(j)
                if extension == '.xml':
                    operations.add("workflow/" + i +"/"+j)

    submit_and_trigger_flow(operations, args)


def submit_and_trigger_flow(flows, args):
    for flow in flows:
        if os.path.exists(flow):
            flow_name=flow.split("/")[-1].split(".")[0]
            cmd = ['jsub', '-r', flow]
            ret, out, err = execute(cmd)
            if ret is not 0:
                logging.warning('Failed to submit flow %s.' % flow)
                continue
            else:
                logging.debug('Flow %s submitted.' % flow_name)
            # trigger implies release
            if args.operation == 'release' or args.operation == 'trigger':
                cmd = ['jrelease', flow_name]
                ret, out, err = execute(cmd)
                if ret is not 0:
                    logging.warning('Failed to release flow %s.' % flow_name)
                else:
                    logging.debug('Flow %s released.' % flow_name)
            if args.operation == 'trigger':
                cmd = ['jtrigger', flow_name]
                ret, out, err = execute(cmd)
                if ret is not 0:
                    logging.warning('Failed to trigger flow %s.' % flow_name)
                else:
                    logging.debug('Flow %s triggered.' % flow_name)
        else:
            logging.warning('Flow %s does not exist.' % flow)


def execute(cmd):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()    
    ret = proc.returncode
    out = out.decode('utf8')
    err = err.decode('utf8')
    if ret is not 0:
        logging.error('Failed to run %s, due to %s %s.' %(cmd ,err, out))

    return ret, out, err


def git_manager(args):
    # 1. get current commit id
    cmd = ['git', 'log', '--pretty=format:%H', '-1']
    ret, out, _ = execute(cmd)
    if ret is not 0:
        return

    commit_id = out.split('\n')     
    if len(commit_id) == 0:
        logging.warning('Cannot get current commit id from git log output <%s>.' % out)
        return

    logging.info('Current commint id is <%s>.' % commit_id)
    # 2. pull repo to update the directory
    cmd = ['git', 'pull']
    ret, out, err = execute(cmd)
    if ret is not 0:
        return
    # 3. get operations for changed files
    cmd = ['git', 'diff', '--name-only', commit_id[0], 'HEAD']
    ret, out, err = execute(cmd)
    if ret is not 0:
        return
    
    # 4. get the flow list to be submitted/triggerred 
    files = out.split('\n')     
    if len(files) == 0:
        logging.info('There is no diff comparing with previous git status.')
        return

    logging.info('Updated files are %s.' % files)
    
    operations = set() 
    for file in files:
        if file == '':
            continue
        #only monitor the flow definition changes workflow/flow_dir_name/flowname.xml, not the data/out/status
        if len(file.split("/")) == 3 and file.endswith(".xml"):
            #maybe deleted
            if os.path.exists(file):
                operations.add(file)

    logging.info('Determined flows to be triggerred are %s.' % operations)

    # 5. submitted/triggerred flows
    submit_and_trigger_flow(operations,args)


def main(argv):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(process)d: %(message)s')
    signal.signal(signal.SIGINT, signal_fun)
    signal.signal(signal.SIGHUP, signal_fun)
    signal.signal(signal.SIGTERM, signal_fun)

    js_envdir = os.environ.get('JS_ENVDIR', None)
    if js_envdir is None:
        logging.error('This tool should be run in PM context. Please source your PM profile.')
        sys.exit(-1)

    parser = argparse.ArgumentParser(description='PPM workload triggerred by git.')
    parser.add_argument('-p', '--path', type=str, default=os.getcwd(), help='absolute path with PPM workload and managed by git')
    parser.add_argument('-r', '--repo', type=str, help='repo managed by git that will be cloned to current directory. eg: git@github.com:exmaple/xxx.git')
    parser.add_argument('-o', '--operation', type=str, default='trigger', help='trigger the flow or only release the flow after the repo changed, valid values: release, trigger')
    parser.add_argument('-i', '--interval', type=int, default=5, help='interval in wainting for next pulling')
    args = parser.parse_args()

    if args.operation != 'release' and args.operation != 'trigger':
        logging.error('You specified operation: %s is not supported, only support "release" or "trigger".' % args.operation)
        sys.exit(-1)

    init_submit(args)
  
    while True:
        git_manager(args)
        time.sleep(args.interval)


if __name__ == "__main__":
    main(sys.argv[1:])

