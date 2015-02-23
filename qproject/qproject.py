#!/usr/bin/env python
from __future__ import print_function

import argparse
import collections
import os
import subprocess
import re
import shutil
import logging
import signal
import sys

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Download data from OpenBIS and prepare working directory'
    )

    parser.add_argument('command', choices=['prepare', 'run', 'commit'])
    parser.add_argument('target', help='Base directory where the files should '
                        'be stored')
    parser.add_argument('--workflow', '-w', nargs='+',
                        help='Checkout a workflow from this git repository')
    parser.add_argument('--commit', '-c', nargs='+',
                        help="Commits of the workflows.")
    parser.add_argument('--params', '-p', nargs='+',
                        help='Parameter file for each specified workflow')
    parser.add_argument('--data', help='Input files to copy to workdir',
                        nargs='+')
    parser.add_argument('--jobid', help="A jobid at a workflow server. "
                        "Status update will be sent to this server")
    parser.add_argument('--server-file', help="Path to a file that contains "
                        "the address of a workflow server and a password. "
                        "Requires jobid.")
    parser.add_argument('--daemon', '-d', help="Daemonize qproject")
    parser.add_argument('--pid-file', help="Path to pidfile")
    parser.add_argument('--user', '-u', help='User name')
    args = parser.parse_args()
    print(args)
    sys.exit(0)


def prepare(target, force_create=True):
    """ Prepare directory structure for a qbic workflow.

    Parameters
    ----------
    target: path
        Path to the directory where the workflow will be executed. The parent
        directory must already exist.
    force_create: bool
        Whether to assume that the target workdir exists. If `False`, this
        function will do nothing but return the existing workdir.

    Return `namedtuple` with the following fields:

    base: path
        Same as target.
    data: path
        Directory for input data. The workflow should not write to this
        directory.
    src: path
        The source code of the workflow should be stored here. It must either
        contain a script called `run` that reads a parameter file `config.json`
        in `src` or it must contain one or more subdirectories like that.
    var: path
        Workflows should store intermediate results here.
    results: path
        Workflows should write their results to this directory.

    """
    Workdir = collections.namedtuple(
        'Workdir', ['base', 'data', 'src', 'var', 'result', 'run']
    )
    workdir = Workdir(*(
        [target] + [os.path.join(target, f) for f in Workdir._fields[1:]]
    ))

    if os.path.exists(target) and force_create:
        raise ValueError('Target directory exists.')
    elif not os.path.exists(target):
        for dir in workdir:
            os.mkdir(dir, 0o770)
    else:
        for dir in workdir:
            assert os.path.isdir(dir)
    return workdir


def clone_workflows(workdir, workflows, commits=None, require_signature=False):
    """ Clone git repositories to the workflow/src.

    Parameters
    ----------
    workdir: namedtuple
        As returned by prepare
    workflows: list
        A list of git repositories. They will be cloned inside the src directory
        of the workdir. Each entry can be anything `git clone` will recognize
        or a string like `github:qbicsoftware/qcprot`. Existing workflows will
        be ignored.
    commits: dict
        Map from workflow to commit hash or tag that should be checked out.
        If no commit is specified it defaults to HEAD.
    require_signature: bool
        If True, check signatures of commit tags
    """
    if require_signature:  # TODO
        raise NotImplemented

    if commits is None:
        commits = {}

    if workflows is None:
        workflows = []

    def clone(remote, target, commit=None):
        if remote.startswith('github:'):
            remote = 'https://github.com/%s' % remote[len('github:'):]
        if os.path.exists(target):
            logger.debug('Workflow %s exists. Skipping cloning' % target)
        subprocess.check_call(['git', 'clone', remote, target])
        if commit is not None:
            subprocess.check_call(
                [
                    'git',
                    '--work-tree', target,
                    '--git-dir', os.path.join(target, '.git'),
                    'checkout',
                    commit
                ]
            )

    workflow_dirs = {}
    for workflow in workflows:
        target = os.path.join(workdir.src, workflow.split('/')[-1])
        workflow_dirs[workflow] = target
        assert re.match("^[_a-z-Z0-9]*$", target.split('/')[-1])
        clone(workflow, target, commits.get(workflow, None))

    return workflow_dirs


def config(workdir, param_files):
    """ Copy parameter files to workflow directories.

    param_files: dict
        Keys are workflows are paths to workflow directories, values are paths
        to parameter files.
    """
    for workflow, params in param_files.items():
        shutil.copy(params, workflow)


def copy_data(workdir, address, barcodes=None, barcode_csv=None, user=None):
    pass


def commit(workdir, address, project, user=None, barcode=None):
    pass


def forward_status(socket_path, workflow_server, stop_signal):
    while not stop_signal.is_set():
        pass


def run(workdir, workflows=None, user=None):
    """ Execute workflows in the specified order.

    worklfows: list
        List of workflow directories. Specify if only a subset of available
        workflows should be executed or if the order is important.
    """
    if user:
        sudo = ['sudo', '-u', user]
    else:
        sudo = []

    if workflows is None:
        workflows = [dir for dir in os.listdir(workdir.src)
                     if os.path.isdir(dir)]
    else:
        workflows = [os.path.join(workdir.src, name) for name in workflows]

    for workflow_dir in workflows:
        if not os.path.exists(workflow_dir):
            raise ValueError("Workflow dir does not exist: %s" % workflow_dir)
        executable = os.path.join(workflow_dir, 'run')
        if not os.path.exists(executable):
            logger.warn('Trying to start workflow %s, but could not find '
                        'executable %s', workflow_dir, executable)
            raise ValueError("File not found: %s" % executable)
        process = subprocess.Popen(sudo + [executable])

        got_sigterm = []

        def sigterm_handler(signum, frame):
            got_sigterm.append(1)

        signal.signal(signal.SIGTERM, sigterm_handler)
        process.wait()
        if got_sigterm:
            logger.info("Got SIGTERM. killing workflow process.")
            try:
                subprocess.check_call(sudo + ["kill", str(process.pid)])
            except Exception:
                logger.warn("Could not send SIGTERM to workflow process.")
            finally:
                raise SystemExit()

        if process.returncode:
            logger.warn('Workflow %s returned non-zero returncode %s',
                        executable, process.returncode)
            raise RuntimeError("non-zero exit code in %s" % executable)
        else:
            logger.info('Successfully executed workflow %s', executable)


def error(message, retcode=1):
    logger.error(message)
    exit(retcode)


def prepare_command(args):
    workdir = prepare(args.target, force_create=False)
    workflow_dirs = clone_workflows(workdir, args.workflow)
    if args.data or args.data_csv:
        copy_data(workdir, args.address, args.data, args.data_csv, args.user)
    return workflow_dirs


def run_command(args):
    workdir = prepare_command(args)
    run(workdir, args.workflow)


def commit_command(args):
    pass


def main():
    args = parse_args()
    exists = os.path.exists(args.target)
    if args.command == 'prepare':
        prepare_command(args)
    elif args.command == 'run':
        run_command(args)
    elif args.command == 'commit':
        commit_command(args)

if __name__ == '__main__':
    args = parse_args()
