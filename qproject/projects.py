#!/usr/bin/env python
from __future__ import print_function

import collections
import os
import subprocess
import re
import shutil
import logging
import signal
import pwd
from . import utils

logger = logging.getLogger(__name__)


def prepare(target, force_create=True, user=None, group=None):
    """ Prepare directory structure for a qbic workflow.

    Parameters
    ----------
    target: path
        Path to the directory where the workflow will be executed. The parent
        directory must already exist.
    force_create: bool
        Whether to assume that the target workdir exists. If `False`, this
        function will do nothing but return the existing workdir.
    user: str, optional
        Make all directories accessable by user by acl.

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
        for directory in workdir:
            os.mkdir(directory, 0o700)
            if user is not None:
                utils.add_acl(directory, user, 'rwx', group=group)
    else:
        for directory in workdir:
            assert os.path.isdir(directory)
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
        old_mask = os.umask(0)
        try:
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
        finally:
            os.umask(old_mask)

    workflow_dirs = {}
    for workflow in workflows:
        target = os.path.join(workdir.src, workflow.split('/')[-1])
        workflow_dirs[workflow] = target
        assert re.match("^[_a-z-Z0-9]*$", target.split('/')[-1])
        clone(workflow, target, commits.get(workflow, None))

    return workflow_dirs


def config(workdir, param_files, user=None):
    """ Copy parameter files to workflow directories.

    param_files: dict
        Keys are workflows are paths to workflow directories, values are paths
        to parameter files.
    """
    for workflow, params in param_files.items():
        dest = os.path.join(workdir.src, 'config.json')
        shutil.copy(params, dest)
        if user:
            utils.add_acl(dest, user, 'r')


def copy_data(workdir, data, user=None):
    logger.info("Copying data files to %s" % workdir.data)
    for path in data:
        base, name = os.path.split(path)
        dest = os.path.join(workdir.data, name)
        logger.debug("Copying %s to %s", path, dest)
        shutil.copyfile(path, dest)
        if user:
            utils.add_acl(dest, user, 'r')


def commit(workdir, dropbox, barcode, user):
    dest = os.path.join(dropbox, barcode)
    try:
        userid = pwd.getpwnam(user).pw_uid
    except KeyError:
        logger.error("Could not find user %s" % user)
        raise
    try:
        os.mkdir(dest, 0o700)
    except OSError:
        logger.critical("Could not create dropbox directory %s" % dest)
        raise

    # like shutil.copytree, but make sure we only copy files owned by user
    for root, dirs, files, rootfd in os.fwalk(workdir.result):
        assert root.startswith(workdir.result)
        local_dest = root[len(workdir.result) + 1:]
        local_dest = os.path.join(dest, local_dest)

        root_owner = os.fstat(rootfd).st_uid
        if root != workdir.result and root_owner != userid:
            logger.critical("Found dir with invalid owner. %s should be "
                            "owned by %s but is owned by %s. Can not write "
                            "results to dropbox", root, user, root_owner)
            raise ValueError

        for file in files:
            def opener(f, flags):
                return os.open(f, flags, dir_fd=rootfd)
            with open(file, 'rb', opener=opener) as fsrc:
                owner = os.fstat(fsrc.fileno()).st_uid
                if owner != userid:
                    logger.critical("Found file with invalid owner. %s should "
                                    "be owned by %s but is owned by %s. Can "
                                    "not write results to dropbox",
                                    os.path.join(root, file), user, owner)
                    raise ValueError
                with open(os.path.join(local_dest, file), 'wb') as fdst:
                    shutil.copyfileobj(fsrc, fdst)

        for dir in dirs:
            os.mkdir(os.path.join(local_dest, dir), 0o700)


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
