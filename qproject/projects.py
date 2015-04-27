from __future__ import print_function

import collections
import os
import subprocess
import re
import shutil
import logging
import pwd
import json
import time
from . import utils

logger = logging.getLogger(__name__)

Workspace = collections.namedtuple(
    'Workspace',
    ['base', 'data', 'ref', 'src', 'var', 'result', 'run', 'etc',
     'logs', 'archive', 'usr']
)


class Workflow(object):
    """ Represent a qsnake workflow.

    Parameters
    ----------
    workspace: namedtuple Workspace
        As returned by prepare.
    name: str
        The name of the workflow. This is used as subdirectory below 'src'.
    remote: str, optional
        A github repository that contains the source of the workflow.
        It can be anything `git clone` will recognize or a string like
        `github:qbicsoftware/qcprot`.
    commit: dict, optional
        A commit hash or tag that should be checked out.  If no commit is
        specified it defaults to HEAD.
    params: dict, optional
        Parameters for the workflow. They are written to the config file in
        `write_config`.

    Attributes
    ----------
    dirs: `namedtuple` with the following fields:
        base: path
            Same as root.
        data: path
            Directory for input data. The workflow should not write to this
            directory.
        ref: path
            Directory for third party data like reference genomes.
        src: path
            The source code of the workflow should be stored here. It must
            either contain a script called `run` that reads a parameter file
            `config.json` in `src`.
        var: path
            Workflows should store intermediate results here.
        logs: path
            Directory for log files.
        run: path
            Data about the status of the workflows is stored here.
        result: path
            Workflows should write their results to this directory.
    """
    def __init__(self, root, name=None, remote=None, commit=None, params=None):
        if name is None:
            if remote is not None:
                name = remote.split('/')[-1]

        if name and re.match("^[_a-zA-Z0-9\-]+$", name) is None:
            raise ValueError("Invalid workflow name: %s" % name)

        dirs = tuple(os.path.join(root, name) for name in Workspace._fields[1:])
        self.dirs = Workspace(*((root, ) + dirs))
        self.name = name
        self.remote = remote
        self.git_commit = commit
        self.params = params

    def create(self, mode=0o777, user=None, group=None):
        """ Create all workflow directories specified in `self.dirs`. """
        for directory in self.dirs:
            if not os.path.exists(directory):
                os.mkdir(directory, mode)
                utils.add_acl(directory, 'rwx', user, group)

    def write_config(self, user=None, group=None):
        """ Write a config file to src containing paths and parameters.

        Do not call this method before `clone`, or git will complain
        about an existing non-empty directory.
        """
        config = self.dirs._asdict()
        for key in config:
            config[key] = os.path.abspath(config[key])
        if self.params is not None:
            if self.params is not None:
                config['params'] = self.params
            else:
                config['params'] = {}

        path = os.path.join(self.dirs.src, 'config.json')
        logger.debug("Writing config file %s" % path)
        with open(path, 'w') as f:
            json.dump(config, f, indent=4)
        utils.add_acl(path, 'r', user, group)

    def clone(self):
        """ Clone the remote repository to `self.workdir.src`. """
        if not self.remote:
            raise ValueError("Remote is not set")
        utils.clone(self.remote, self.dirs.src, commit=self.git_commit)

    def _check_runnable(self):
        """ Check if all directories and the config file exist. """
        for directory in self.dirs:
            if not os.path.isdir(directory):
                raise ValueError("Could not find directory %s" % directory)
        config = os.path.join(self.dirs.src, 'config.json')
        if not os.path.exists(config):
            logger.warn("Config file is missing: %s" % config)

    def run(self, user=None):
        """ Execute the workflow as `user` and return a Popen. """

        logger.info("Executing workflow.")
        self._check_runnable()

        executable = os.path.join(self.dirs.src, 'run')
        if not os.path.exists(executable):
            raise ValueError('Trying to start workflow %s, but could not find '
                             'executable %s' % (self.name, executable))

        args = ['./run']
        if user:
            args.append(user)

        process = subprocess.Popen(args, cwd=self.dirs.src)

        return process

    def commit(self, dropbox, user=None, umask=None):
        """ Copy results and logs to a dropbox.

        If `user` is specified, copy only files that are owned by `user`.
        """
        userid = pwd.getpwnam(user).pw_uid if user else None
        if umask:
            old_umask = os.umask(umask)
        try:
            dropbox_result = os.path.join(dropbox, 'result')
            dropbox_logs = os.path.join(dropbox, 'logs')
            if not os.path.exists(dropbox_result):
                os.makedirs(dropbox_result)
            if not os.path.exists(dropbox_logs):
                os.makedirs(dropbox_logs)
            utils.copytree_owner(self.dirs.result, dropbox_result, userid)
            utils.copytree_owner(self.dirs.logs, dropbox_logs, userid)
        finally:
            if umask:
                os.umask(old_umask)

    def abort(self):
        raise NotImplementedError()

    def describe_state(self):
        # TODO
        return {}

    def archive_result(self):
        """ Write a zip file to archive that contains src, log and etc.

        It also contains a file `meta.json` with some information like
        origin of github repository and checksums of input and output files.
        """
        time_str = time.strftime("%Y-%m-%dT%H%M%S-%Z")
        dest = os.path.join(self.dirs.archive, time_str) + ".zip"
        for _ in range(10):
            if not os.path.exists(dest):
                break
            else:
                time.sleep(.5)
                time_str = time.strftime("%Y-%m-%dT%H%M%S-%Z")
                dest = os.path.join(self.dirs.archive, time_str) + ".zip"
        else:
            raise ValueError("Could not write archive.")

        logger.info("Writing archive to %s" % dest)
        meta = self.describe_state()
        meta_file = os.path.join(self.dirs.base, 'meta.json')
        with open(meta_file, 'w') as f:
            json.dump(meta, f)
        data = [self.dirs.src, self.dirs.etc, self.dirs.logs, meta_file]
        utils.write_zip(data, dest)
        return dest


def copy_data(target, data, user=None, group=None, permissions='r'):
    """ Copy a list of data files to `workspace.data`.

    If `user` or `group` is specified set an acl for this group or user.
    """
    logger.info("Copying data files to %s" % target)
    for path in data:
        base, name = os.path.split(path)
        dest = os.path.join(target, name)
        logger.debug("Copying %s to %s", path, dest)
        shutil.copyfile(path, dest)
        utils.add_acl(dest, permissions, user, group)
