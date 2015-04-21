import tempfile
from qproject import projects
try:
    from unittest import mock
except ImportError:
    import mock
import os
import pwd
import pytest
import shutil


def touch(file):
    with open(file, 'w'):
        pass


@mock.patch('subprocess.check_call')
def test_clone_workflows(check_call):
    tmp = tempfile.mkdtemp()
    try:
        name = 'QTEST'
        target = os.path.join(tmp, name)
        remote = 'github:qbicsoftware/qcprot'
        workflow = projects.Workflow(target, remote=remote, commit='HEAD')
        workflow.clone()
        check_call.assert_any_call(
            ['git', 'clone', 'https://github.com/qbicsoftware/qcprot',
             workflow.dirs.src]
        )
        check_call.assert_any_call(
            [
                'git',
                '--work-tree', workflow.dirs.src,
                '--git-dir', os.path.join(workflow.dirs.src, '.git'),
                'checkout',
                'HEAD',
            ]
        )
    finally:
        shutil.rmtree(tmp)


@mock.patch('subprocess.Popen')
@mock.patch('subprocess.check_call')
def test_run(Popen, check_call):
    tmp = tempfile.mkdtemp()
    try:
        name = "QTEST"
        workflow = projects.Workflow(os.path.join(tmp, name), name=name)
        with pytest.raises(ValueError):
            workflow.run()
    finally:
        shutil.rmtree(tmp)


def test_commit():
    tmp = tempfile.mkdtemp()
    try:
        name = "QTEST"
        user = pwd.getpwuid(os.getuid()).pw_name
        workdir = os.path.join(tmp, name)
        workflow = projects.Workflow(workdir, name=name)
        workflow.create(user=user)
        dirs = workflow.dirs
        touch(os.path.join(dirs.result, 'result'))
        os.mkdir(os.path.join(dirs.result, 'dir'))
        touch(os.path.join(dirs.result, 'dir', 'res'))
        workflow.commit(tmp, user=user)
        os.symlink('/etc/bash.bashrc', os.path.join(dirs.result, 'evil'))
        shutil.rmtree(os.path.join(tmp, 'result'))
        shutil.rmtree(os.path.join(tmp, 'logs'))
        workflow.commit(tmp, user)
        assert not os.path.isdir(os.path.join(tmp, 'result', 'evil'))
    finally:
        shutil.rmtree(tmp)
