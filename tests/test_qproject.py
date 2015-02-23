import tempfile
import qproject
from unittest import mock
import os
import pwd
import pytest


def touch(file):
    with open(file, 'w'):
        pass


def test_prepare():
    with tempfile.TemporaryDirectory() as tmp:
        name = 'QTEST'
        target = os.path.join(tmp, name)
        workdir = qproject.prepare(target)
        assert os.path.exists(target)
        for dir in ['src', 'var', 'data', 'result']:
            assert os.path.exists(os.path.join(target, dir))
        assert workdir.base == target
        assert workdir.src == os.path.join(target, 'src')


@mock.patch('subprocess.check_call')
def test_clone_workflows(check_call):
    with tempfile.TemporaryDirectory() as tmp:
        name = 'QTEST'
        target = os.path.join(tmp, name)
        workdir = qproject.prepare(target)
        remote = 'github:qbicsoftware/qcprot'
        dirs = qproject.clone_workflows(
            workdir, [remote], {remote: "HEAD"}
        )
        check_call.assert_any_call(
            ['git', 'clone', 'https://github.com/qbicsoftware/qcprot',
             os.path.join(workdir.src, 'qcprot')]
        )
        check_call.assert_any_call(
            [
                'git',
                '--work-tree', dirs[remote],
                '--git-dir', os.path.join(dirs[remote], '.git'),
                'checkout',
                'HEAD',
            ]
        )


@mock.patch('subprocess.Popen')
@mock.patch('subprocess.check_call')
def test_run(Popen, check_call):
    with tempfile.TemporaryDirectory() as tmp:
        name = "QTEST"
        workdir = qproject.prepare(os.path.join(tmp, name))
        with pytest.raises(ValueError):
            qproject.run(workdir, ['foo'])
        workflow = os.path.join(workdir.src, 'foo')
        os.mkdir(workflow)
        with pytest.raises(ValueError):
            qproject.run(workdir, ['foo'])


def test_commit():
    with tempfile.TemporaryDirectory() as tmp:
        name = "QTEST"
        user = pwd.getpwuid(os.getuid()).pw_name
        workdir = qproject.prepare(os.path.join(tmp, name), user=user)
        touch(os.path.join(workdir.result, 'result'))
        os.mkdir(os.path.join(workdir.result, 'dir'))
        touch(os.path.join(workdir.result, 'dir', 'res'))
        qproject.commit(workdir, tmp, "123", user)
        os.symlink('/etc/bash.bashrc', os.path.join(workdir.result, 'evil'))
        with pytest.raises(ValueError):
            qproject.commit(workdir, tmp, "124", user)
