import atexit
import logging
import os
import re
import subprocess
import sys
import shutil

logger = logging.getLogger(__name__)

USER_REGEX = "^[a-zA-Z0-9]*$"


def add_acl(file, permissions, user=None, group=None):
    if user:
        logger.debug("Add acl %s for %s to file %s", permissions, user, file)
    if group:
        logger.debug("Add acl %s for %s to file %s", permissions, group, file)
    if user and re.match(USER_REGEX, user) is None:
        logger.critical("Tried to set acl for invalid user name %s." % user)
        raise ValueError("Invalid user name: %s", user)
    if group and re.match(USER_REGEX, group) is None:
        logger.critical("Tried to set acl for invalid group name %s." % user)
        raise ValueError("Invalid group name: %s", user)
    if re.match("[rwx]*", permissions) is None:
        raise ValueError("Invalid acl permissions %s" % permissions)
    try:
        if user:
            arg = 'u:%s:%s' % (user, permissions)
            subprocess.check_call(['setfacl', '-m', arg, file])
        if group:
            arg = 'g:%s:%s' % (group, permissions)
            subprocess.check_call(['setfacl', '-m', arg, file])
    except subprocess.CalledProcessError:
        logger.exception("Could not set acl for directory %s. This will "
                         "probably lead to failures later" % file)


def clone(remote, target, commit=None):
    if remote.startswith('github:'):
        remote = 'https://github.com/%s' % remote[len('github:'):]
    old_mask = os.umask(0)
    try:
        if os.path.exists(target) and os.listdir(target):
            raise ValueError("Target repository exists: %s" % target)
        logger.info("Cloning %s to %s", remote, target)
        subprocess.check_call(['git', 'clone', remote, target])
        if commit is not None:
            print(target)
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


def write_zip(dirs, dest):
    """ Zip the dirs and write them to a zip archive `dest`. """
    subprocess.check_output(
        ['zip', '-r', dest] + dirs
    )


def copytree_owner(src, dest, userid):
    """
    Copy the contents of a directory but ignore files not owned by `userid`.
    """
    src = os.path.abspath(src)
    dest = os.path.abspath(dest)

    for root, dirs, files, rootfd in os.fwalk(src):
        assert root.startswith(src)
        local_dest = root[len(src) + 1:]
        local_dest = os.path.join(dest, local_dest)

        root_owner = os.fstat(rootfd).st_uid
        if root != src and root_owner != userid:
            logger.critical("Found dir with invalid owner. %s should be "
                            "owned by %s but is owned by %s. Can not write "
                            "to dropbox", root, userid, root_owner)
            continue

        for file in files:
            def opener(f, flags):
                return os.open(f, flags, dir_fd=rootfd)
            with open(file, 'rb', opener=opener) as fsrc:
                owner = os.fstat(fsrc.fileno()).st_uid
                if userid is not None and owner != userid:
                    logger.critical("Found file with invalid owner. %s should "
                                    "be owned by %s but is owned by %s. Can "
                                    "not write to dropbox",
                                    os.path.join(root, file), userid, owner)
                    continue
                with open(os.path.join(local_dest, file), 'wb') as fdst:
                    shutil.copyfileobj(fsrc, fdst)

        for dir in dirs:
            os.mkdir(os.path.join(local_dest, dir), 0o700)


def daemonize(func, pidfile, umask, *args, **kwargs):
    """ Run ``func`` in new process independent from this one.

    Write the pid of the new daemon to pidfile.
    """
    logger.info("Starting new daemon")
    try:
        pid = os.fork()
    except OSError:
        logger.critical("Fork failed.")
        sys.exit(1)
    if pid:
        os._exit(0)

    # new process group
    os.setsid()

    try:
        pid = os.fork()
    except OSError:
        logger.error("Fork failed.")
        sys.exit(1)

    if pid:
        os._exit(0)

    logger.info("PID of new daemon: %s", os.getpid())

    os.umask(umask)
    write_pidfile(pidfile)
    close_open_fds()
    try:
        func(*args, **kwargs)
    except Exception:
        logger.critical("Unexpected error. Daemon is stopping")
        logger.exception("Error was:")


def write_pidfile(pidfile):
    try:
        with open(pidfile, 'xt') as f:
            f.write(str(os.getpid()) + '\n')

        def remove_pid():
            try:
                os.remove(pidfile)
            except OSError:
                pass
        atexit.register(remove_pid)
    except OSError:
        logger.critical("Could not write pidfile %s. Is the daemon running?",
                        pidfile)
        raise


def close_open_fds():
    # use devnull for std file descriptors
    devnull = os.open('/dev/null', os.O_RDWR)
    for i in range(3):
        os.dup2(devnull, i)
