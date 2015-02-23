import atexit
import logging
import os
import re
import subprocess
import sys

logger = logging.getLogger(__name__)

USER_REGEX = "^[a-zA-Z0-9]*$"


def add_acl(file, user, permissions, group=None):
    logger.debug("Add acl %s for %s to file %s", permissions, user, file)
    if re.match(USER_REGEX, user) is None:
        logger.critical("Tried to set acl for invalid user name %s." % user)
        raise ValueError("Invalid user name: %s", user)
    if group and re.match(USER_REGEX, group) is None:
        logger.critical("Tried to set acl for invalid user name %s." % user)
        raise ValueError("Invalid user name: %s", user)
    if re.match("[rwx]*", permissions) is None:
        logger.critical("Tried to set invalid acl permissions %s" % permissions)
        raise ValueError("Invalid acl permissions %s" % permissions)
    try:
        arg = 'u:%s:%s' % (user, permissions)
        subprocess.check_call(['setfacl', '-m', arg, file])
        if group is not None:
            arg = 'g:%s:%s' % (user, permissions)
            subprocess.check_call(['setfacl', '-m', arg, file])
    except subprocess.CalledProcessError:
        logger.error("Could not set acl for directory %s. This will probably "
                     "leed to failures later" % file)


def daemonize(func, pidfile, umask, *args, **kwargs):
    """ Run ``func`` in new process independent from this one.

    Write the pid of the new daemon to pidfile.
    """
    logger.info("Starting new daemon")
    os.chdir('/')
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
        os.dup2(devnull, 0)
