import sys
import argparse
import signal
import logging
from . import projects, utils

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
    parser.add_argument('--dropbox', help="Write results to this dir")
    parser.add_argument('--barcode', help='barcode for dropbox')
    parser.add_argument('--daemon', '-d', help="Daemonize qproject")
    parser.add_argument('--pid-file', help="Path to pidfile")
    parser.add_argument('--user', '-u', help='User name for execution of '
                        'workflow. ACL will be set so this user can access '
                        'input files and write to result and var')
    parser.add_argument('--umask', help="Umask for files in workdir",
                        default=0o077)
    return parser.parse_args()


def validate_args(args):
    pass


def init_signal_handler():
    def handler(sig, frame):
        if sig == signal.SIGTERM:
            logger.warn("Daemon got SIGTERM. Shutting down.")
            raise SystemExit
        else:
            logger.error("Signal handler did not expect to get %s", sig)

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGHUP, handler)


def prepare_command(args):
    workdir = projects.prepare(args.target, force_create=False)
    workflow_dirs = projects.clone_workflows(workdir, args.workflow)
    if args.data:
        projects.copy_data(workdir, args.data, args.user)
    return workflow_dirs


def run_commit(workdir, args):
    projects.run(workdir, args.workflow)
    commit_command(workdir, args)


def run_command(args):
    try:
        workdir = projects.prepare_command(args)
    except BaseException:
        logger.exception("Could not prepare workdir:")
        sys.exit(1)

    if args.daemon:
        utils.daemonize(run_commit, args.pidfile, args.umask, workdir, args)
    else:
        run_commit(workdir, args)


def commit_command(workdir, args):
    projects.commit(workdir, args.dropbox, args.barcode, args.user)


def main():
    args = parse_args()
    validate_args(args)
    if args.command == 'prepare':
        prepare_command(args)
        return

    workdir = projects.prepare(args.target, force_create=False)
    if args.command == 'run':
        run_command(workdir, args)
    elif args.command == 'commit':
        commit_command(workdir, args)

if __name__ == '__main__':
    main()
