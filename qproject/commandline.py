import sys
import argparse
import signal
import logging
from . import qproject

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
    workdir = qproject.prepare(args.target, force_create=False)
    workflow_dirs = qproject.clone_workflows(workdir, args.workflow)
    if args.data or args.data_csv:
        qproject.copy_data(workdir, args.address, args.data,
                           args.data_csv, args.user)
    return workflow_dirs


def run_command(args):
    workdir = qproject.prepare_command(args)
    qproject.run(workdir, args.workflow)


def commit_command(args):
    pass


def main():
    args = parse_args()
    if args.command == 'prepare':
        prepare_command(args)
    elif args.command == 'run':
        run_command(args)
    elif args.command == 'commit':
        commit_command(args)

if __name__ == '__main__':
    args = parse_args()
