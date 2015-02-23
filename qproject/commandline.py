import sys
import argparse
import logging
import os
from . import projects, utils


handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)20s - %(levelname)s - %(message)s'
)

handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

for module in ['qproject.utils', 'qproject.projects']:
    module_logger = logging.getLogger(module)
    module_logger.addHandler(handler)


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
    parser.add_argument('--daemon', '-d', help="Daemonize qproject",
                        action="store_true", default=False)
    parser.add_argument('--pidfile', help="Path to pidfile")
    parser.add_argument('--user', '-u', help='User name for execution of '
                        'workflow. ACL will be set so this user can access '
                        'input files and write to result and var')
    parser.add_argument('--umask', help="Umask for files in workdir",
                        default=0o077)
    return parser.parse_args()


def validate_args(args):
    if args.dropbox and not args.barcode:
        raise ValueError("barcode must be specified if dropbox is")
    if args.daemon and not args.pidfile:
        raise ValueError("pidfile must be specified if daemon is")
    if args.daemon:
        if os.path.exists(args.pidfile):
            raise ValueError("Pidfile exists: %s" % args.pidfile)
        if not os.path.isdir(os.path.dirname(args.pidfile)):
            raise ValueError("Invalid pidfile: %s" % args.pidfile)


def prepare_command(workdir, args):
    workflow_dirs = projects.clone_workflows(workdir, args.workflow)
    if args.data:
        projects.copy_data(workdir, args.data, args.user)
    return workflow_dirs


def run_command(workdir, args):
    def run_commit(workdir, args):
        prepare_command(workdir, args)
        projects.run(workdir)
        if args.dropbox:
            commit_command(workdir, args)

    if args.daemon:
        utils.daemonize(run_commit, args.pidfile, args.umask, workdir, args)
    else:
        run_commit(workdir, args)


def commit_command(workdir, args):
    projects.commit(workdir, args.dropbox, args.barcode, args.user)


def main():
    try:
        args = parse_args()
        validate_args(args)
        logger.info(
            "Starting qproject for user %s with command '%s' and target '%s'",
            args.user, args.command, args.target
        )
        workdir = projects.prepare(args.target, force_create=False)
        if args.command == 'prepare':
            prepare_command(workdir, args)
        elif args.command == 'run':
            run_command(workdir, args)
        elif args.command == 'commit':
            commit_command(workdir, args)
    except Exception:
        logger.exception("Failed to run qproject:")
        sys.exit(1)

if __name__ == '__main__':
    main()
