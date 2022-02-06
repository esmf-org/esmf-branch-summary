#!/glade/u/apps/ch/opt/python/3.7.9/gnu/9.1.0/bin/python3

"""
ESMF Branch Summary Tool

Aggregates and summarizes ESMF test results and pushes them
to a repository in a markdown formatted table.

author: Ryan Long <ryan.long@noaa.gov>
"""


import logging
import os
import pathlib
import signal
import sys
import timeit


import compass as _compass
import gateway as _gateway
import git as _git
import view as _view
import job as _job

MACHINE_NAME_LIST = sorted(
    [
        "cheyenne",
        "hera",
        "orion",
        "jet",
        "gaea",
        "discover",
        "chianti",
        "acorn",
    ]
)

LOG_FORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
LOG_FORMATTER = logging.Formatter(LOG_FORMAT)


def handle_logging(args):
    """handles logging based on CLI arguments"""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    levels = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }
    level = levels.get(args.log.lower())
    if level is None:
        raise ValueError(
            f"log level given: {args.log}"
            f" -- must be one of: {' | '.join(levels.keys())}"
        )

    logging.basicConfig(
        level=logging.DEBUG,
        format=LOG_FORMAT,
        filename=f"{os.path.join(dir_path, 'esmf-branch-summary.log')}",
        filemode="w",
    )
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(LOG_FORMATTER)
    logging.getLogger("").addHandler(console)


def signal_handler(_, __):
    """run code before exiting on ctrl-c"""
    print("Exiting.")
    sys.exit(0)


def main():
    """main point of execution"""
    starttime = timeit.default_timer()
    signal.signal(signal.SIGINT, signal_handler)

    args = _view.ViewCLI().get_args()
    handle_logging(args)

    logging.info("starting...")
    logging.debug("Args are : %s", args)

    root = pathlib.Path(__file__)
    repopath = pathlib.Path(os.path.abspath(args.repo_path))

    compass = _compass.Compass.from_path(root, repopath)
    archive = _gateway.Archive(compass.archive_path)
    git = _git.Git(str(compass.repopath))

    processor = _job.JobProcessor(
        MACHINE_NAME_LIST,
        args.branches,
        args.number,
        _job.BranchSummaryGateway(git, archive, compass),
    )
    logging.info(
        "itterating over %s branches in %s machines over %s most recent branches",
        len(args.branches),
        len(MACHINE_NAME_LIST),
        args.number,
    )
    processor.run_jobs()
    logging.info("finished in %s", (timeit.default_timer() - starttime) / 60)


if __name__ == "__main__":
    main()
