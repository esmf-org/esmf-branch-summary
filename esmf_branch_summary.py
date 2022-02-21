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
import shutil
import signal
import sys
import tempfile
import timeit


from src import compass as _compass
from src import git as _git
from src import view as _view
from src import job as _job
from src.gateway import summaries as _gateway


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
        "gaffney",
        "izumi",
        "koehr",
        "onyx",
    ]
)


class BranchSummaryGateway:
    """represents gateways needed"""

    def __init__(
        self,
        git_artifacts: _git.Git,
        git_summaries: _git.Git,
        archive: _gateway.DatabaseSqlite3,
        compass: _compass.Compass,
    ):
        self.git_artifacts = git_artifacts
        self.git_summaries = git_summaries
        self.archive = archive
        self.compass = compass


def handle_logging(args):
    """handles logging based on CLI arguments"""
    log_format = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    log_formatter = logging.Formatter(log_format)
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
        format=log_format,
        filename=f"{os.path.join(dir_path, 'esmf-branch-summary.log')}",
        filemode="w",
    )
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(log_formatter)
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
    archive = _gateway.Summaries(pathlib.Path(compass.archive_path))
    git_artifacts = _git.Git(str(compass.repopath))
    temp_dir = os.path.join(tempfile.gettempdir(), "esmf_branch_summary_space")
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.mkdir(temp_dir)
    git_summaries = _git.from_clone(
        "git@github.com:esmf-org/esmf-test-summary.git", temp_dir
    )

    processor = _job.JobProcessor(
        MACHINE_NAME_LIST,
        args.branches,
        args.number,
        BranchSummaryGateway(git_artifacts, git_summaries, archive, compass),
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
