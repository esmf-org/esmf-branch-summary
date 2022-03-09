"""
view.py

CLI View layer

author: Ryan Long <ryan.long@noaa.gov>
"""

import argparse
import pathlib


class ViewCLI:
    """interfaces user via cli"""

    @classmethod
    def get_args(cls):
        """get_args display CLI to user and gets options

        Returns:
            Namespace: object-
        """
        parser = argparse.ArgumentParser(
            description="""esmf_branch_summary aggregates esmf framework test results from
            other branches into a summary file ."""
        )
        parser.add_argument(
            "repo_path",
            type=pathlib.Path,
            help="path to esmf-artifacts-merge",
        )
        parser.add_argument(
            "-b",
            "--branches",
            nargs="+",
            help=(
                "branch(es) to summarize. All by default. "
                "Example --name develop feature_1 feature_2"
            ),
        )
        parser.add_argument(
            "-n",
            "--number",
            type=int,
            default=1,
            help=(
                "number of commits to compile from most recent" "Example --number 10"
            ),
        )
        parser.add_argument(
            "-l",
            "--log",
            default="info",
            help=("Provide logging level. " "Example --log debug', default='info'"),
        )
        parser.add_argument(
            "--reset",
            action="store_true",
            help=("deletes the database and temporary directories persisted by the app"),
        )

        return parser.parse_args()
