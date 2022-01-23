import argparse
import pathlib


class ViewCLI:
    def __init__(self, machine_names_list):
        self.machine_names_list = machine_names_list

    def get_args(self):
        """get_args display CLI to user and gets options

        Returns:
            Namespace: object-
        """
        parser = argparse.ArgumentParser(
            description="esmf_branch_summary aggregates esmf framework test results from other branches into a summary file ."
        )
        parser.add_argument(
            "repo_path",
            type=pathlib.Path,
            help="path to esmf-artifacts-merge",
        )
        parser.add_argument(
            "machine_name",
            type=str,
            help="name of machine to summarize",
            choices=self.machine_names_list,
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
            "-l",
            "--log",
            default="warning",
            help=("Provide logging level. " "Example --log debug', default='warning'"),
        )

        return parser.parse_args()
