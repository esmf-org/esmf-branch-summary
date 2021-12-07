"""
Summary

author: Ryan Long <ryan.long@noaa.gov>
"""

import argparse
import logging
import os
import pathlib


logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)


def get_args():
    """get_args [summary]

    Returns:
        [type]: [description]
    """
    parser = argparse.ArgumentParser(description="new_project description")
    parser.add_argument(
        "positional argument path",
        type=pathlib.Path,
        help="positional argument path",
        default=os.getcwd(),
    )
    parser.add_argument(
        "-n",
        "--name",
        help="optional flag with name",
        default="default",
    )
    parser.add_argument(
        "-b",
        "--bool",
        help="boolean flag",
        default=False,
    )
    return parser.parse_args()


def main():
    """main execution"""
    print("main")


if __name__ == "__main__":
    main()
