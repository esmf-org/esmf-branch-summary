"""
Git commands module

Subprocess convenience module for interacting with Git

author: Ryan Long <ryan.long@noaa.gov>
"""


import os
import subprocess
import logging
import argparse
import pathlib
import csv

import esmf_git as git

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.DEBUG)


def get_args():
    """get_args display CLI to user and gets options

    Returns:
        Namespace: object-
    """
    parser = argparse.ArgumentParser(
        description="Git log parser for generating records"
    )
    parser.add_argument(
        "repo_path",
        type=pathlib.Path,
        help="path to esmf-artifacts-merge",
        default=os.getcwd(),
    )
    parser.add_argument(
        "-n",
        "--name",
        help="name of the branch to use",
        default="develop",
    )
    parser.add_argument(
        "-a",
        "--all",
        help="pulls from all servers",
        default=False,
    )
    return parser.parse_args()


# branch = server(hera, cheyenne), branch_name=branch(main, develop)
def checkout(branch_name, server="", path=os.getcwd()):
    """checkout a branch or branch/server combo

    Args:
        branch_name (str):
        server (str, optional): Defaults to "".
        path (str, optional): Defaults to os.getcwd().
    """
    # logger.debug(branch_name, server, path)
    if server == "":
        logger.debug("running with server %s", server)
        return git.checkout(branch_name, repopath=path)
    return git.checkout(server, "origin", branch_name, repopath=path)


def find_files_containing_string(value, _root_path):
    """find_files_containing_string recursive searches through
    all files in the _root_path returning a list of files
    containing the string value

    Args:
        value (str): value to search for
        _root_path (str): root path to search

    Returns:
        list: file_paths
    """
    results = []
    for root, _, files in os.walk(_root_path, followlinks=True):
        for file in files:
            if "summary.dat" in file:
                with open(os.path.join(root, file)) as _file:
                    if value in _file.read():
                        results.append(os.path.join(root, file))
    return results


def get_last_branch_hash(branch_name, server):
    """get_last_branch_hash return the hash of the last commit
    to the branch/server

    Args:
        branch_name (str): develop, main, etc
        server (str): cheyenne, hera

    Returns:
        str:
    """
    result = subprocess.run(
        ["git", "log", "--format=%B", f"origin/{server}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        encoding="utf-8",
    )

    for entry in result.stdout.split("\n"):
        if branch_name in entry and server in entry:
            return entry.split(" ")[7]
    return ""


def get_test_results(file_path):
    """get_test_results scrapes data from the file at
    file_path and compiles a csv/table summarizing the
    data.

    Each return value is essentially a row in the csv, with
    key/value pairs.

    Args:
        file_path (str): absolute or relative file path

    Returns:
        dict: fieldname/value
    """
    results = {}
    with open(file_path, "r") as _file:
        for line in _file:
            if "Build for" in line:
                line_cleaned = line.split("=", 1)[1].strip()
                group1, group2 = line_cleaned.split(",")
                (
                    results["compiler"],
                    results["version"],
                    results["mpi_type"],
                    results["o_g"],
                    results["branch"],
                ) = group1.strip().split("_")

                (
                    _,
                    _,
                    results["mpi_version"],
                    _,
                    results["host"],
                    _,
                    results["os"],
                ) = group2.strip().split(" ")
            if "test results" in line:
                key, value = line.split("\t", 1)
                key_cleaned = key.split(" ", 1)[0]

                try:
                    pass_, fail_ = value.split("\t", 1)
                    pass_ = (
                        pass_.replace("\n", "")
                        .replace("-1", "queued")
                        .strip()
                        .split(" ")[1]
                    )
                    fail_ = (
                        fail_.replace("\n", "")
                        .replace("-1", "queued")
                        .strip()
                        .split(" ")[1]
                    )
                    results[f"{key_cleaned}_pass"] = pass_
                    results[f"{key_cleaned}_fail"] = fail_
                    results[f"{key_cleaned}_fail"] = fail_

                except ValueError as _:
                    pass_, fail_ = "fail", "fail"
                    results[f"{key_cleaned}_pass"] = pass_
                    results[f"{key_cleaned}_fail"] = fail_
                    logger.error(
                        "No %s test results in file %s", key_cleaned, file_path
                    )
    return results


def write_file(data, file_path):
    """write_file writes the data to file_path
    as csv.

    Args:
        data (dict):
        file_path (str):
    """
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def main():
    """main point of execution"""
    server_list = [
        "cheyenne",
        "hera",
        "orion",
        "jet",
        "gaea",
        "discover",
        "chianti",
        "acorn",
    ]

    cwd = os.getcwd()

    args = get_args()
    logger.debug("Args are : %s", args)
    os.chdir(args.repo_path)
    branch_name = args.name
    logger.info("HEY branchname is %s", branch_name)

    logger.info("checking out main")
    checkout("main")

    for server in server_list:
        logger.info("checking out branch_name %s from server %s", branch_name, server)
        checkout(branch_name, server, args.repo_path)
        _hash = get_last_branch_hash(branch_name, server)
        logger.info("last branch hash is %s", _hash)
        found_files = find_files_containing_string(_hash, os.path.abspath(branch_name))
        logger.info("searching %d files", len(found_files))

        results = []
        for idx, _file in enumerate(found_files):
            results.append(get_test_results(_file))
            if idx % 10 == 0:
                logger.info("scanned %d", idx)
        write_file(results, os.path.join(cwd, "./eggs.csv"))


if __name__ == "__main__":
    main()
