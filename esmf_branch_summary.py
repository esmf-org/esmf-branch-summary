"""
ESMF Branch Summary Tool

Aggregates and summarizes ESMF test results and pushes them
to a repository in a markdown formatted table.

author: Ryan Long <ryan.long@noaa.gov>
"""


import os
import subprocess
import logging
import argparse
import pathlib
from typing import List
from collections import OrderedDict
import inspect

from tabulate import tabulate

import esmf_git as git


def get_args():
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
        default=os.getcwd(),
    )
    parser.add_argument(
        "-n",
        "--name",
        help=("name of the branch to use. " "Example --name 'develop'"),
        default="develop",
    ),
    parser.add_argument(
        "-log",
        "--log",
        default="warning",
        help=("Provide logging level. " "Example --log debug', default='warning'"),
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
    logging.debug("fx=%s: vars=%s", inspect.stack()[0][3], locals())
    if server == "":
        return git.checkout(branch_name, repopath=path)
    return git.checkout(server, "origin", branch_name, repopath=path)


def any_string_in_string(needles, haystack):
    return any(needle in haystack for needle in needles)


def find_files(
    _root_path,
    value_search_strings="",
    file_name_search_strings=[],
    file_name_ignore_strings=[],
):
    import bisect

    results = []
    if not isinstance(value_search_strings, List):
        value_search_strings = value_search_strings.split()

    for root, _, files in os.walk(_root_path, followlinks=True):
        for file in files:
            file = os.path.join(root, file)
            has_filename_search = bool(
                len(file_name_search_strings) + len(file_name_ignore_strings)
            )
            has_filename_search_string = any(
                search_string in file for search_string in file_name_search_strings
            )
            has_filename_ignore_string = any(
                search_string in file for search_string in file_name_ignore_strings
            )

            if not has_filename_search or (
                has_filename_search_string and not has_filename_ignore_string
            ):
                file_path = os.path.join(root, file)
                with open(file_path, "r", errors="ignore") as _file:
                    for line in _file.readlines():
                        if any(
                            search_string in line
                            for search_string in value_search_strings
                        ):

                            bisect.insort(results, os.path.join(root, file))
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


def is_build_passing(file_path):
    logging.debug("fx=%s: vars=%s", inspect.stack()[0][3], locals())
    if not os.path.exists(file_path):
        logging.error("file path %s does not exist", file_path)
        return False
    with open(file_path, "r") as _file:
        is_passing = False
        lines_read = []
        for idx, line in enumerate(reversed(list(_file))):
            if "ESMF library built successfully" in line:
                is_passing = True
            lines_read.append(line)
            # Check the last 5 lines only for speed
            if idx > 5:
                break
        logging.debug("build result not found, see output:")
        logging.debug(lines_read)
        return is_passing


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
    _temp = {}
    results = OrderedDict()
    with open(file_path, "r") as _file:
        for line in _file:
            if "Build for" in line:
                line_cleaned = line.split("=", 1)[1].strip()
                group1, group2 = line_cleaned.split(",")
                (
                    _temp["compiler_type"],
                    _temp["compiler_version"],
                    _temp["mpi_type"],
                    _temp["o_g"],
                    _temp["branch"],
                ) = group1.strip().split("_")

                (
                    _,
                    _,
                    _temp["mpi_version"],
                    _,
                    _temp["host"],
                    _,
                    _temp["os"],
                ) = group2.strip().split(" ")

                # Keeps the order of insertion for printing
                results["branch"] = _temp["branch"]
                results["host"] = _temp["host"]
                results["compiler_type"] = _temp["compiler_type"]
                results["compiler_version"] = _temp["compiler_version"]
                results["mpi_type"] = _temp["mpi_type"]
                results["mpi_version"] = _temp["mpi_version"]
                results["o_g"] = _temp["o_g"]
                results["os"] = _temp["os"]

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
                    logging.warning(
                        "No %s test results in file %s", key_cleaned, file_path
                    )
    return results


def write_file(data, file_path):
    # logging.debug("fx=%s: vars=%s", inspect.stack()[0][3], locals())
    _sorted = sorted(
        data,
        key=lambda x: x["branch"]
        + x["host"]
        + x["compiler_type"]
        + x["compiler_version"]
        + x["mpi_type"]
        + x["mpi_version"]
        + x["o_g"],
    )
    table = tabulate(_sorted, headers="keys", showindex="always", tablefmt="github")
    logging.debug("writing file %s", file_path)
    with open(file_path, "w", newline="") as _file:
        _file.write(table)


def normalize(_path):
    result = os.path.normpath(_path).split(os.sep)
    if len(result) < 14:
        result.insert(-2, "none")

    return {
        "branch": result[-9],
        "host": result[-8],
        "compiler_type": result[-7],
        "compiler_version": result[-6],
        "o_g": result[-5],
        "mpi_type": result[-4],
        "mpi_version": result[-3].lower(),
    }


def fetch_build_result(needle, haystack):
    try:
        result = filter(
            lambda x: x["compiler_type"].lower() == needle["compiler_type"].lower()
            and x["compiler_version"].lower() == needle["compiler_version"].lower()
            and x["mpi_type"].lower() == needle["mpi_type"].lower()
            and x["mpi_version"].lower() == needle["mpi_version"].lower()
            and x["host"].lower() == needle["host"].lower(),
            haystack,
        )
        return list(result)[0]["build_passed"]
    except IndexError as _:
        logging.warning("build result not found")
        logging.debug("needle: %s", needle)
        return False


def generate_commit_message(_server, branch_name, _hash):
    return f"updated summary for hash {_hash} on {branch_name}{_server}"


def main():
    """main point of execution"""
    args = get_args()
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
    if level is None:
        raise ValueError(
            f"log level given: {args.log}"
            f" -- must be one of: {' | '.join(levels.keys())}"
        )
    LOG_FORMAT = "%(asctime)s_%(name)s_%(levelname)s: %(message)s"
    logging.basicConfig(level=level, format=LOG_FORMAT)

    logging.debug("Args are : %s", args)

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

    os.chdir(args.repo_path)
    git.pull(args.repo_path)
    branch_name = args.name
    logging.debug("HEY branchname is %s", branch_name)
    logging.info("checking out main")
    checkout("main")

    for server in server_list:
        logging.info("checking out branch_name %s from server %s", branch_name, server)
        checkout(branch_name, server, args.repo_path)
        _hash = get_last_branch_hash(branch_name, server)
        logging.info("last branch hash is %s", _hash)

        logging.info("fetching matching logs to determine build pass/fail")
        matching_logs = set(
            find_files(os.path.abspath(branch_name), [_hash], ["build.log"])
        )
        logging.info("parsing %s logs", len(matching_logs))
        build_passing_results = []
        for idx, _file in enumerate(matching_logs):
            build_passing_results.append(
                dict(**normalize(_file), **{"build_passed": is_build_passing(_file)})
            )
        logging.info("done parsing logs")

        logging.info("fetching matching summary files to extract test results")
        test_results = []
        matching_summaries = set(
            find_files(os.path.abspath(branch_name), [_hash], ["summary.dat"])
        )
        logging.info("parsing %d summaries", len(matching_summaries))
        for idx, _file in enumerate(matching_summaries):

            result = get_test_results(_file)
            pass_fail = fetch_build_result(result, build_passing_results)

            test_results.append(
                {**result, "branch": branch_name, "build_passed": pass_fail}
            )
            if idx % 10 == 0:
                logging.debug("scanned %d", idx)
        logging.info("done parsing summaries")

        output_file_path = os.path.abspath(
            os.path.join(args.repo_path, branch_name, f"{_hash}.md")
        )

        logging.info("writing summary results to %s", output_file_path)
        write_file(test_results, output_file_path)

        logging.info("git add %s, %s", output_file_path, args.repo_path)
        git.add(output_file_path, args.repo_path)

        logging.info(
            "committing [%s/%s/%s] to %s", server, branch_name, _hash, args.repo_path
        )
        git.commit(
            generate_commit_message(server, branch_name, _hash), args.repo_path
        )  # Message update for test of intel_18.0.5_mpt_g_develop with hash ESMF_8_3_0_beta_snapshot_04-8-g60a38ef on cheyenne
        logging.info("pushing summary to main from %s", args.repo_path)
        try:
            git.push(branch="main", repopath=args.repo_path)
        except subprocess.CalledProcessError as _:
            logging.error(
                "git push failed.  Try updating the esmf-test-artifacts repo."
            )
            raise


if __name__ == "__main__":
    main()
