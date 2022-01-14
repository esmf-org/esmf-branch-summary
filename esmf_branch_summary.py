"""
ESMF Branch Summary Tool

Aggregates and summarizes ESMF test results and pushes them
to a repository in a markdown formatted table.

author: Ryan Long <ryan.long@noaa.gov>
"""


import argparse
import bisect
import inspect
import logging
import os
import sys
import pathlib
import subprocess
from collections import OrderedDict
from typing import List

import esmf_git as git
from tabulate import tabulate


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
        default="develop",
        help=("name of the branch to use. " "Example --name 'develop'"),
    )
    parser.add_argument(
        "-log",
        "--log",
        default="warning",
        help=("Provide logging level. " "Example --log debug', default='warning'"),
    )

    return parser.parse_args()


def memoize(func):
    cache = {}

    def memoized_func(*args):
        if args in cache:
            return cache[args]
        result = func(*args)
        cache[args] = result
        return result

    return memoized_func


def checkout(branch_name, server="", repopath=os.getcwd()):
    """checkout a branch or branch/server combo

    Args:
        branch_name (str):
        server (str, optional): Defaults to "".
        path (str, optional): Defaults to os.getcwd().
    """
    logging.debug("fx=%s: vars=%s", inspect.stack()[0][3], locals())
    if server == "":
        return git.checkout(branch_name, repopath=repopath)
    return git.checkout(server, "origin", branch_name, repopath=repopath)


def find_files(
    _root_path,
    value_search_strings="",
    file_name_search_strings=None,
    file_name_ignore_strings=None,
):

    results = []
    if not isinstance(value_search_strings, List):
        value_search_strings = value_search_strings.split()

    for root, _, files in os.walk(_root_path, followlinks=True):
        for file in files:
            file = os.path.join(root, file)
            has_filename_search_string = (
                True
                if file_name_search_strings is None
                else any(
                    search_string in file for search_string in file_name_search_strings
                )
            )
            has_filename_ignore_string = (
                False
                if file_name_ignore_strings is None
                else any(
                    search_string in file for search_string in file_name_ignore_strings
                )
            )

            if has_filename_search_string and not has_filename_ignore_string:
                file_path = os.path.join(root, file)
                with open(file_path, "r", errors="ignore", encoding="utf-8") as _file:
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
    with open(file_path, "r", encoding="utf-8") as _file:
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
    with open(file_path, "r", encoding="utf-8") as _file:
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
    logging.debug("fx=%s: vars=%s", inspect.stack()[0][3], locals())
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
    logging.debug("fx=%s: vars=%s", inspect.stack()[0][3], locals())
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
    _cache = {}

    def wrapper(*args):
        logging.debug("fx=%s: vars=%s", inspect.stack()[0][3], locals())
        if args not in _cache:
            try:
                result = filter(
                    lambda x: x["compiler_type"].lower()
                    == needle["compiler_type"].lower()
                    and x["compiler_version"].lower()
                    == needle["compiler_version"].lower()
                    and x["mpi_type"].lower() == needle["mpi_type"].lower()
                    and x["mpi_version"].lower() == needle["mpi_version"].lower()
                    and x["host"].lower() == needle["host"].lower(),
                    haystack,
                )
                logging.info(list(result)[0])
                _cache[args] = list(result)[0]["build_passed"]
            except IndexError as _:
                logging.warning("build result not found")
                logging.debug("needle: %s", needle)
                _cache[args] = False
        return _cache[args]

    return wrapper


def generate_commit_message(_server, branch_name, _hash):
    return f"updated summary for hash {_hash} on {branch_name}{_server}"


def setup_logging(args):
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
    LOG_FORMAT = "%(asctime)s:%(levelname)s:%(name)s: %(message)s"
    logging.basicConfig(level=level, format=LOG_FORMAT, stream=sys.stdout)


def fetch_build_passing_results(logs):
    _cache = {}

    def wrapper(*args):
        if args not in _cache:
            build_passing_results = []
            for _file in logs:
                build_passing_results.append(
                    dict(
                        **normalize(_file), **{"build_passed": is_build_passing(_file)}
                    )
                )
            _cache[args] = build_passing_results
        return _cache[args]

    return wrapper


def fetch_test_results(branch_name: str, _hash: str):
    matching_summaries = set(
        find_files(os.path.abspath(branch_name), [_hash], ["summary.dat"])
    )
    matching_logs = set(
        find_files(os.path.abspath(branch_name), [_hash], ["build.log"])
    )
    logging.info(
        "matched: %s logs: %s summaries", len(matching_logs), len(matching_summaries)
    )
    build_passing_results = fetch_build_passing_results(matching_logs)
    test_results = []
    for idx, _file in enumerate(matching_summaries):

        result = get_test_results(_file)
        pass_fail = fetch_build_result(result, build_passing_results)

        test_results.append(
            {**result, "branch": branch_name, "build_passed": pass_fail}
        )
        if idx % 5 == 0:
            logging.debug("scanned %d", idx)
    return test_results


def main():
    """main point of execution"""
    args = get_args()
    setup_logging(args)

    logging.debug("Args are : %s", args)

    repopath = os.path.abspath(args.repo_path)
    os.chdir(repopath)

    logging.info("running 'git pull' in %s", repopath)
    git.pull(repopath=repopath)

    logging.info("running 'git checkout main' in %s", repopath)
    checkout("main", repopath=repopath)

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
    branch_name = args.name
    for server in server_list:
        logging.info("checking out branch_name %s from server %s", branch_name, server)
        checkout(branch_name, server, repopath)

        _hash = get_last_branch_hash(branch_name, server)
        logging.info("last branch hash is %s", _hash)

        results = fetch_test_results(branch_name, _hash)

        output_file_path = os.path.abspath(
            os.path.join(repopath, branch_name, f"{_hash}.md")
        )
        logging.info("writing summary results to %s", output_file_path)
        write_file(results, output_file_path)

        logging.info("git add %s, %s", output_file_path, repopath)
        git.add(output_file_path, repopath)

        logging.info(
            "committing [%s/%s/%s] to %s", server, branch_name, _hash, repopath
        )
        git.commit(generate_commit_message(server, branch_name, _hash), repopath)

        logging.info("pushing summary to main from %s", repopath)
        try:
            git.push(branch="main", repopath=repopath)
        except subprocess.CalledProcessError as _:
            logging.error(
                "git push failed.  Try updating the esmf-test-artifacts repo."
            )
            raise


if __name__ == "__main__":
    main()
