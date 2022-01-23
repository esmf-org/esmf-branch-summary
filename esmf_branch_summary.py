"""
ESMF Branch Summary Tool

Aggregates and summarizes ESMF test results and pushes them
to a repository in a markdown formatted table.

author: Ryan Long <ryan.long@noaa.gov>
"""


import bisect
import datetime
import hashlib
import inspect
import logging
import os

import subprocess

from collections import OrderedDict
from typing import List

from view import ViewCLI
from git import Git
from gateway import Archive, SummaryRow
from tabulate import tabulate

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


def any_string_in_string(needles, haystack):
    return any(needle in haystack for needle in needles)


def find_files(
    _root_path,
    value_search_strings="",
    file_name_search_strings=[],
    file_name_ignore_strings=[],
):

    if not os.path.exists(_root_path):
        raise ValueError(f"{_root_path} is invalid")

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
                with open(file_path, "r", errors="ignore", encoding="utf-8") as _file:
                    for line in _file.readlines():
                        if any(
                            search_string in line
                            for search_string in value_search_strings
                        ):

                            bisect.insort(results, os.path.join(root, file))
    return results


def get_last_branch_hash(branch_name):
    # TODO This is broken, getting the last hash from log...
    """get_last_branch_hash return the hash of the last commit
    to the branch/machine_name

    Args:
        branch_name (str): develop, main, etc
        machine_name (str): cheyenne, hera

    Returns:
        str:
    """
    try:
        result = subprocess.run(
            ["git", "log", "--format=%B", branch_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            encoding="utf-8",
        )
        raw_line = list(
            filter(
                lambda x: branch_name in x and "ESMF" in x, result.stdout.split("\n")
            )
        )[0]
        # TODO Why do we need to account for both?
        try:
            return raw_line.split(" ")[7]
        except IndexError as _:
            return raw_line.split(" ")[4]
    except subprocess.CalledProcessError as e:
        logging.error(e.stderr)
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


def fetch_test_results(file_path):
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


def generate_id(row, _hash):
    return hashlib.md5(
        f"{row['branch']}{row['host']}{row['os']}{row['compiler_type']}{row['compiler_version']}{row['mpi_type']}{row['mpi_version']}{_hash}".encode()
    ).hexdigest()


def write_archive(data, _hash, gateway: Archive):
    timestamp = datetime.datetime.now()
    gateway.create_table()
    rows = [
        SummaryRow(**item, hash=_hash, modified=timestamp, id=generate_id(item, _hash))
        for item in data
    ]
    gateway.insert_rows(rows)


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


def generate_commit_message(_machine_name, branch_name, _hash):
    return f"updated summary for hash {_hash} on {branch_name}"


def handle_logging(args):
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
    logging.basicConfig(level=level, format=LOG_FORMAT)


def get_matching_logs(cwd, _hash):
    return set(find_files(cwd, [_hash], ["build.log"]))


def get_matching_summaries(cwd, _hash):
    return set(find_files(cwd, [_hash], ["summary.dat"]))


def parse_logs_for_build_passing(matching_logs):
    build_passing_results = []
    for _file in matching_logs:
        build_passing_results.append(
            dict(**normalize(_file), **{"build_passed": is_build_passing(_file)})
        )
    return build_passing_results


def compile_test_results(matching_summaries, build_passing_results, branch_name):
    test_results = []
    for idx, _file in enumerate(matching_summaries):

        result = fetch_test_results(_file)
        pass_fail = fetch_build_result(result, build_passing_results)

        test_results.append(
            {**result, "branch": branch_name, "build_passed": pass_fail}
        )
        if idx % 10 == 0:
            logging.debug("scanned %d", idx)
    return test_results


def main():
    """main point of execution"""
    args = ViewCLI(MACHINE_NAME_LIST).get_args()

    handle_logging(args)
    logging.debug("Args are : %s", args)

    gateway = Archive("./summaries.db")
    repopath = os.path.abspath(args.repo_path)
    git = Git(repopath)
    git.git_fetch()

    machine_name = args.machine_name
    branches = args.branches
    for branch_name in branches:

        CWD = os.path.abspath(os.path.join(repopath, branch_name))
        os.chdir(CWD)
        logging.debug("current working directory is: %s = %s", CWD, os.getcwd())

        logging.info("git checking out branch: %s", branch_name)
        git.git_checkout(branch_name)

        logging.info("finding last branch hash")
        _hash = get_last_branch_hash(branch_name)  # TODO change to regex?
        if _hash == "":
            logging.error("could not find last hash for branch: %s", branch_name)
            continue
        logging.info("last branch hash is %s", _hash)

        # TODO Check if the file is empty, then warn or error
        output_file_path = os.path.abspath(os.path.join(repopath, f"{_hash}.md"))

        logging.info("fetching matching logs to determine build pass/fail")
        matching_logs = get_matching_logs(CWD, _hash)

        logging.info("fetching matching summary files to extract test results")
        matching_summaries = get_matching_summaries(CWD, _hash)

        logging.info("parsing %s logs", len(matching_logs))
        build_passing_results = parse_logs_for_build_passing(matching_logs)
        logging.info("done parsing logs")

        logging.info("parsing %d summaries", len(matching_summaries))
        test_results = compile_test_results(
            matching_summaries, build_passing_results, branch_name
        )
        logging.info("done parsing summaries")

        git.git_checkout("summary")
        write_archive(test_results, _hash, gateway)

        summary_file_contents = [
            item._asdict() for item in gateway.fetch_rows_by_hash(_hash)
        ]
        write_file(summary_file_contents, output_file_path)

        logging.info("adding all modified files in summary")
        git.git_add()

        logging.info("committing to %s", "summary")
        git.git_commit(generate_commit_message(machine_name, branch_name, _hash))

        logging.info("pushing to summary")
        git.git_push("origin", "summary")


if __name__ == "__main__":
    import timeit

    starttime = timeit.default_timer()
    main()
    logging.info("finished in %s", timeit.default_timer() - starttime)
