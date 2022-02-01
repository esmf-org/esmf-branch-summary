#!/glade/u/apps/ch/opt/python/3.7.9/gnu/9.1.0/bin/python3

"""
ESMF Branch Summary Tool

Aggregates and summarizes ESMF test results and pushes them
to a repository in a markdown formatted table.

author: Ryan Long <ryan.long@noaa.gov>
"""


import bisect
import datetime
import hashlib
import itertools
import logging
import os
import re
import signal
import sys
import timeit
from collections import OrderedDict
from typing import Generator, Tuple

from tabulate import tabulate
from compressor import Compressor

from gateway import Archive, SummaryRow
from git import Git
from view import ViewCLI

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


def find_files(
    _root_path,
    value_search_strings="",
    file_name_search_strings=None,
    file_name_ignore_strings=None,
):

    if not os.path.exists(_root_path):
        raise ValueError(f"{_root_path} is invalid")

    file_name_search_strings = (
        [] if file_name_search_strings is None else list(file_name_search_strings)
    )

    file_name_ignore_strings = (
        [] if file_name_ignore_strings is None else list(file_name_ignore_strings)
    )

    results = []
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


def get_recent_branch_hashes(
    machine_name, branch_name, limit, git
) -> Generator[str, None, None]:
    """Returns the most recent branch on machine_name + branch_name"""
    count = 0
    hashes = get_branch_hashes(machine_name, git, branch_name)
    while count < limit:
        try:
            yield next(hashes)
        except StopIteration:
            return
        count += 1


class Error(Exception):
    """Base class for other exceptions"""


class HashNotFound(Exception):
    "Raised when no branch has is found"


def to_unique(items: Generator[str, None, None]) -> Generator[str, None, None]:
    """Returns a list with only unique values, regardles if hashable"""
    result = []
    for item in items:
        if item not in result:
            result.append(item)
            yield item


def get_branch_hashes(
    machine_name, git: Git, branch_name=None
) -> Generator[str, None, None]:
    """Uses git log to determine all hashes for a machine_name/(branch_name)"""
    result = git.git_log(f"origin/{machine_name}")

    _stdout = result.stdout.split("\n")
    pattern = r"ESMF.*-\S{8}"
    if branch_name is not None:
        _stdout = (line for line in _stdout if branch_name in line)
    return to_unique(
        re.findall(pattern, item)[0]
        for item in _stdout
        if len(re.findall(pattern, item)) > 0
    )


def is_build_passing(file_path):
    """Determines if the build is passing by scanning file_path"""
    if not os.path.exists(file_path):
        logging.error("file path %s does not exist", file_path)
        return False
    with open(file_path, "r", encoding="utf-8") as _file:
        lines_read = []
        for idx, line in enumerate(reversed(list(_file))):
            if "ESMF library built successfully" in line:
                return True
            lines_read.append(line)
            # Check the last 5 lines only for speed
            if idx > 25:
                logging.debug("build result not found, see output below:")
                logging.debug("\n".join(lines_read))
                break
        return False


def fetch_test_results(file_path, compressor: Compressor):
    """Fetches test results from file_path and returns them as an ordered dict"""
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
                ) = group1.strip().split("_")[:5]

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
                    compressor.add(file_path)
    return results


def write_file(data, file_path):
    _sorted = sorted(
        data,
        key=lambda x: str(x["build_passed"])
        + x["branch"]
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
    """generate an md5 hash from unique row data"""
    return hashlib.md5(
        f"{row['branch']}{row['host']}{row['os']}{row['compiler_type']}{row['compiler_version']}{row['mpi_type']}{row['mpi_version'].lower()}{row['o_g']}{_hash}".encode()
    ).hexdigest()


def write_archive(data, _hash, gateway: Archive):
    timestamp = datetime.datetime.now()
    gateway.create_table()

    rows = [
        SummaryRow(
            **item,
            hash=_hash,
            modified=timestamp,
            id=generate_id(item, _hash),
        )
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


def generate_commit_message(branch_name, _hash):
    return f"updated summary for hash {_hash} on {branch_name}"


def handle_logging(args):
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
    LOG_FORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
    LOG_FORMATTER = logging.Formatter(LOG_FORMAT)
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


def get_matching_logs(cwd: str, _hash: str):
    return set(find_files(cwd, [_hash.replace("/", "_")], ["build.log"]))


def get_matching_summaries(cwd: str, _hash: str):
    return set(find_files(cwd, [_hash.replace("/", "_")], ["summary.dat"]))


def parse_logs_for_build_passing(matching_logs):
    build_passing_results = []
    for idx, _file in enumerate(matching_logs):
        build_passing_results.append(
            dict(**normalize(_file), **{"build_passed": is_build_passing(_file)})
        )
        if idx % 10 == 0 and idx > 0:
            logging.debug("%d finished", idx)
        if idx >= len(matching_logs) - 1:
            logging.debug("%d finished", idx + 1)
    return build_passing_results


def compile_test_results(
    matching_summaries, build_passing_results, branch_name, compressor: Compressor
):
    test_results = []
    for idx, _file in enumerate(matching_summaries):

        result = fetch_test_results(_file, compressor)
        pass_fail = fetch_build_result(result, build_passing_results)

        test_results.append(
            {**result, "branch": branch_name, "build_passed": pass_fail}
        )
        if idx % 10 == 0 and idx > 0:
            logging.debug("%d finished", idx)
        if idx >= len(matching_summaries) - 1:
            logging.debug("%d finished", idx + 1)
    return test_results


def generate_permutations(list1, list2) -> Generator[Tuple, None, None]:
    """retuns list of tuples containing each permutation of the two lists"""
    return (each_permutation for each_permutation in itertools.product(list1, list2))


def generate_summary_file_contents(_hash, gateway):
    return [item._asdict() for item in gateway.fetch_rows_by_hash(_hash)]


def generate_link(**kwds):
    """generates a link to github to jump to the _hash passed in"""
    return f"[artifacts](https://github.com/esmf-org/esmf-test-artifacts/tree/{kwds['host'].replace('/', '_')}/{kwds['branch']}/{kwds['host'].replace('/', '_')}/{kwds['compiler_type']}/{kwds['compiler_version']}/{kwds['o_g']}/{kwds['mpi_type']}/{kwds['mpi_version'].lower()})"


def strip_branch_prefix(value):
    """removes 'origin' and '/' the the branch"""
    return value.replace("origin", "").replace("/", "_")


def fetch_summary_file_contents(_hash, gateway):
    """fetches the contents to create a summary file based on _hash"""
    results = []
    for item in gateway.fetch_rows_by_hash(_hash):
        row = item._asdict()
        row["hash"] = generate_link(**item._asdict())
        results.append(dict(**row))
    return results


def get_cwd(repopath, branch_name):
    CWD = os.path.abspath(os.path.join(repopath, strip_branch_prefix(branch_name)))
    if not os.path.exists(CWD):
        logging.debug("creating directory %s", CWD)
        os.mkdir(CWD)
    return CWD


def get_compressor(tar_path, branch_name, machine_name, _hash) -> Compressor:
    if not os.path.exists(tar_path):
        os.makedirs(tar_path)
    return Compressor(
        os.path.join(
            tar_path,
            f"./{branch_name.replace('/', '_')}-{machine_name}-{_hash}_error_artifacts.tar.gz",
        )
    )


def generate_summaries(
    machine_name, branch_name, git, qty, CWD, repopath, gateway, ROOT_CWD
):
    for _hash in get_recent_branch_hashes(machine_name, branch_name, qty, git):

        logging.debug("last branch hash is %s", _hash)

        logging.debug("fetching matching logs to determine build pass/fail")
        matching_logs = get_matching_logs(CWD, _hash)

        logging.debug("fetching matching summary files to extract test results")
        matching_summaries = get_matching_summaries(CWD, _hash)

        logging.debug("reading %s logs", len(matching_logs))
        build_passing_results = parse_logs_for_build_passing(matching_logs)
        logging.debug("finished reading logs")

        logging.debug("reading %d summaries", len(matching_summaries))
        tar_path = os.path.join(ROOT_CWD, "error_artifacts")
        compressor = get_compressor(tar_path, branch_name, machine_name, _hash)
        test_results = compile_test_results(
            matching_summaries, build_passing_results, branch_name, compressor
        )
        compressor.close()
        logging.debug("finished reading summaries")

        git.git_checkout(branch_name="summary", force=True)
        if len(test_results) > 0:
            output_file_path = os.path.abspath(
                os.path.join(repopath, f"{_hash.replace('/', '_')}.md")
            )
            write_archive(test_results, _hash, gateway)
            write_file(fetch_summary_file_contents(_hash, gateway), output_file_path)

            logging.debug("adding all modified files in summary")
            git.git_add()

            logging.debug("committing to %s", "summary")
            git.git_commit(generate_commit_message(branch_name, _hash))

        logging.debug("pushing to summary")
        git.git_push("origin", "summary")
        logging.info(
            "finished summary for B:%s M: %s [%s]", branch_name, machine_name, _hash
        )


def signal_handler(sig, frame):
    print("Exiting.")
    sys.exit(0)


def main():

    """main point of execution"""
    signal.signal(signal.SIGINT, signal_handler)

    ROOT_CWD = os.getcwd()

    starttime = timeit.default_timer()
    args = ViewCLI().get_args()
    handle_logging(args)

    logging.info("starting...")
    logging.debug("Args are : %s", args)

    gateway = Archive(os.path.join(ROOT_CWD, "./summaries.db"))
    repopath = os.path.abspath(args.repo_path)
    git = Git(repopath)

    logging.debug("running git_fetch")
    git.git_fetch()

    branches = (
        args.branches
        if args.branches is not None
        else git.git_snapshot("https://github.com/esmf-org/esmf")
    )

    logging.info(
        "itterating over %s branches in %s machines over %s most recent branches",
        len(branches),
        len(MACHINE_NAME_LIST),
        args.number,
    )

    for machine_name, branch_name in generate_permutations(MACHINE_NAME_LIST, branches):
        logging.info(
            "starting summaries for branch %s on machine %s", branch_name, machine_name
        )
        logging.debug("git checking out branch: %s [%s]", branch_name, machine_name)
        git.git_checkout(machine_name)
        CWD = get_cwd(repopath, branch_name)
        os.chdir(CWD)
        generate_summaries(
            machine_name,
            branch_name,
            git,
            args.number,
            CWD,
            repopath,
            gateway,
            ROOT_CWD,
        )
        logging.info(
            "finished summaries for branch %s on machine %s", branch_name, machine_name
        )

    logging.info("finished in %s", (timeit.default_timer() - starttime) / 60)


if __name__ == "__main__":
    main()
