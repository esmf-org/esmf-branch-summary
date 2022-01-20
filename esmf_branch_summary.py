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
import pathlib
import subprocess
from collections import OrderedDict
from typing import List

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
    )
    parser.add_argument(
        "machine_name",
        type=str,
        help="name of machine to summarize",
        choices=MACHINE_NAME_LIST,
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


def any_string_in_string(needles, haystack):
    return any(needle in haystack for needle in needles)


def fetch_git_something():
    pass


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
        print(root)
        for file in files:
            print(file)
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


def git_fetch(repopath=os.getcwd()):
    return subprocess.run(
        ["git", "fetch"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=repopath,
        check=True,
        encoding="utf-8",
    )


def get_last_branch_hash(branch_name, machine_name):
    """get_last_branch_hash return the hash of the last commit
    to the branch/machine_name

    Args:
        branch_name (str): develop, main, etc
        machine_name (str): cheyenne, hera

    Returns:
        str:
    """
    result = subprocess.run(
        ["git", "log", "--format=%B", f"origin/{machine_name}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        encoding="utf-8",
    )
    raw_line = list(filter(lambda x: machine_name in x, result.stdout.split("\n")))[0]

    # TODO Why do we need to account for both?
    try:
        return raw_line.split(" ")[7]
    except IndexError as _:
        return raw_line.split(" ")[4]


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
    return f"updated summary for hash {_hash} on {branch_name}/{_machine_name}"


""" ==== GIT SECTION ==== """


class Git:
    def __init__(self, repopath=os.getcwd()):
        self.repopath = repopath

    @classmethod
    def _command_safe(cls, cmd, cwd=os.getcwd()) -> subprocess.CompletedProcess:
        """_command_safe ensures commands are run safely and raise exceptions
        on error

        https://stackoverflow.com/questions/4917871/does-git-return-specific-return-error-codes
        """
        WARNINGS = ["not something we can merge"]

        try:
            logging.debug("running '%s' in '%s'", cmd, cwd)
            return subprocess.run(
                cmd,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                encoding="utf-8",
            )
        except subprocess.CalledProcessError as error:
            logging.info(error.stdout)
            if error.stderr:
                if any((warning for warning in WARNINGS if warning in error.stderr)):
                    logging.warning(error.stderr)
                else:
                    logging.error(error.stderr)
                    raise
            return subprocess.CompletedProcess(
                returncode=0, args="", stdout=error.stdout
            )

    def git_fetch(self):
        cmd = ["git", "fetch"]
        return self._command_safe(cmd, self.repopath)

    def git_add(self, _file_path=None):
        """git_add

        Args:
            _path (str): path of assets to add
            repopath (str, optional): local repository path if not cwd. Defaults to os.getcwd().

        Returns:
            CompletedProcess:
        """
        cmd = ["git", "add", "--all"]
        if _file_path is not None:
            cmd = ["git", "add", _file_path]
        return self._command_safe(cmd, self.repopath)

    def git_checkout(self, branch_name, path_spec=None):
        """git_checkout

        Args:
            branch_name (str): name of the branch being checked out
            repopath (str, optional): local repository path if not cwd. Defaults to os.getcwd().

        Returns:
            CompletedProcess:
        """
        cmd = ["git", "checkout", branch_name]

        if path_spec is not None:
            cmd.append("--")
            cmd.append(path_spec)
        return self._command_safe(cmd, self.repopath)

    def git_commit(self, message):
        """git_commit

        Args:
            username (str):
            name (str): name of report to commit
            repopath (str, optional): local repository path if not cwd. Defaults to os.getcwd().

        Returns:
            CompletedProcess:
        """
        cmd = ["git", "commit", "-m", f"'{message}'"]
        return self._command_safe(cmd, self.repopath)

    def git_status(self):
        """status returns the output from git status

        Args:
            repopath (str, optional): The root path of the repo. Defaults to os.getcwd().

        Returns:
            CompletedProcess
        """
        return self._command_safe(["git", "status"], self.repopath)

    def git_pull(self, destination="origin", branch=None):
        """git_pull

        Args:
            destination (str, optional): Defaults to "origin".
            branch (str, optional): Defaults to current branch.
            repopath (str, optional): Defaults to os.getcwd().

        Returns:
            CompletedProcess
        """

        cmd = ["git", "pull", destination]
        if branch:
            cmd.append(branch)
        return self._command_safe(cmd, self.repopath)

    def git_push(self, destination="origin", branch=None):
        """git_push

        Args:
            destination (str, optional): Defaults to "origin".
            branch (str, optional): Defaults to current branch.
            repopath (str, optional): Defaults to os.getcwd().

        Returns:
            CompletedProcess
        """
        cmd = ["git", "push", destination]
        if branch is not None:
            cmd.append(branch)
        return self._command_safe(cmd, self.repopath)

    def git_clone(self, url, target_path):
        """git_clone

        Args:
            url (str): remote url
            target_path (str): local target path

        Returns:
            CompletedProcess
        """
        cmd = ["git", "clone", url, target_path]
        return self._command_safe(cmd, target_path)

    def git_merge(self, machine_name):
        cmd = ["git", "merge", f"{machine_name}"]
        return self._command_safe(cmd)

    def git_rebase(self, machine_name):
        cmd = ["git", "rebase", f"origin/{machine_name}"]
        return self._command_safe(cmd)


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
    args = get_args()
    handle_logging(args)
    logging.debug("Args are : %s", args)

    repopath = os.path.abspath(args.repo_path)
    git = Git(repopath)
    git.git_fetch()
    branch_name = args.branches[0]  # TODO not just develop
    machine_name = args.machine_name

    CWD = os.path.join(repopath, branch_name)
    os.chdir(CWD)
    logging.debug("current working directory is: %s = %s", CWD, os.getcwd())

    logging.info("git checkout machine_name: %s", machine_name)
    git.git_checkout(branch_name)

    _hash = get_last_branch_hash(branch_name, machine_name)  # TODO change to regex?
    logging.info("last branch hash is %s", _hash)

    # TODO Check if the file is empty, then warn or error
    output_file_path = os.path.abspath(
        os.path.join(repopath, branch_name, machine_name, f"{_hash}.md")
    )

    if not os.path.exists(output_file_path):
        logging.info("generating %s", output_file_path)
        logging.info("fetching matching logs to determine build pass/fail")
        matching_logs = get_matching_logs(branch_name, _hash)

        logging.info("fetching matching summary files to extract test results")
        matching_summaries = get_matching_summaries(branch_name, _hash)

        logging.info("parsing %s logs", len(matching_logs))
        build_passing_results = parse_logs_for_build_passing(matching_logs)
        logging.info("done parsing logs")

        logging.info("parsing %d summaries", len(matching_summaries))
        test_results = compile_test_results(
            matching_summaries, build_passing_results, branch_name
        )
        logging.info("done parsing summaries")

        logging.info("writing summary results to %s", output_file_path)
        write_file(test_results, output_file_path)

    logging.info("git add repopath")
    git.git_add()

    logging.info("committing to %s", branch_name)
    git.git_commit(generate_commit_message(machine_name, branch_name, _hash))
    git.git_push("origin", branch_name)

    git.git_fetch()

    logging.info("checking out summary")
    git.git_checkout("summary")

    logging.info(
        "checking out summary file %s from %s", f"{branch_name}{_hash}", branch_name
    )
    git.git_checkout(branch_name, f"{branch_name}/{_hash}.md")

    logging.info("adding all modified files in ")
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
