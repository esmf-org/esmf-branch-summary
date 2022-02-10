"""
ESMF Branch Summary Tool

Collection of utilities to search, aggregate, and process
repository data into summary files while also interacting
with the repository via calls to the CLI

author: Ryan Long <ryan.long@noaa.gov>
"""

import collections
import functools
import itertools
import logging
import os
import re
import bisect
import csv
import shutil
import subprocess
from typing import Any, Dict, Generator, List, Set, Tuple

from tabulate import tabulate


def _replace(old: str, new: str, target: str):
    return target.replace(old, new)


sanitize_branch_name = functools.partial(_replace, "/", "_")

BranchSummaryGateway = collections.namedtuple(
    "BranchSummaryGateway", ["git", "archive", "compass"]
)

Job = collections.namedtuple("Job", ["machine_name", "branch_name", "qty"])


class JobProcessor:
    """aggregates and summarizes processing jobs"""

    REPO_URL = "https://github.com/esmf-org/esmf"

    def __init__(
        self,
        machines: List[str],
        branches: List[str],
        history_increments: int,
        gateway: BranchSummaryGateway,
    ):

        self.machines = machines
        self._branches = branches
        self.history_increments = history_increments
        self.gateway = gateway

    def __iter__(self):
        return (x for x in self.jobs)

    def __repr__(self):
        class_name = type(self).__name__
        return f"<{class_name}>"

    @property
    def branches(self):
        """return branches to be summarized"""
        if not self._branches:
            self.gateway.git.fetch()
            self._branches = self.gateway.git.snapshot(self.REPO_URL)
        return self._branches

    @property
    def jobs(self) -> Generator[Job, None, None]:
        """returns a generator of job to be summarized"""
        return (
            Job(machine_name, branch_name, self.history_increments)
            for machine_name, branch_name in generate_permutations(
                self.machines, self.branches
            )
        )

    def run_jobs(self):
        """runs the instance jobs"""
        for job in self.jobs:
            os.chdir(self.gateway.compass.repopath)
            self.generate_summaries(job)
            logging.info(
                "finished summaries for branch %s on machine %s",
                job.branch_name,
                job.machine_name,
            )
        logging.debug("pushing to summary")
        shutil.copyfile(
            f"{self.gateway.compass.root}/esmf-branch-summary.log",
            f"{self.gateway.compass.repopath}/esmf-branch-summary.log",
        )
        shutil.copyfile(
            f"{self.gateway.compass.root}/summaries.db",
            f"{self.gateway.compass.repopath}/summaries.db",
        )
        self.gateway.git.add()
        self.gateway.git.commit("updating test artifacts")
        self.gateway.git.push("origin", "summary")

    def get_recent_branch_hashes(self, job: Job) -> Generator[str, None, None]:
        """Returns the most recent branch on machine_name + branch_name"""
        hashes = list(get_branch_hashes(job, self.gateway.git))
        for idx, _hash in enumerate(hashes):
            yield _hash
            if idx + 1 >= job.qty:
                return

    def write_archive(self, data, _hash):
        """writes the provided data to the archive"""
        self.gateway.archive.create_table()
        self.gateway.archive.insert_rows(data, _hash)

    def _verify_matches(self, matching_summaries, matching_logs, _hash):
        if not matching_summaries or not matching_logs:
            results = subprocess.run(
                [
                    "grep",
                    "-r",
                    "-n",
                    "-w",
                    ".",
                    "--exclude=esmf-branch-summary.log",
                    "--exclude-dir=.git",
                    "-e",
                    _hash,
                ],
                cwd=self.gateway.compass.repopath,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
            )
            if results.stdout != "":
                logging.error(
                    "could not verify matches, grep returned %i",
                    len(results.stdout.splitlines()),
                )

    def generate_summary(self, _hash, job: Job) -> List[str]:
        """generates summary based on _hash and job and returns the results"""
        logging.debug("last branch hash is %s", _hash)

        matching_logs = get_matching_logs(self.gateway.compass.repopath, _hash, job)
        logging.debug("matching logs: %i", len(matching_logs))

        matching_summaries = get_matching_summaries(
            str(self.gateway.compass.repopath), _hash, job
        )

        self._verify_matches(matching_summaries, matching_logs, _hash)
        logging.debug("matching summaries: %i", len(matching_summaries))

        build_passing_results = extract_build_passing_results(matching_logs)
        logging.debug("finished reading logs")

        return compile_test_results(matching_summaries, build_passing_results, job)

    def send_summary_to_repo(
        self, job: Job, summary: List[Any], _hash: str, is_latest: bool = False
    ):
        """sends the summary based on the job information to the remote repository"""
        logging.debug("checking out summary")
        self.gateway.git.checkout(branch_name="summary", force=True)
        branch_path = os.path.join(
            self.gateway.compass.repopath,
            sanitize_branch_name(job.branch_name),
        )
        if not os.path.exists(branch_path):
            logging.debug("creating directory %s", branch_path)
            os.mkdir(branch_path)

        output_file_path_prefix = os.path.abspath(os.path.join(branch_path, _hash))

        logging.debug("writing archive %s length %i", _hash, len(summary))
        self.write_archive(summary, _hash)

        logging.debug("writing files %s", output_file_path_prefix)
        self.write_files(_hash, output_file_path_prefix, is_latest)

        logging.debug("copying log file to repo")
        logging.debug("adding all modified files in summary")
        self.gateway.git.add()

        logging.debug("committing to %s", "summary")
        self.gateway.git.commit(generate_commit_message(job.branch_name, _hash))

        logging.info(
            "finished summary for B:%s M: %s [%s]",
            job.branch_name,
            job.machine_name,
            _hash,
        )

    def generate_summaries(self, job: Job):
        """generates all the summaries for job"""
        logging.info(
            "generating summaries for %s [%s]", job.branch_name, job.machine_name
        )
        logging.debug("checking out %s", job.machine_name)
        self.gateway.git.checkout(job.machine_name)
        os.chdir(
            self.gateway.compass.get_branch_path(sanitize_branch_name(job.branch_name))
        )
        for idx, _hash in enumerate(self.get_recent_branch_hashes(job)):
            summary = self.generate_summary(_hash, job)
            if len(summary) == 0:
                logging.info(
                    "No summary data found for %s, %s [%s]",
                    _hash,
                    job.branch_name,
                    job.machine_name,
                )
                continue
            self.send_summary_to_repo(job, summary, _hash, idx == 0)

    def fetch_summary_file_contents(self, _hash: str):
        """fetches the contents to create a summary file based on _hash"""
        results = []
        for item in self.gateway.archive.fetch_rows_by_hash(_hash):
            row = item._asdict()
            row["hash"] = generate_link(**item._asdict())
            row["build"] = "Pass" if row["build"] == 1 else "Fail"
            results.append(dict(**row))
        return sort_file_summary_content(results)

    def write_files(self, _hash: str, file_path: str, is_latest: bool = False):
        """writes all file types required to disk"""
        data = self.fetch_summary_file_contents(_hash)

        if is_latest is True:
            write_file_latest(data, file_path)
        write_file_md(data, file_path)
        write_file_csv(data, file_path)


def write_file_md(data: List[Dict[str, str]], file_path: str) -> None:
    """writes markdown file"""
    logging.debug("writing file md: %s", file_path)
    table = tabulate(data, headers="keys", showindex="always", tablefmt="github")
    with open(file_path + ".md", "w", newline="") as _file:
        _file.write(table)


def write_file_csv(data: List[Dict[str, str]], file_path: str) -> None:
    """writes csv file"""
    logging.debug("writing file csv[%i]: %s", len(data), file_path)
    with open(file_path + ".csv", "w", newline="") as csv_file:
        writer = csv.writer(csv_file, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(data[0].keys())
        for row in data:
            writer.writerow(row.values())


def write_file_latest(data: List[Any], file_path: str) -> None:
    """writes the most recent file as -latest.md"""
    logging.debug("writing file -latest: %s", file_path)
    table = tabulate(data, headers="keys", showindex="always", tablefmt="github")
    last_char_index = file_path.rfind("/")
    latest_file_path = file_path[:last_char_index] + "/-latest.md"
    with open(latest_file_path, "w", newline="") as _file:
        _file.write(table)


def to_unique(items: Generator[str, None, None]) -> List[Any]:
    """Returns a list with only unique values, regardles if hashable

    This will reverse the order of the list as a side affect.
    That behavior is what we want though its shadowed by the
    implementation
    """

    result = []
    for item in items:
        if item not in result:
            result.append(item)
    return result


def sort_file_summary_content(data: List[Any]) -> List[Any]:
    """sorts the summary file contents"""
    return sorted(
        data,
        key=lambda x: x["branch"]
        + x["host"]
        + x["compiler"]
        + x["c_version"]
        + x["mpi"]
        + x["m_version"]
        + x["o_g"],
    )


def generate_permutations(
    list1: List[Any], list2: List[Any]
) -> Generator[Tuple, None, None]:
    """retuns list of tuples containing each permutation of the two lists"""
    return (each_permutation for each_permutation in itertools.product(list1, list2))


def generate_commit_message(branch_name: str, _hash: str) -> str:
    """canned message for commits"""
    return f"updated summary for hash {_hash} on {branch_name}"


def get_matching_logs(cwd: str, _hash: str, job: Job) -> Set[str]:
    """finds the build.log files"""
    logging.debug("fetching matching logs to determine build pass/fail")
    return set(
        find_files(
            cwd,
            [_hash],
            ["build.log", sanitize_branch_name(job.branch_name), job.machine_name],
            ["module", "python"],
        )
    )


def get_matching_summaries(cwd: str, _hash: str, job: Job) -> Set[str]:
    """finds the summary.dat files"""
    logging.debug("fetching matching summaries to extract test results")
    return set(
        find_files(
            cwd,
            [_hash],
            ["summary.dat", sanitize_branch_name(job.branch_name), job.machine_name],
        )
    )


def find_files(
    _root_path: str,
    value_search_strings: List[str] = None,
    file_name_search_strings: List[str] = None,
    file_name_ignore_strings: List[str] = None,
) -> List[str]:
    """finds files containing all values_search_strings where file path
    includes file_name_search_strings but does not contain any
    file_name_ignore_strings"""

    if not os.path.exists(_root_path):
        raise ValueError(f"{_root_path} is invalid")

    value_search_strings = (
        [] if value_search_strings is None else list(value_search_strings)
    )

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

            has_filename_search_string = len(file_name_search_strings) == 0 or all(
                search_string in file for search_string in file_name_search_strings
            )

            has_filename_ignore_string = any(
                search_string in file for search_string in file_name_ignore_strings
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


def extract_build_passing_results(log_paths: Set[str]) -> List[Dict]:
    """searches through logs to find build_passing results"""
    build_passing_results = []
    for _file in log_paths:
        build_passing_results.append(
            dict(
                **extract_attributes_from_path(_file),
                **{"build_passed": is_build_passing(_file)},
            )
        )
    return build_passing_results


def extract_attributes_from_path(_path: str) -> Dict[str, Any]:
    """searches the path for job attributes"""
    result = os.path.normpath(_path).split(os.sep)
    if len(result) < 14:
        result.insert(-2, "none")

    results = {
        "branch": result[-9],
        "host": result[-8],
        "compiler": result[-7],
        "c_version": result[-6],
        "o_g": result[-5],
        "mpi": result[-4],
        "m_version": result[-3].lower(),
    }
    return results


def is_build_passing(file_path: str) -> bool:
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
            if idx > 200:
                logging.warning("missing build result %s", file_path)
                break
        return False


def compile_test_results(
    matching_summaries: Set[str], build_passing_results: List[Dict[str, Any]], job: Job
) -> List[str]:
    """takes all of the gathered data and returns a list of the results"""
    test_results = []
    for _file in matching_summaries:

        test_result = fetch_test_results(_file)
        build_result = fetch_build_result(test_result, build_passing_results)

        test_results.append(
            {**test_result, "branch": job.branch_name, "build_passed": build_result}
        )
    return test_results


def fetch_test_results(file_path: str) -> Dict[str, Any]:
    """Fetches test results from file_path and returns them as an ordered dict"""
    _temp = {}
    results = collections.OrderedDict()
    with open(file_path, "r", encoding="utf-8") as _file:
        for line in _file:
            # Build for = gfortran_10.3.0_mpich3_g_develop, mpi version 8.1.7 on acorn esmf_os: Linux
            if "Build for" in line:
                line_cleaned = line.split("=", 1)[1].strip()
                group1, group2 = line_cleaned.split(",")

                (
                    group1,
                    group2,
                ) = line_cleaned.split(",")

                (
                    _temp["compiler"],
                    _temp["c_version"],
                    _temp["mpi"],
                    _temp["o_g"],
                ) = group1.strip().split("_")[0:4]

                (_temp["branch"],) = group1.strip().split("_", 4)[4:]

                (
                    _,
                    _,
                    _temp["m_version"],
                    _,
                    _temp["host"],
                    _,
                    _temp["os"],
                ) = group2.strip().split(" ")

                # Keeps the order of insertion for printing
                results["branch"] = _temp["branch"]
                results["host"] = _temp["host"]
                results["compiler"] = _temp["compiler"]
                results["c_version"] = _temp["c_version"]
                results["mpi"] = _temp["mpi"]
                results["m_version"] = _temp["m_version"]
                results["o_g"] = _temp["o_g"]
                results["os"] = _temp["os"]

            if "test results" in line:

                key, value = line.split("\t", 1)
                key_cleaned = key.split(" ", 1)[0]

                try:
                    value.replace("PASS", "").replace("FAIL", "").replace(
                        "\n", ""
                    ).strip()
                    pass_, fail_ = value.split(" ", 1)
                    pass_ = (
                        int(pass_.replace("\n", "").strip())
                        # .replace("-1", "queued")
                    )
                    fail_ = (
                        int(fail_.replace("\n", "").strip())
                        # .replace("-1", "queued")
                    )
                    results[f"{key_cleaned}_pass"] = pass_
                    results[f"{key_cleaned}_fail"] = fail_

                except ValueError as _:
                    logging.error(
                        "found no numeric %s test results using default [%s]",
                        key_cleaned,
                        file_path,
                    )
                    results[f"{key_cleaned}_pass"] = "fail"
                    results[f"{key_cleaned}_fail"] = "fail"

    return results


def fetch_build_result(needle: Dict[str, Any], haystack: List[Dict[str, Any]]):
    """searches through they haystack for the needle"""
    try:
        result = filter(
            lambda x: x["compiler"].lower() == needle["compiler"].lower()
            and x["c_version"].lower() == needle["c_version"].lower()
            and x["mpi"].lower() == needle["mpi"].lower()
            and x["m_version"].lower() == needle["m_version"].lower()
            and x["host"].lower() == needle["host"].lower(),
            haystack,
        )
        return list(result)[0]["build_passed"]
    except IndexError as _:
        logging.warning("build result not found")
        logging.debug("needle: %s", needle)
        return False


def generate_link(**kwds) -> str:
    """generates a link to github to jump to the _hash passed in"""
    return f"[artifacts](https://github.com/esmf-org/esmf-test-artifacts/tree/{kwds['host'].replace('/', '_')}/{kwds['branch'].replace('/', '_')}/{kwds['host'].replace('/', '_')}/{kwds['compiler']}/{kwds['c_version']}/{kwds['o_g']}/{kwds['mpi']}/{kwds['m_version'].lower()})"


def get_branch_hashes(job, git) -> List[Any]:
    """Uses git log to determine all unique hashes for a branch_name/[machine_name]"""
    result = git.log(f"origin/{job.machine_name}")

    pattern = r"ESMF.*-\S{8}"
    _stdout = list(
        line.strip()
        for line in result.stdout.split("\n")
        if sanitize_branch_name(job.branch_name) in line and job.machine_name in line
    )

    return to_unique(
        re.findall(pattern, item)[0]
        for item in _stdout
        if len(re.findall(pattern, item)) > 0
    )


class Error(Exception):
    """Base class for other exceptions"""


class HashNotFoundError(Error):
    "Raised when no branch has is found"
