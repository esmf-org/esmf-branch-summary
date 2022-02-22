"""
Represents A Job, being all the attribtues, rules, and excutions in order
to get a summary results from a repository of test artifacts.

author: Ryan Long <ryan.long@noaa.gov>
"""

import bisect
import collections
import csv
import functools
import itertools
import logging
import os
import re
import shutil
import subprocess
from typing import Any, Dict, Generator, List, Set, Tuple

from tabulate import tabulate

from src import constants


def _replace(old: str, new: str, target: str):
    return target.replace(old, new)


sanitize_branch_name = functools.partial(_replace, "/", "_")


TestResult = collections.namedtuple(
    "TestResult",
    [
        "branch",
        "host",
        "compiler",
        "c_version",
        "mpi",
        "m_version",
        "o_g",
        "os",
        "unit_pass",
        "unit_fail",
        "system_pass",
        "system_fail",
        "example_pass",
        "example_fail",
        "nuopc_pass",
        "nuopc_fail",
        "build_passed",
    ],
)

JobRequest = collections.namedtuple(
    "JobRequest", ["machine_name", "branch_name", "qty"]
)

JobAttributes = collections.namedtuple(
    "JobAttributes",
    ["branch", "host", "compiler", "c_version", "o_g", "mpi", "m_version"],
)


class JobProcessor:
    """aggregates and summarizes processing jobs"""

    REPO_URL = "https://github.com/esmf-org/esmf"

    def __init__(
        self,
        machines: List[str],
        branches: List[str],
        history_increments: int,
        _gateway,
    ):

        self.machines = machines
        self._branches = branches
        self.history_increments = history_increments
        self.gateway = _gateway

    def __iter__(self):
        return (x for x in self.jobs)

    def __repr__(self):
        class_name = type(self).__name__
        return f"<{class_name}>"

    @property
    def branches(self):
        """return branches to be summarized"""
        if not self._branches:
            self.gateway.git_artifacts.fetch()
            self._branches = self.gateway.git_artifacts.snapshot(self.REPO_URL)
        return self._branches

    @property
    def jobs(self) -> Generator[JobRequest, None, None]:
        """returns a generator of job to be summarized"""
        return (
            JobRequest(machine_name, branch_name, self.history_increments)
            for machine_name, branch_name in generate_permutations(
                self.machines, self.branches
            )
        )

    def branch_path(self, job: JobRequest, _root: str, force: bool):
        """returns the branch path in context.  Will create if force is True"""
        branch_path = os.path.join(
            _root,
            sanitize_branch_name(job.branch_name),
        )
        if force and not os.path.exists(branch_path):
            logging.debug("creating directory %s", branch_path)
            os.mkdir(branch_path)
        return branch_path

    def copy_files_to_repo_path(self, files: List[str]) -> None:
        """copies local files to repopath"""

        for _file in files:
            shutil.copyfile(
                os.path.join(self.gateway.compass.root, _file),
                os.path.join(self.gateway.git_summaries.repopath, _file),
            )

    def run_jobs(self) -> None:
        """runs the instance jobs"""
        self.gateway.git_summaries.clone(
            "git@github.com:esmf-org/esmf-test-summary.git",
            self.gateway.git_summaries.repopath,
        )
        self.gateway.git_summaries.repopath = os.path.join(
            self.gateway.git_summaries.repopath
        )
        for job in self.jobs:
            os.chdir(self.gateway.compass.repopath)
            self.generate_summaries(job)
            logging.info(
                "finished summaries for branch %s on machine %s",
                job.branch_name,
                job.machine_name,
            )
        logging.debug("pushing to summary")
        self.copy_files_to_repo_path(["esmf-branch-summary.log", "summaries.db"])
        self.gateway.git_summaries.add()
        self.gateway.git_summaries.commit("updating test artifacts")
        self.gateway.git_summaries.push("origin")

    def get_recent_branch_hashes(self, job: JobRequest) -> Generator[str, None, None]:
        """Returns the most recent branch on machine_name + branch_name"""
        hashes = list(get_branch_hashes(job, self.gateway.git_artifacts))
        for idx, _hash in enumerate(hashes):
            yield _hash
            if idx + 1 >= job.qty:
                return

    def write_archive(self, data: List[TestResult], _hash) -> None:
        """writes the provided data to the archive"""
        logging.debug("writing archive %s length %i", _hash, len(data))
        self.gateway.archive.create_table()
        self.gateway.archive.insert_rows([item._asdict() for item in data], _hash)

    def _verify_matches(
        self, matching_summaries: Set[str], matching_logs: Set[str], _hash: str
    ) -> None:
        """this method is soley for additional verification and should be removed"""
        if not matching_summaries and not matching_logs:
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
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
            )
            if results.stdout != "":
                logging.error(
                    "could not verify matches, grep returned %i ...\n[%s]",
                    len(results.stdout.splitlines()),
                    [x for x in results.stdout.splitlines()[:11]],
                )

    def generate_summary(self, _hash, job: JobRequest) -> List[TestResult]:
        """generates summary based on _hash and job and returns the results"""
        logging.debug("last branch hash is %s", _hash)

        matching_logs = get_matching_logs(
            str(self.gateway.compass.repopath), _hash, job
        )
        logging.debug("matching logs: %i", len(matching_logs))
        if matching_logs == 0:
            logging.warning(
                "no build.log found containing %s, no build data can be collected",
                _hash,
            )

        matching_summaries = get_matching_summaries(
            str(self.gateway.compass.repopath), _hash, job
        )
        if matching_summaries == 0:
            logging.warning(
                "no summary.dat found containing %s; no test data can be collected",
                _hash,
            )
        logging.debug("matching summaries: %i", len(matching_summaries))

        # TODO Remove after sending to prod
        self._verify_matches(matching_summaries, matching_logs, _hash)

        build_passing_results = extract_build_passing_results(matching_logs)
        logging.debug("finished reading logs")

        return compile_test_results(matching_summaries, build_passing_results)

    def send_summary_to_repo(
        self,
        job: JobRequest,
        summary: List[TestResult],
        _hash: str,
        is_latest: bool = False,
    ) -> None:
        """sends the summary based on the job information to the remote repository"""
        logging.debug("checking out summary")
        branch_path = self.branch_path(job, self.gateway.git_summaries.repopath, True)
        output_file_path_prefix = os.path.abspath(os.path.join(branch_path, _hash))

        self.write_archive(summary, _hash)
        self.write_files(_hash, output_file_path_prefix, is_latest)

        logging.debug("adding all modified files in summary")
        self.gateway.git_summaries.add()

        logging.debug("committing to summary")
        self.gateway.git_summaries.commit(
            generate_commit_message(job.branch_name, _hash)
        )

        logging.info(
            "finished summary for B:%s M: %s [%s]",
            job.branch_name,
            job.machine_name,
            _hash,
        )

    def generate_summaries(self, job: JobRequest):
        """generates all the summaries for job"""
        logging.info(
            "generating summaries for %s [%s]", job.branch_name, job.machine_name
        )
        logging.debug("checking out %s", job.machine_name)
        self.gateway.git_artifacts.checkout(job.machine_name)
        logging.debug("pulling from %s", job.machine_name)
        self.gateway.git_artifacts.pull()
        os.chdir(
            self.gateway.compass.get_branch_path(sanitize_branch_name(job.branch_name))
        )
        for idx, _hash in enumerate(self.get_recent_branch_hashes(job)):
            summary = self.generate_summary(_hash, job)
            if len(summary) == 0:
                logging.info(
                    "missing summary data for %s, %s [%s]",
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
            # replace -1 with "pending"
            row = {
                k: "pending" if v == constants.QUEUED else v
                for k, v in item._asdict().items()
            }
            row["hash"] = generate_link(**row)
            row["build"] = "Pass" if row["build"] == constants.PASS else "Fail"
            results.append(dict(**row))
        return sort_file_summary_content(results)

    def write_files(self, _hash: str, file_path: str, is_latest: bool = False):
        """writes all file types required to disk"""
        logging.debug("writing files %s", file_path)
        data = self.fetch_summary_file_contents(_hash)

        _dir = os.path.dirname(file_path)
        if not os.path.exists(_dir):
            os.makedirs(_dir)

        if is_latest is True:
            write_file_latest(data, file_path)
        write_file_md(data, file_path)
        write_file_csv(data, file_path)


def write_file_md(data: List[Dict[str, str]], file_path: str) -> None:
    """writes markdown file"""
    logging.debug("writing file md: %s", file_path)
    table = tabulate(data, headers="keys", showindex="always", tablefmt="github")
    with open(file_path + ".md", "w+", newline="") as _file:
        _file.write(table)


def write_file_csv(data: List[Dict[str, str]], file_path: str) -> None:
    """writes csv file"""
    logging.debug("writing file csv[%i]: %s", len(data), file_path)
    with open(file_path + ".csv", "w+", newline="") as csv_file:
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
    with open(latest_file_path, "w+", newline="") as _file:
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


def get_matching_logs(cwd: str, _hash: str, job: JobRequest) -> Set[str]:
    """finds the build.log files"""
    logging.debug("fetching matching logs to determine build pass/fail")
    return set(
        find_files(
            cwd,
            [_hash],
            ["build.log", sanitize_branch_name(job.branch_name), job.machine_name],
            ["module", "python", "swp"],
        )
    )


def get_matching_summaries(cwd: str, _hash: str, job: JobRequest) -> Set[str]:
    """finds the summary.dat files"""
    logging.debug("fetching matching summaries to extract test results")
    return set(
        find_files(
            cwd,
            [_hash],
            ["summary.dat", sanitize_branch_name(job.branch_name), job.machine_name],
            ["swp"],
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


def extract_build_passing_results(log_paths: Set[str]) -> Dict[JobAttributes, bool]:
    """searches through logs to find build_passing results

    JobAttributes namedtuple is immutable so it can be used as a dict key
    """
    return {fetch_job_attributes(_file): is_build_passing(_file) for _file in log_paths}


def fetch_job_attributes(_path: str) -> JobAttributes:
    """returns job attributes based on position in path"""
    result = os.path.normpath(_path).split(os.sep)
    return JobAttributes(
        *[result[x].lower().replace("out", "") for x in range(-9, -2, 1)]
    )


def is_build_passing(file_path: str) -> bool:
    """Determines if the build is passing by scanning file_path"""
    if not os.path.exists(file_path):
        logging.error("file path does not exist [%s]", file_path)
        return False
    with open(file_path, "r", encoding="utf-8") as _file:
        lines_read = []
        for idx, line in enumerate(reversed(list(_file))):
            if "ESMF library built successfully" in line:
                return True
            lines_read.append(line)
            # Check the last 200 lines only for speed
            if idx > 200:
                logging.debug(
                    "success message not found in file [%s]",
                    file_path,
                )
                return False

        return False


def compile_test_results(
    matching_summaries: Set[str],
    build_passing_results: Dict[JobAttributes, Any],
) -> List[TestResult]:
    """takes all of the gathered data and returns a list of the results"""
    return [
        TestResult(
            **fetch_test_results(_file),
            build_passed=fetch_build_result(
                fetch_test_results(_file), build_passing_results
            ),
        )
        for _file in matching_summaries
    ]


def extract_build_attributes(line, file_path) -> Dict[str, Any]:
    """extracs build atrributes when found in file_path"""
    _temp = {}
    results = collections.OrderedDict()
    line_cleaned = line.split("=", 1)[1].strip()
    group1, group2 = line_cleaned.split(",", 1)
    try:

        (
            group1,
            group2,
        ) = line_cleaned.split(",", 1)

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
        results["m_version"] = _temp["m_version"].lower()
        results["o_g"] = _temp["o_g"]
        results["os"] = _temp["os"]

        return results

    except ValueError:
        logging.error("group1: %s", group1)
        logging.error("group2: %s", group2)
        logging.error("file_path: %s", file_path)
        logging.error("could not split %s on ", line_cleaned)
        raise


def extract_test_results(line, file_path, results) -> Dict[str, Any]:
    """extracts test results in a line of text and appends those values to results"""

    def clean_value(value):
        delete_carriage_returns = functools.partial(_replace, "\n", "")
        return delete_carriage_returns(
            value.replace("PASS", "").replace("FAIL", "")
        ).strip()

    key, value = line.split("\t", 1)
    key_cleaned = key.split(None, 1)[0]

    try:
        value = clean_value(value)
        pass_, fail_ = value.split(None, 1)
        pass_ = int(pass_.strip())
        fail_ = int(fail_.strip())

        results[f"{key_cleaned}_pass"] = pass_
        results[f"{key_cleaned}_fail"] = fail_
    except ValueError as err:
        logging.error(
            "found no numeric %s test results, setting to fail [%s]",
            key_cleaned,
            file_path,
        )
        logging.error("message: %s", err)
        logging.error("line being parsed: %s", value)
        results[f"{key_cleaned}_pass"] = "fail"
        results[f"{key_cleaned}_fail"] = "fail"
    return results


def fetch_test_results(file_path: str) -> Dict[str, Any]:
    """Fetches test results from file_path and returns them as an ordered dict"""

    def clean_value(value):
        delete_carriage_returns = functools.partial(_replace, "\n", "")
        return delete_carriage_returns(
            value.replace("PASS", "").replace("FAIL", "")
        ).strip()

    with open(file_path, "r", encoding="ISO-8859-1") as _file:
        results = {}
        for line in _file.readlines():
            # Build for = gfortran_10.3.0_mpich3_g_develop, mpi version 8.1.7 on acorn esmf_os: Linux
            if "Build for" in line:
                results = extract_build_attributes(line, file_path)

            if "test results" in line:

                key, value = line.split("\t", 1)
                key_cleaned = key.split(None, 1)[0]

                try:
                    value = clean_value(value)
                    pass_, fail_ = value.split(None, 1)
                    pass_ = int(pass_.strip())
                    fail_ = int(fail_.strip())

                    results[f"{key_cleaned}_pass"] = pass_
                    results[f"{key_cleaned}_fail"] = fail_

                except ValueError as err:
                    logging.error(
                        "found no numeric %s test results, setting to fail [%s]",
                        key_cleaned,
                        file_path,
                    )
                    logging.error("message: %s", err)
                    logging.error("line being parsed: %s", value)
                    results[f"{key_cleaned}_pass"] = "fail"
                    results[f"{key_cleaned}_fail"] = "fail"

    return results


def fetch_build_result(needle: Dict[str, Any], haystack: Dict[JobAttributes, Any]):
    """searches through they haystack for the needle"""
    data = {k: needle.get(k, "none").lower() for k in (JobAttributes._fields)}
    try:
        return haystack[JobAttributes(**data)]
    except KeyError:
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
    )[: job.qty]
