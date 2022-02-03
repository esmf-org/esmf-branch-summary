import collections
import functools
import itertools
import logging
import os
import re
import bisect
from typing import Generator, List, Tuple

from tabulate import tabulate

import esmf_branch_summary as ebs


def _replace(old: str, new: str, target: str):
    return target.replace(old, new)


sanitize_branch_name = functools.partial(_replace, "/", "_")

Job = collections.namedtuple("Job", ["machine_name", "branch_name", "qty"])


class JobProcessor:
    REPO_URL = "https://github.com/esmf-org/esmf"

    def __init__(
        self,
        machines: List[str],
        branches: List[str],
        history_increments: int,
        gateway: ebs.BranchSummaryGateway,
    ):

        self.machines = machines
        self._branches = branches
        self.history_increments = history_increments
        self.gateway = gateway

    @property
    def branches(self):
        if not self._branches:
            self._update_repo()
            self._branches = self.gateway.git.snapshot(self.REPO_URL)
        return self._branches

    def branches_sanitized(self) -> Generator[str, None, None]:
        return (sanitize_branch_name(branch) for branch in self.branches)

    def _update_repo(self):
        return self.gateway.git.fetch()

    @property
    def jobs(self) -> Generator[Job, None, None]:
        return (
            Job(machine_name, branch_name, self.history_increments)
            for machine_name, branch_name in generate_permutations(
                self.machines, self.branches
            )
        )

    def run_jobs(self):
        for job in self.jobs:
            os.chdir(self.gateway.compass.repopath)
            self.generate_summaries(job)
            self.gateway.git.push("origin", "summary")
            logging.info(
                "finished summaries for branch %s on machine %s",
                job.branch_name,
                job.machine_name,
            )

    def get_recent_branch_hashes(
        self,
        machine_name,
        branch_name,
        limit,
    ) -> Generator[str, None, None]:
        """Returns the most recent branch on machine_name + branch_name"""
        count = 0
        hashes = self.get_branch_hashes(machine_name, branch_name)
        while count < limit:
            try:
                yield next(hashes)
            except StopIteration:
                return
            count += 1

    def get_branch_hashes(
        self, machine_name, branch_name=None
    ) -> Generator[str, None, None]:
        """Uses git log to determine all hashes for a machine_name/(branch_name)"""
        result = self.gateway.git.log(f"origin/{machine_name}")

        _stdout = result.stdout.split("\n")
        pattern = r"ESMF.*-\S{8}"
        if branch_name is not None:
            _stdout = (line for line in _stdout if branch_name in line)
        return to_unique(
            re.findall(pattern, item)[0]
            for item in _stdout
            if len(re.findall(pattern, item)) > 0
        )

    def write_archive(self, data, _hash):
        """writes the provided data to the archive"""
        self.gateway.archive.create_table()
        self.gateway.archive.insert_rows(data, _hash)

    def generate_summary(self, _hash, branch_name):
        _hash = sanitize_branch_name(_hash)
        rootpath = str(self.gateway.compass.root)

        logging.debug("last branch hash is %s", _hash)

        logging.debug("fetching matching logs to determine build pass/fail")
        matching_logs = get_matching_logs(rootpath, _hash)

        logging.debug("fetching matching summary files to extract test results")
        matching_summaries = get_matching_summaries(
            str(self.gateway.compass.repopath), _hash
        )

        logging.debug("reading %s logs", len(matching_logs))
        build_passing_results = parse_logs_for_build_passing(matching_logs)
        logging.debug("finished reading logs")

        logging.debug("reading %d summaries", len(matching_summaries))

        return compile_test_results(
            matching_summaries, build_passing_results, branch_name
        )

    def generate_summaries(self, job: Job):
        self.gateway.git.checkout(job.machine_name)
        os.chdir(
            self.gateway.compass.get_branch_path(sanitize_branch_name(job.branch_name))
        )
        for _hash in self.get_recent_branch_hashes(
            job.machine_name, job.branch_name, job.qty
        ):
            self.generate_summary(_hash, job.branch_name)

            _hash = sanitize_branch_name(_hash)
            repopath = str(self.gateway.compass.repopath)
            rootpath = str(self.gateway.compass.root)

            logging.debug("last branch hash is %s", _hash)

            logging.debug("fetching matching logs to determine build pass/fail")
            matching_logs = get_matching_logs(rootpath, _hash)

            logging.debug("fetching matching summary files to extract test results")
            matching_summaries = get_matching_summaries(
                str(self.gateway.compass.repopath), _hash
            )

            logging.debug("reading %s logs", len(matching_logs))
            build_passing_results = parse_logs_for_build_passing(matching_logs)
            logging.debug("finished reading logs")

            logging.debug("reading %d summaries", len(matching_summaries))

            test_results = compile_test_results(
                matching_summaries, build_passing_results, job.branch_name
            )

            logging.debug("finished reading summaries")

            self.gateway.git.checkout(branch_name="summary", force=True)
            if len(test_results) > 0:
                if not os.path.exists(
                    os.path.join(repopath, job.branch_name.replace("/", "_"))
                ):
                    os.mkdir(os.path.join(repopath, job.branch_name.replace("/", "_")))
                output_file_path = os.path.abspath(
                    os.path.join(
                        repopath,
                        job.branch_name.replace("/", "_"),
                        f"{_hash.replace('/', '_')}.md",
                    )
                )
                self.gateway.archive.insert_rows(test_results, _hash)
                self.write_file(
                    _hash,
                    output_file_path,
                )

                logging.debug("adding all modified files in summary")
                self.gateway.git.add()

                logging.debug("committing to %s", "summary")
                self.gateway.git.commit(generate_commit_message(job.branch_name, _hash))

            logging.debug("pushing to summary")

            logging.info(
                "finished summary for B:%s M: %s [%s]",
                job.branch_name,
                job.machine_name,
                _hash,
            )

    def fetch_summary_file_contents(self, _hash: str):
        """fetches the contents to create a summary file based on _hash"""
        print(_hash)
        print(type(_hash))
        results = []
        for item in self.gateway.archive.fetch_rows_by_hash(_hash):
            row = item._asdict()
            row["hash"] = generate_link(**item._asdict())
            row["build_passed"] = "Pass" if row["build_passed"] == 1 else "Fail"
            results.append(dict(**row))
        return results

    def write_file(self, _hash, file_path):
        data = self.fetch_summary_file_contents(_hash)
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


def generate_permutations(list1, list2) -> Generator[Tuple, None, None]:
    """retuns list of tuples containing each permutation of the two lists"""
    return (each_permutation for each_permutation in itertools.product(list1, list2))


def generate_commit_message(branch_name, _hash):
    return f"updated summary for hash {_hash} on {branch_name}"


def get_matching_logs(cwd: str, _hash: str):
    return set(find_files(cwd, [_hash.replace("/", "_")], ["build.log"]))


def get_matching_summaries(cwd: str, _hash: str):
    return set(find_files(cwd, [_hash.replace("/", "_")], ["summary.dat"]))


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
                logging.debug("file: %s", file_path)
                logging.debug("\n".join(lines_read))
                break
        return False


def compile_test_results(matching_summaries, build_passing_results, branch_name):
    test_results = []
    for idx, _file in enumerate(matching_summaries):

        result = fetch_test_results(_file)
        pass_fail = fetch_build_result(result, build_passing_results)

        test_results.append(
            {**result, "branch": branch_name, "build_passed": pass_fail}
        )
        if idx % 10 == 0 and idx > 0:
            logging.debug("%d finished", idx)
        if idx >= len(matching_summaries) - 1:
            logging.debug("%d finished", idx + 1)
    return test_results


def fetch_test_results(file_path):
    """Fetches test results from file_path and returns them as an ordered dict"""
    _temp = {}
    results = collections.OrderedDict()
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

    return results


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


def generate_link(**kwds):
    """generates a link to github to jump to the _hash passed in"""
    return f"[artifacts](https://github.com/esmf-org/esmf-test-artifacts/tree/{kwds['host'].replace('/', '_')}/{kwds['branch'].replace('/', '_')}/{kwds['host'].replace('/', '_')}/{kwds['compiler_type']}/{kwds['compiler_version']}/{kwds['o_g']}/{kwds['mpi_type']}/{kwds['mpi_version'].lower()})"
