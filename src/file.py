"""
Emulates file contenent reading and specialized files

author: Ryan Long <ryan.long@noaa.gov>
"""

import abc
import collections
import functools
import os
import pathlib
import logging
from typing import Any, Dict, Generator, List

from src import constants
from src.gateway.database import SummaryRowFormatted


def _replace(old: str, new: str, target: str):
    return target.replace(old, new)


class ReadOnly(abc.ABC):
    """read only file"""

    def __init__(self, file_path: pathlib.Path):
        self.file_path = file_path

    @property
    def content(self) -> Generator[str, None, None]:
        """line by line iterator"""
        with open(
            self.file_path, "r", encoding=constants.DEFAULT_FILE_ENCODING
        ) as _file:
            yield _file.readline()


class Summary(ReadOnly):
    """represents a summary.dat file"""

    PROPS = [
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
    ]

    def __init__(self, file_path: pathlib.Path):
        super().__init__(file_path)
        self.data = self.fetch_test_results()

    def __dir__(self):
        return list(self.__dict__.keys()) + self.PROPS

    def fetch_test_results(self) -> Dict[str, Any]:
        """Fetches test results from file_path and returns them as an ordered dict"""

        def clean_value(value):
            delete_carriage_returns = functools.partial(_replace, "\n", "")
            return delete_carriage_returns(
                value.replace("PASS", "").replace("FAIL", "")
            ).strip()

        results = {}
        for line in self.content:
            # Build for = gfortran_10.3.0_mpich3_g_develop, mpi version 8.1.7 on acorn esmf_os: Linux
            if "Build for" in line:
                results = extract_build_attributes(line)

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
                        self.file_path,
                    )
                    logging.error("message: %s", err)
                    logging.error("line being parsed: %s", value)
                    results[f"{key_cleaned}_pass"] = "fail"
                    results[f"{key_cleaned}_fail"] = "fail"
        return results

    def parse_summary_file_row(self, row: SummaryRowFormatted) -> Dict[str, Any]:
        """formats and replaces values for outputing to summary file"""
        parsed_row = {
            k: "pending" if v == constants.QUEUED else v
            for k, v in row._asdict().items()
        }
        parsed_row["build"] = "Pass" if row.build == constants.PASS else "Fail"
        parsed_row["artifacts_hash"] = generate_link(hash=row.artifacts_hash)
        return parsed_row


def generate_link(**kwds) -> str:
    """generates a link to github to jump to the _hash passed in"""
    return f"[artifacts]({constants.REPO_ESMF_TEST_ARTIFACTS}/tree/{kwds['hash']})"


def generate_link_old(**kwds) -> str:
    """generates a link to github to jump to the _hash passed in"""
    return f"[artifacts]({constants.REPO_ESMF_TEST_ARTIFACTS}/tree/{kwds['host'].replace('/', '_')}/{kwds['branch'].replace('/', '_')}/{kwds['host'].replace('/', '_')}/{kwds['compiler']}/{kwds['c_version']}/{kwds['o_g']}/{kwds['mpi']}/{kwds['m_version'].lower()})"


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


def extract_build_attributes(line: str) -> Dict[str, Any]:
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
        logging.error("could not split %s on ", line_cleaned)
        raise


class Build(ReadOnly):
    """represents a build.log file"""

    SUCCESS_MESSAGE = "ESMF library built successfully"

    def __init__(self, file_path: pathlib.Path):
        if not file_path.suffix == ".log":
            raise ValueError("")
        super().__init__(file_path)
        self.data = fetch_job_attributes(file_path)

    @property
    def is_build_passing(self) -> bool:
        """Determines if the build is passing by scanning file_path"""
        for idx, line in enumerate(self.content):
            if self.SUCCESS_MESSAGE in line:
                return True
            # Check the bottom 200 lines only for speed
            if idx > 200:
                logging.debug(
                    "success message not found in file [%s]",
                    self.file_path,
                )
                return False
        return False


def fetch_job_attributes(_path: pathlib.Path):
    """returns job attributes based on position in path"""
    result = os.path.normpath(_path).split(os.sep)
    return [result[x].lower().replace("out", "") for x in range(-9, -2, 1)]
