"""
gateway.py

Database interaction layer

author: Ryan Long <ryan.long@noaa.gov>
"""

import pathlib
import sqlite3
import collections
import time
from typing import Any, Dict, Generator, List
import abc

from src import constants
from src import file


SummaryRowData = collections.namedtuple(
    "SummaryRowData",
    "branch, host, compiler, c_version, mpi, m_version, o_g, os, unit_pass, unit_fail, system_pass, system_fail, example_pass, example_fail, nuopc_pass, nuopc_fail, build_passed, netcdf_c, netcdf_f, artifacts_hash, branch_hash, modified",
)


class SummaryRow:
    """represents a row in the summary table"""

    INSERTION_ORDER = [
        "branch",
        "host",
        "compiler",
        "c_version",
        "mpi",
        "m_version",
        "o_g",
        "os",
        "u_pass",
        "u_fail",
        "s_pass",
        "s_fail",
        "e_pass",
        "e_fail",
        "nuopc_pass",
        "nuopc_fail",
        "build",
        "netcdf_c",
        "netcdf_f",
        "artifacts_hash",
        "branch_hash",
        "modified",
    ]
    KEY_ORDER = [
        "branch",
        "host",
        "compiler",
        "c_version",
        "mpi",
        "m_version",
        "o_g",
        "os",
        "netcdf_c",
        "netcdf_f",
        "build",
        "u_pass",
        "u_fail",
        "s_pass",
        "s_fail",
        "e_pass",
        "e_fail",
        "nuopc_pass",
        "nuopc_fail",
        "artifacts_hash",
        "modified",
    ]

    def __init__(self, row: Dict[str, Any]):
        self.row = row

    def __getitem__(self, key):
        return self.row.get(key, None)

    def __getattr__(self, __name: str) -> Any:
        return self.__getitem__(__name)

    def __iter__(self):
        return iter(self.row.keys())

    def formatted(self):
        """returns formatted dict on summary output from database"""
        # replace queued value with "pending"
        parsed_row = {
            k: "pending" if v == constants.QUEUED else v
            for k, v in self.ordered().items()
        }
        # replace 1/0 with Pass/Fail
        parsed_row["build"] = "Pass" if self.build_passed == constants.PASS else "Fail"
        # generate link for github
        parsed_row["artifacts_hash"] = file.generate_link(
            hash=self.artifacts_hash, path=self.relative_path
        )
        return parsed_row

    @property
    def relative_path(self):
        """git relative path for hyperlinks"""
        return f"{self.branch}/{self.host}/{self.compiler}/{self.c_version}/{self.o_g}/{self.mpi}/{self.m_version}"

    def ordered(self):
        """returns dict ordered by self.KEY_ORDER"""
        return {key: self[key] for key in self.KEY_ORDER}

    def _for_db(self):
        """returns ordered values for db insertion"""
        return [self[x] for x in self.INSERTION_ORDER]


class Database(abc.ABC):
    """Database abstract"""

    @abc.abstractmethod
    def create_table(self):
        """creates table"""
        raise NotImplementedError

    @abc.abstractmethod
    def insert_rows(self, data: List[Any]) -> int:
        """inserts rows"""
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_rows_by_hash(self, _hash):
        """fetchs rows by hash"""
        raise NotImplementedError


class Archive(Database):
    """persists data to a sqlite3 database"""

    def __init__(self, db_path: pathlib.Path):
        self.con = sqlite3.connect(str(db_path))

    def create_table(self):
        cur = self.con.cursor()
        cur.execute(
            """CREATE TABLE if not exists Summaries (branch, host, compiler, c_version, mpi, m_version, o_g, os, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, build, netcdf_c, netcdf_f, artifacts_hash PRIMARY KEY, branch_hash, modified DATETIME DEFAULT CURRENT_TIMESTAMP)"""
        )
        cur.execute(
            """CREATE INDEX if not exists summary_branch_hash_idx ON Summaries (branch_hash)"""
        )
        self.con.commit()

    def insert_rows(self, data: List[Dict[str, Any]]) -> int:
        self.create_table()

        rows = list(SummaryRowData(**row, modified=str(time.time())) for row in data)
        cur = self.con.cursor()
        cur.executemany(
            "INSERT OR REPLACE INTO summaries VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        self.con.commit()
        return cur.rowcount

    def fetch_rows_by_hash(self, _hash: str):
        cur = self.con.cursor()
        cur.execute(
            """SELECT branch, host, compiler, c_version, mpi, m_version, o_g, os, build, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, netcdf_c, netcdf_f, artifacts_hash, modified FROM Summaries WHERE branch_hash = ? ORDER BY branch, host, compiler, c_version, mpi, m_version, o_g""",
            (str(_hash),),
        )
        columns = list(x[0] for x in cur.description)
        return (SummaryRow(dict(zip(columns, values))) for values in cur.fetchall())


def to_summary_row(item: Dict[str, Any], modified: str):
    """converts dict to SummaryRow"""
    return SummaryRowData(**item, modified=modified)


def to_summary_rows(
    data: List[Dict[str, Any]], modified: str
) -> Generator[SummaryRowData, None, None]:
    """returns a generator of summary rows"""
    return (to_summary_row(item, modified) for item in data)
