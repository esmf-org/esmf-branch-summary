"""
gateway.py

Database interaction layer

author: Ryan Long <ryan.long@noaa.gov>
"""

import datetime
import sqlite3
import collections
from typing import Any, Dict, Generator, List
import abc


SummaryRow = collections.namedtuple(
    "SummaryRow",
    "branch, host, compiler, c_version, mpi, m_version, o_g, os, unit_pass, unit_fail, system_pass, system_fail, example_pass, example_fail, nuopc_pass, nuopc_fail, build_passed, artifacts_hash, branch_hash, modified",
)

SummaryRowFormatted = collections.namedtuple(
    "SummaryRowFormatted",
    "branch, host, compiler, c_version, mpi, m_version, o_g, os, build, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, artifacts_hash, modified",
)


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

    def __init__(self, db_path):
        self.con = sqlite3.connect(db_path)

    def create_table(self):
        cur = self.con.cursor()
        cur.execute(
            """CREATE TABLE if not exists Summaries (branch, host, compiler, c_version, mpi, m_version, o_g, os, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, build, artifacts_hash PRIMARY KEY, branch_hash, modified DATETIME DEFAULT CURRENT_TIMESTAMP)"""
        )
        cur.execute(
            """CREATE INDEX if not exists summary_branch_hash_idx ON Summaries (branch_hash)"""
        )
        self.con.commit()

    def insert_rows(self, data: List[Dict[str, Any]]) -> int:
        self.create_table()
        rows = list(
            to_summary_rows(
                data,
                modified=datetime.datetime.now().strftime("%m/%d/%Y_%H:%M:%S"),
            )
        )
        cur = self.con.cursor()
        cur.executemany(
            "INSERT OR REPLACE INTO summaries VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        self.con.commit()
        return cur.rowcount

    def fetch_rows_by_hash(self, _hash: str):
        cur = self.con.cursor()
        cur.execute(
            """SELECT branch, host, compiler, c_version, mpi, m_version, o_g, os, build, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, artifacts_hash, modified FROM Summaries WHERE branch_hash = ?""",
            (str(_hash),),
        )
        return (SummaryRowFormatted(*item) for item in cur.fetchall())


def to_summary_row(item: Dict[str, Any], modified: str):
    """converts dict to SummaryRow"""
    return SummaryRow(**item, modified=modified)


def to_summary_rows(
    data: List[Dict[str, Any]], modified: str
) -> Generator[SummaryRow, None, None]:
    """returns a generator of summary rows"""
    return (to_summary_row(item, modified) for item in data)


# def generate_id(item: Dict[str, Any]) -> str:
#     """generate an md5 hash from unique row data"""
#     return hashlib.md5(
#         f"{item['branch']}{item['host']}{item['os']}{item['compiler']}{item['c_version']}{item['mpi']}{item['m_version'].lower()}{item['o_g']}{item['hash']}".encode()
#     ).hexdigest()
