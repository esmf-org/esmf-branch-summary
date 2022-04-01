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


from src.gateway.row import SummaryRow


SummaryRowData = collections.namedtuple(
    "SummaryRowData",
    "branch, host, compiler, c_version, mpi, m_version, o_g, os, unit_pass, unit_fail, system_pass, system_fail, example_pass, example_fail, nuopc_pass, nuopc_fail, build_passed, netcdf_c, netcdf_f, artifacts_hash, branch_hash, modified",
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

    def __init__(self, db_path: pathlib.Path):
        self.con = sqlite3.connect(str(db_path))
        self.db_path = db_path

    def create_table(self):
        cur = self.con.cursor()
        cur.execute(
            """CREATE TABLE if not exists Summaries (branch, host, compiler, c_version, mpi, m_version, o_g, os, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, build, netcdf_c, netcdf_f, artifacts_hash, branch_hash, modified DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (branch, host, compiler, compiler, c_version, mpi, m_version, o_g, os))"""
        )
        cur.execute(
            """CREATE INDEX if not exists summary_branch_hash_idx ON Summaries (branch_hash)"""
        )
        self.con.commit()

    def insert_rows(self, data: List[Dict[str, Any]]) -> int:
        self.create_table()

        rows = list(SummaryRowData(**row) for row in data)
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

    def fetch_rows_by_branch(self):
        cur = self.con.cursor()
        cur.execute(
            """SELECT branch, host, compiler, c_version, mpi, m_version, o_g, os, build, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, netcdf_c, netcdf_f, artifacts_hash, modified FROM Summaries ORDER BY branch, host, compiler, c_version, mpi, m_version, o_g"""
        )
        columns = list(x[0] for x in cur.description)
        return (SummaryRow(dict(zip(columns, values))) for values in cur.fetchall())

    def fetch_all_branch_hashes_by_branch_name(self, branch_name):
        cur = self.con.cursor()
        cur.execute(
            """select DISTINCT(branch_hash) from Summaries where branch = ?""",
            (branch_name,),
        )
        return cur.fetchall()


def to_summary_row(item: Dict[str, Any]):
    """converts dict to SummaryRow"""
    return SummaryRowData(**item)


def to_summary_rows(
    data: List[Dict[str, Any]]
) -> Generator[SummaryRowData, None, None]:
    """returns a generator of summary rows"""
    return (to_summary_row(item) for item in data)
