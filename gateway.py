import hashlib
import datetime
import sqlite3
from collections import namedtuple
from typing import Any, List
import abc


SummaryRow = namedtuple(
    "SummaryRow",
    "branch, host, compiler_type, compiler_version, mpi_type, mpi_version, o_g, os, unit_pass, unit_fail, system_pass, system_fail, example_pass, example_fail, nuopc_pass, nuopc_fail, build_passed, hash, modified, id",
)

SummaryRowFormatted = namedtuple(
    "SummaryRowFormatted",
    "branch, host, compiler_type, compiler_version, mpi_type, mpi_version, o_g, os, build_passed, unit_pass, unit_fail, system_pass, system_fail, example_pass, example_fail, nuopc_pass, nuopc_fail, hash, modified",
)


class Database(abc.ABC):
    @abc.abstractmethod
    def create_table(self):
        raise NotImplementedError

    def insert_rows(self):
        raise NotImplementedError

    def fetch_rows_by_hash(self):
        raise NotImplementedError


class Archive(Database):
    def __init__(self, db_path):
        self.con = sqlite3.connect(db_path)

    def create_table(self):
        cur = self.con.cursor()
        cur.execute(
            """CREATE TABLE if not exists Summaries (branch, host, compiler_type, compiler_version, mpi_type, mpi_version, o_g, os, unit_pass, unit_fail, system_pass, system_fail, example_pass, example_fail, nuopc_pass, nuopc_fail, build_passed, hash, modified DATETIME DEFAULT CURRENT_TIMESTAMP, id PRIMARY KEY)"""
        )
        cur.execute("""CREATE INDEX if not exists summary_id_idx ON Summaries (id)""")
        self.con.commit()

    def insert_rows(self, data: List[Any], _hash):
        rows = toSummaryRows(data, _hash, modified=datetime.datetime.now())
        cur = self.con.cursor()
        cur.executemany(
            "INSERT OR REPLACE INTO summaries VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        self.con.commit()

    def fetch_rows_by_hash(self, _hash: str):
        cur = self.con.cursor()
        cur.execute(
            """SELECT branch, host, compiler_type, compiler_version, mpi_type, mpi_version, o_g, os, build_passed, unit_pass, unit_fail, system_pass, system_fail, example_pass, example_fail, nuopc_pass, nuopc_fail, hash, modified FROM Summaries WHERE hash = ?""",
            (_hash,),
        )
        return (SummaryRowFormatted(*item) for item in cur.fetchall())


def toSummaryRow(item, _hash, modified):
    return SummaryRow(
        **item, hash=_hash, modified=modified, id=generate_id(item, _hash)
    )


def toSummaryRows(data, _hash, modified):
    return (toSummaryRow(item, _hash, modified) for item in data)


def generate_id(item, _hash):
    """generate an md5 hash from unique row data"""
    return hashlib.md5(
        f"{item['branch']}{item['host']}{item['os']}{item['compiler_type']}{item['compiler_version']}{item['mpi_type']}{item['mpi_version'].lower()}{item['o_g']}{_hash}".encode()
    ).hexdigest()
