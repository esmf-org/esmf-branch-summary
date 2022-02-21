"""
gateway.py

Database interaction layer

author: Ryan Long <ryan.long@noaa.gov>
"""


import collections
import hashlib
import time
from typing import Any, Dict, Generator, List

from src.gateway.database_sqlite3 import DatabaseSqlite3

SummaryRow = collections.namedtuple(
    "SummaryRow",
    "branch, host, compiler, c_version, mpi, m_version, o_g, os, unit_pass, unit_fail, system_pass, system_fail, example_pass, example_fail, nuopc_pass, nuopc_fail, build_passed, hash, modified, id",
)

SummaryRowFormatted = collections.namedtuple(
    "SummaryRowFormatted",
    "branch, host, compiler, c_version, mpi, m_version, o_g, os, build, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, hash, modified",
)

TableId = str


class Summaries(DatabaseSqlite3):
    """persists data to a sqlite3 database"""

    TABLE_NAME: str = "Summaries"

    def create_table(self):
        """creates table if doesnt exist
        I'm using an f-string to create the table because ? interpolation didn't work
        when creating.
        """
        cur = self.con.cursor()
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                branch,
                host,
                compiler,
                c_version,
                mpi,
                m_version,
                o_g, os,
                u_pass,
                u_fail,
                s_pass,
                s_fail,
                e_pass,
                e_fail,
                nuopc_pass,
                nuopc_fail,
                build,
                hash,
                modified DATETIME DEFAULT CURRENT_TIMESTAMP,
                id PRIMARY KEY
                )"""
        )
        cur.execute("""CREATE INDEX if not exists summary_id_idx ON Summaries (id)""")
        self.con.commit()

    def insert_rows(self, data: List[Dict[str, Any]], _hash) -> None:
        self.create_table()
        rows = to_summary_rows(data, _hash, modified=time.time())
        cur = self.con.cursor()
        cur.executemany(
            "INSERT OR REPLACE INTO summaries VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        self.con.commit()

    def fetch_rows_by_hash(
        self, _hash: str
    ) -> Generator[SummaryRowFormatted, None, None]:
        cur = self.con.cursor()
        cur.execute(
            "SELECT branch, host, compiler, c_version, mpi, m_version, o_g, os, build, u_pass, u_fail, s_pass, s_fail, e_pass, e_fail, nuopc_pass, nuopc_fail, hash, modified FROM Summaries WHERE hash = ? ",
            (_hash,),
        )
        return (SummaryRowFormatted(*item) for item in cur.fetchall())


def to_summary_row(item: Dict[str, Any], _hash: str, modified: float) -> SummaryRow:
    """converts dict to SummaryRow"""
    return SummaryRow(
        **item, hash=_hash, modified=modified, id=generate_id(item, _hash)
    )


def to_summary_rows(
    data: List[Dict[str, Any]], _hash: str, modified: float
) -> Generator[SummaryRow, None, None]:
    """returns a generator of summary rows"""
    return (to_summary_row(item, _hash, modified) for item in data)


def generate_id(item: Dict[str, Any], _hash) -> TableId:
    """generate an md5 hash from unique row data"""
    return hashlib.md5(
        f"{item['branch']}{item['host']}{item['os']}{item['compiler']}{item['c_version']}{item['mpi']}{item['m_version'].lower()}{item['o_g']}{_hash}".encode()
    ).hexdigest()
