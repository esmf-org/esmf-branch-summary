"""
stats.py

Statistics convenience class

author: Ryan Long <ryan.long@noaa.gov>
"""


import collections
import datetime
import typing

from src.gateway.database_sqlite3 import DatabaseSqlite3


_Hash = str


class Stats(DatabaseSqlite3):
    """persists data to a sqlite3 database"""

    def fetch_last_hash(self) -> typing.Tuple[_Hash, datetime.datetime]:
        """returns tuple of last hash and last modified"""
        cur = self.con.cursor()
        cur.execute(
            "select hash, modified from summaries where branch = 'develop' order by modified desc limit 1;",
        )
        _hash, modified = cur.fetchone()
        return (_hash, datetime.datetime.fromtimestamp(modified))

    def fetch_build_passing_results(self, _hash: _Hash) -> typing.Tuple[int, int]:
        """returns number passing build results for hash"""
        cur = self.con.cursor()
        cur.execute(
            "select count(*), (select count(*) from Summaries where hash = ?) from Summaries where hash = ? and build = 1",
            (_hash, _hash),
        )
        return cur.fetchone()

    def fetch_build_failing_results(self, _hash: _Hash) -> typing.Tuple[int, int]:
        """returns number failing build results for hash"""
        cur = self.con.cursor()
        cur.execute(
            "select count(*), (select count(*) from Summaries where hash = ?) from Summaries where hash = ? and build = 0",
            (_hash, _hash),
        )
        return cur.fetchone()

    def fetch_build_success_pct(self, _hash: _Hash) -> typing.Tuple[float, float]:
        """returns the percentage of pass/fail"""
        BuildSuccess = collections.namedtuple("BuildSuccess", "p f")
        _pass, total = self.fetch_build_passing_results(_hash)
        _fail, total = self.fetch_build_failing_results(_hash)
        return BuildSuccess(_pass / total, _fail / total)
