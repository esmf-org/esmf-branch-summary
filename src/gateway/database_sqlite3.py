import abc
from pathlib import Path
import sqlite3


class DatabaseSqlite3(abc.ABC):
    """Sqlite3 abstract abstract"""

    def __init__(self, db_path: Path):
        self.con: sqlite3.Connection = sqlite3.connect(db_path)
