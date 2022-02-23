"""
compass.py

Utility class for navigating directories from a root

author: Ryan Long <ryan.long@noaa.gov>
"""

import os
import pathlib
import logging

logging.getLogger(__name__)


class Compass:
    """keeps track of important directories in the file structure"""

    def __init__(self, _root: pathlib.Path, repopath: pathlib.Path):
        self.root = _root
        self.repopath = pathlib.Path(os.path.join(self.root, repopath))

    @property
    def archive_path(self) -> pathlib.Path:
        """where the local database file is written to"""
        return pathlib.Path(os.path.join(self.root, "summaries.db"))

    @classmethod
    def from_path(cls, root: pathlib.Path, repopath: pathlib.Path) -> "Compass":
        """root usually __file__"""
        return Compass(pathlib.Path(root).parent.resolve(), repopath)

    def get_branch_path(self, branch_name):
        """returns the absolute path of branch_name"""
        return _mkdir_if_not_exists(
            pathlib.Path(os.path.join(self.root, self.repopath, branch_name))
        )


def _mkdir_if_not_exists(_path: pathlib.Path) -> pathlib.Path:
    if not os.path.exists(_path):
        os.mkdir(_path)
    return _path
