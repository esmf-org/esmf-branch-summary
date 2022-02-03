import os, pathlib
import logging

logging.getLogger(__name__)


class Compass:
    def __init__(self, _root: pathlib.Path, repopath: pathlib.Path):
        self.root = _root
        self.repopath = repopath

    @property
    def archive_path(self):
        return os.path.join(self.root, "summaries.db")

    @classmethod
    def from_path(cls, root: pathlib.Path, repopath: pathlib.Path):
        """root usually __file__"""
        return Compass(pathlib.Path(root).parent.resolve(), repopath)

    def get_branch_path(self, branch_name):
        return mkdir_if_not_exists(
            os.path.abspath(os.path.join(self.repopath, branch_name))
        )


def mkdir_if_not_exists(_path):
    if not os.path.exists(_path):
        os.mkdir(_path)
    return _path
