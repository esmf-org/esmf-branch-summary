import abc
import pathlib
import logging
from typing import Generator

from src import constants


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

    # def __init__(self, branch, host, compiler, c_version, o_g, mpi, mpi_version):
    #     self.branch = branch
    #     self.host = host
    #     self.compiler = compiler
    #     self.c_version = c_version
    #     self.o_g = o_g
    #     self.mpi = mpi
    #     self.mpi_version = mpi_version


class Build(ReadOnly):
    """represents a build.log file"""

    SUCCESS_MESSAGE = "ESMF library built successfully"

    def __init__(self, file_path: pathlib.Path):
        if not file_path.suffix == ".dat":
            raise ValueError("")
        super().__init__(file_path)

    def is_build_passing(self, file_path: pathlib.Path) -> bool:
        """Determines if the build is passing by scanning file_path"""
        for idx, line in enumerate(self.content):
            if self.SUCCESS_MESSAGE in line:
                return True
            # Check the bottom 200 lines only for speed
            if idx > 200:
                logging.debug(
                    "success message not found in file [%s]",
                    file_path,
                )
                return False
        return False
