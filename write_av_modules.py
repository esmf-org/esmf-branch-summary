"""
Writes available modules to file.

author: Ryan Long <ryan.long@noaa.gov>
"""

import logging
import os
import pathlib
import socket
import subprocess

from src.git import Git


ROOT = pathlib.Path(__file__).parent.resolve()

LOG_FILE_PATH = pathlib.Path(ROOT / f"{os.path.basename(__file__)[:-3]}.log").resolve()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    filename=LOG_FILE_PATH,
    filemode="w",
)


def hostname() -> str:
    """returns os hostname"""
    logging.debug("fetching hostname from os")
    return socket.gethostname()


def module_av() -> subprocess.CompletedProcess:
    """_command_safe ensures commands are run safely and raise exceptions
    on error

    https://stackoverflow.com/questions/4917871/does-git-return-specific-return-error-codes
    """

    cmd = "module av"
    try:
        return subprocess.run(
            cmd,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            check=True,
            encoding="utf-8",
        )
    except subprocess.CalledProcessError as error:
        logging.info(error.stdout)
        return subprocess.CompletedProcess(returncode=0, args="", stdout=error.stdout)


if __name__ == "__main__":
    file_path = pathlib.Path(os.path.join(ROOT, f"MODULES_{hostname()}.md"))
    with open(file_path, "w", encoding="utf-8") as _file:
        _file.write(module_av().stdout)

        summary_repo = Git(ROOT)
        summary_repo.add(file_path, force=True)
        summary_repo.commit(f"update {file_path}")
        summary_repo.push(force=True)

        logging.info("finished")
