"""
Writes available modules to file.

author: Ryan Long <ryan.long@noaa.gov>
"""

import logging
import os
import pathlib
import subprocess


ROOT = pathlib.Path(__file__).parent.resolve()

LOG_FILE_PATH = pathlib.Path(ROOT / f"{os.path.basename(__file__)[:-3]}.log").resolve()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    filename=LOG_FILE_PATH,
    filemode="w",
)


def hostname() -> str:
    return subprocess.run('hostname', encoding="utf-8", stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout[:-1]


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
    with open(f"./MODULES_{hostname()}.md", "w", encoding="utf-8") as _file:
        _file.write(module_av().stdout)
