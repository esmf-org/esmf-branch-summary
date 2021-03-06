"""
Used to dynamically git pull repositories based on a JSON configuration.

author: Ryan Long <ryan.long@noaa.gov>
"""

import collections
import json
import logging
import os
import pathlib
import re
import socket
from typing import List
from src.constants import DEFAULT_FILE_ENCODING

from src.git import Git

ROOT = pathlib.Path(__file__).parent.resolve()

LOG_FILE_PATH = pathlib.Path(ROOT / f"{os.path.basename(__file__)[:-3]}.log").resolve()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    filename=LOG_FILE_PATH,
    filemode="w",
)

Machine = collections.namedtuple(
    "Machine", ["machine_name", "hostname_pattern", "update_paths"]
)


def load_config(_path: pathlib.Path):
    """loads json configuration"""
    with open(_path, "r", encoding=DEFAULT_FILE_ENCODING) as _file:
        return json.load(_file)


def hostname() -> str:
    """returns os hostname"""
    logging.debug("fetching hostname from os")
    return socket.gethostname()


def current_machine(_hostname: str, machines: List[Machine]) -> Machine:
    """returns current machine name"""
    for machine in machines:
        pattern = re.compile(machine.hostname_pattern)
        if pattern.match(_hostname):
            return machine
    raise SystemError(f"could not match hostname[{_hostname}]")


CONFIG_PATH = pathlib.Path(ROOT / "./update_repos.json").resolve()


def main():
    """main execution"""

    logging.info("starting...")
    logging.debug("loading config [%s]", CONFIG_PATH)
    config = load_config(CONFIG_PATH)
    machines = [Machine(**x) for x in config["machines"]]

    machine = current_machine(hostname(), machines)
    logging.debug("identified machine as [%s]", machine.machine_name)
    for _path in machine.update_paths:
        logging.debug("pulling [%s]", _path)
        Git(pathlib.Path(_path).resolve()).pull()
    summary_repo = Git(ROOT)
    summary_repo.add(ROOT / LOG_FILE_PATH, force=True)
    summary_repo.commit(f"update {LOG_FILE_PATH}")
    summary_repo.push(force=True)

    logging.info("finished")


if __name__ == "__main__":
    main()
