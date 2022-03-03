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
from typing import List

from src.git import Git

ESMF_BRANCH_SUMMARY = "git@github.com:esmf-org/esmf-branch-summary.git"
ESMF_TEST_SCRIPTS = "/home/rlong/Sandbox/esmf-test-scripts"

root = pathlib.Path(__file__).parent.resolve()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    filename=os.path.join(root, f"{os.path.basename(__file__)[:-3]}.log"),
    filemode="w",
)


Machine = collections.namedtuple(
    "Machine", ["machine_name", "hostname_pattern", "update_paths"]
)


def load_config(_path: pathlib.Path):
    """loads json configuration"""
    with open(_path, "r") as _file:
        return json.load(_file)


def hostname() -> str:
    """returns os hostname"""
    logging.debug("fetching hostname from os")
    return "cheyenne3"
    # return socket.gethostname()


def current_machine(_hostname: str, machines: List[Machine]) -> Machine:
    """returns current machine name"""
    for machine in machines:
        pattern = re.compile(machine.hostname_pattern)
        if pattern.match(_hostname):
            return machine
    raise SystemError(f"could not match hostname[{_hostname}]")


CONFIG_PATH = pathlib.Path("./update_repos.json")


def main():
    """main execution"""

    logging.info("starting...")
    logging.debug("loading config [%s]", CONFIG_PATH)
    config = load_config(CONFIG_PATH)
    machines = [Machine(**x) for x in config["machines"]]

    for _path in current_machine(hostname(), machines).update_paths:
        logging.debug("pulling [%s]", _path)
        Git(_path).pull()

    logging.info("finished")


if __name__ == "__main__":
    main()
