# pylint: skip-file

from src import git, job
from unittest.mock import MagicMock


def test_example():
    assert True


def test_fetch_branches():
    class MockClz:
        """mock"""

        def __init__(self, data):
            self.data = data

        @property
        def stdout(self):
            return self.data

    clz = None
    with open("./tests/fixtures/git_lot.txt") as _file:
        clz = MockClz(_file.read())

    _git = git.Git()
    _git.log = MagicMock(return_value=clz)

    _job = job.JobRequest(machine_name="cheyenne", branch_name="develop", qty=5)

    expected = [
        "ESMF_8_3_0_beta_snapshot_06-10-gce27d44",
        "ESMF_8_3_0_beta_snapshot_06-9-gd3f8b21",
        "ESMF_8_3_0_beta_snapshot_06-7-gdaedd10",
        "ESMF_8_3_0_beta_snapshot_06-1-ga2344ba",
        "ESMF_8_3_0_beta_snapshot_05-30-gf84ebe0",
    ]

    actual = job.get_branch_hashes(_job, _git)
    print(actual)
    assert actual == expected
