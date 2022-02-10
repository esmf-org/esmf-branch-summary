import job
import git
from unittest.mock import MagicMock


def test_example():
    assert True


def test_fetch_branches():
    class MockClz:
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

    _job = job.Job(machine_name="cheyenne", branch_name="develop", qty=5)

    expected = [
        "ESMF_8_3_0_beta_snapshot_06-10-gce27d44",
        "ESMF_8_3_0_beta_snapshot_06-9-gd3f8b21",
        "ESMF_8_3_0_beta_snapshot_06-7-gdaedd10",
        "ESMF_8_3_0_beta_snapshot_06-1-ga2344ba",
        "ESMF_8_3_0_beta_snapshot_05-30-gf84ebe0",
        "ESMF_8_3_0_beta_snapshot_05-28-g1022def",
        "ESMF_8_3_0_beta_snapshot_05-4-gcfea0e5",
        "ESMF_8_3_0_beta_snapshot_05-3-g889e709",
        "ESMF_8_3_0_beta_snapshot_05-2-geb61916",
        "ESMF_8_3_0_beta_snapshot_05-1-g3e9d170",
        "ESMF_8_3_0_beta_snapshot_04-8-g60a38ef",
        "ESMF_8_3_0_beta_snapshot_04-7-gc7b07b1",
        "ESMF_8_3_0_beta_snapshot_03-5-g5fdc9bc",
        "ESMF_8_3_0_beta_snapshot_02-9-g35b0f61",
        "ESMF_8_3_0_beta_snapshot_02-3-gbcb5d21",
        "ESMF_8_3_0_beta_snapshot_02-1-gd594fca",
        "ESMF_8_3_0_beta_snapshot_00-1-ge61c3b7",
        "ESMF_8_2_0_beta_snapshot_20-18-g0aeed17",
        "ESMF_8_2_0_beta_snapshot_20-1-gb0d63f0",
    ]

    actual = job.get_branch_hashes(_job, _git)
    print(actual)
    assert actual == expected
