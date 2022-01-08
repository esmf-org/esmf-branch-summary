# pylint: disable=all
import esmf_branch_summary as ebs

from unittest.mock import patch
import os


class Dummy:
    def __init__(self):
        self.stdout = ""
        pass


def test():
    assert "function" in str(type(ebs.main))


def testFindFilesContainingString_whenCalledWithFoundValue_ReturnsCorrectFiles():
    expected = 2
    result = ebs.find_files(
        os.path.join(os.getcwd(), "tests", "fixtures", "summary_data_files"),
        "ESMF_8_3_0_beta_snapshot_04-8-g60a38ef",
    )
    actual = len(result)

    assert actual == expected


def testFindFilesContainingString_whenCalledWithNotFoundValue_ReturnsCorrectFiles():
    expected = 0
    result = ebs.find_files(
        "non_existant_hash_123",
        os.path.join(
            os.getcwd(), "tests", "fixtures", "summary_data_files", "summary.dat"
        ),
    )
    actual = len(result)
    assert actual == expected


@patch("subprocess.run")
def testGetLastBranchHash_whenCalled_ReturnsLastBranchHash(run_mock):
    log = "tests/fixtures/git_log/git_log_sample.log"
    _path = os.path.join(
        os.getcwd(), "tests", "fixtures", "git_log", "git_log_sample.log"
    )
    with open(_path) as _file:
        log = _file.read()
    o = Dummy()
    o.stdout = log
    run_mock.return_value = o
    expected = "ESMF_8_3_0_beta_snapshot_04-8-g60a38ef"
    actual = ebs.get_last_branch_hash("develop", "cheyenne")
    assert expected == actual
