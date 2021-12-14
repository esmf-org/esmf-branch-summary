# pylint: disable=all
import esmf_branch_summary as ebs

from unittest.mock import patch


class Dummy(object):
    pass


def test():
    assert "function" in str(type(ebs.main))


def testFindFilesContainingString_whenCalledWithFoundValue_ReturnsCorrectFiles():
    expected = 2
    actual = len(
        ebs.find_files(
            "ESMF_8_3_0_beta_snapshot_04-8-g60a38ef",
            ".\\tests\\fixtures\\summary_data_files",
            "summary.dat",
        )
    )
    assert actual == expected


def testFindFilesContainingString_whenCalledWithNotFoundValue_ReturnsCorrectFiles():
    expected = 0
    actual = len(
        ebs.find_files(
            "non_existant_hash_123",
            ".\\tests\\fixtures\\summary_data_files",
            "summary.dat",
        )
    )
    assert actual == expected


@patch("subprocess.run")
def testGetLastBranchHash_whenCalled_ReturnsLastBranchHash(run_mock):
    log = ""
    with open(".\\tests\\fixtures\\git_log\\git_log_sample.log") as _file:
        log = _file.read()

    o = Dummy()
    o.stdout = log
    run_mock.return_value = o
    expected = "ESMF_8_3_0_beta_snapshot_04-8-g60a38ef"
    actual = ebs.get_last_branch_hash("develop", "cheyenne")
    assert expected == actual
