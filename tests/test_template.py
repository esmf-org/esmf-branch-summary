# pylint: disable=all
import esmf_branch_summary


def test():
    assert "function" in str(type(esmf_branch_summary.main))
    assert True
