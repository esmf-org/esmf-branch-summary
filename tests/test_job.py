# pylint: skip-file

from src.job import UniqueList, Hash
from unittest.mock import MagicMock


def test_hash_parses_without_error_if_correct():
    hash = Hash("ESMF_8_3_0_beta_snapshot_07-g8913088")
    assert hash == "ESMF_8_3_0_beta_snapshot_07-g8913088"


def test_hash_pares_without_error_if_correct():
    hash = Hash("v8.3.0b07-12-g8913088")
    assert hash == "v8.3.0b07-12-g8913088"


def test_unique_list():
    items = [1, 2, 3, 3, 2, 1, 9, 8, 7, 7, 8, 9]
    l = UniqueList(items)
    assert l == [1, 2, 3, 9, 8, 7]


# def test_hash_paes_without_error_if_correct():
#     hash = job.JobHash("EF_8_3_0_beta_snapshot_07")
#     print(hash())
#     assert False
