"""
Model that represents a Summary Table Row
"""
from typing import Any, Dict
from src import constants, file


class SummaryRow:
    """represents a row in the summary table"""

    INSERTION_ORDER = [
        "branch",
        "host",
        "compiler",
        "c_version",
        "mpi",
        "m_version",
        "o_g",
        "os",
        "u_pass",
        "u_fail",
        "s_pass",
        "s_fail",
        "e_pass",
        "e_fail",
        "nuopc_pass",
        "nuopc_fail",
        "build",
        "netcdf_c",
        "netcdf_f",
        "artifacts_hash",
        "branch_hash",
        "modified",
    ]
    # Order used when calling format().  Preserves order.
    # Columns not listed here are not produced in ouput.
    OUTPUT_ORDER = [
        "branch",
        "host",
        "compiler",
        "mpi",
        "netcdf",
        "o_g",
        "os",
        "build",
        "u_pass",
        "u_fail",
        "s_pass",
        "s_fail",
        "e_pass",
        "e_fail",
        "nuopc_pass",
        "nuopc_fail",
        "artifacts_hash",
        "modified",
    ]

    def __init__(self, row: Dict[str, Any]):
        self.row = row

    def __getitem__(self, key):
        return self.row.get(key, None)

    def __getattr__(self, __name: str) -> Any:
        if __name not in self.row.keys():
            raise AttributeError(f"attribute not found [{__name}]")
        return self.__getitem__(__name)

    def __iter__(self):
        return iter(self.row.keys())

    def formatted(self):
        """returns formatted dict on summary output from database"""
        # replace queued value with "pending"
        parsed_row = {
            k: "pending" if v == constants.QUEUED else v
            for k, v in self.ordered().items()
            if k in self.OUTPUT_ORDER
        }
        # replace 1/0 with pass/fail
        parsed_row["build"] = "pass" if self.build == constants.PASS else "fail"

        # format item and versions into one row
        parsed_row["compiler"] = f"{self.compiler}/{self.c_version}"
        parsed_row["mpi"] = f"{self.mpi}/{self.m_version}"

        # concat netcdf versions
        parsed_row["netcdf"] = f"{self.netcdf_c} {self.netcdf_f}"

        # generate link for github
        parsed_row["artifacts_hash"] = file.generate_link(
            hash=self.artifacts_hash, path=self.relative_path
        )
        return parsed_row

    @property
    def relative_path(self):
        """git relative path for hyperlinks"""
        return f"{self.branch}/{self.host}/{self.compiler}/{self.c_version}/{self.o_g}/{self.mpi}/{self.m_version}"

    def ordered(self):
        """returns dict ordered by self.KEY_ORDER"""
        return {key: self[key] for key in self.OUTPUT_ORDER}

    def _for_db(self):
        """returns ordered values for db insertion"""
        return [self[x] for x in self.INSERTION_ORDER]
