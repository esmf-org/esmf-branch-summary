""" Constants """

import os


is_prod = os.environ.get("ESMF_BRANCH_SUMMARIZER_ENV") == "PROD"

# Symbols
QUEUED = -1
PASS = 1
FAIL = 0
NA = "N/A"

# Defaults
DEFAULT_FILE_ENCODING = "ISO-8859-1"
DEFAULT_TEMP_SPACE_NAME = "esmf_branch_summary_space"

# Repositories
REPO_ESMF_TEST_ARTIFACTS = "https://github.com/esmf-org/esmf-test-artifacts"

# Repositories
_DEV_REPO = "git@github.com:ryanlong1004/esmf-test-summary.git"
_PROD_REPO = "git@github.com:esmf-org/esmf-test-summary.git"
REPO_ESMF_BRANCH_SUMMARY = _PROD_REPO if is_prod else _DEV_REPO

# Machines
MACHINE_NAME_LIST = sorted(
    [
        # "cori",
        # "cheyenne",
        "hera",
        # "orion",
        # "jet",
        # "gaea",
        # "discover",
        # "chianti",
        # "acorn",
        # "gaffney",
        # "izumi",
        # "koehr",
        # "onyx",
    ]
)
