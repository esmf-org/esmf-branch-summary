""" Constants """

# Symbols
QUEUED = -1
PASS = 1
FAIL = 0

# Defaults
DEFAULT_FILE_ENCODING = "ISO-8859-1"
DEFAULT_TEMP_SPACE_NAME = "esmf_branch_summary_space"

# Repositories
SUMMARIES_REPO = "git@github.com:esmf/esmf-test-summary.git"

# Machines
MACHINE_NAME_LIST = sorted(
    [
        "cheyenne",
        "hera",
        "orion",
        "jet",
        "gaea",
        "discover",
        "chianti",
        "acorn",
        "gaffney",
        "izumi",
        "koehr",
        "onyx",
    ]
)
