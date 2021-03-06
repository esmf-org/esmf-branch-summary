"""
hash.py

ESMF unique hash type

author: Ryan Long <ryan.long@noaa.gov>
"""

import collections
import re
from typing import List


class Hash(collections.UserString):
    """contains methods to parse and represent a job hash"""

    PARSE_PATTERNS: List[str] = [r"ESMF_\S*", r"v\S*\.\S*\.\S*"]

    def __init__(self, value: str):
        super().__init__(self._parse(value))

    def __str__(self):
        return str(self.data)

    def _parse(self, value) -> str:
        for pattern in self.PARSE_PATTERNS:
            try:
                return re.findall(pattern, value)[0]
            except IndexError:
                continue
        return ""

    def patterns(self) -> List[str]:
        """list of regex patterns used for extrapolating hash"""
        return self.PARSE_PATTERNS

    @property
    def git_prefix(self):
        """returns the git prefix at the end of the Hash string"""
        return self.rsplit("-", maxsplit=1)[-1][1:]
