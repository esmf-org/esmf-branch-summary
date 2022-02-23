import collections
import re
from typing import List


class Hash(collections.UserString):
    """contains methods to parse and represent a job hash"""

    PARSE_PATTERNS: List[str] = [r"ESMF_\S*", r"v\S*\.\S*\.\S*"]

    def __init__(self, value: str):
        self.data = self._parse(value)
        super().__init__(self.data)

    def _parse(self, value) -> str:
        for pattern in self.PARSE_PATTERNS:
            try:
                return re.findall(pattern, value)[0]
            except IndexError:
                continue
        raise ValueError(f"could not parse [{value}]")

    def patterns(self) -> List[str]:
        """list of regex patterns used for extrapolating hash"""
        return self.PARSE_PATTERNS
