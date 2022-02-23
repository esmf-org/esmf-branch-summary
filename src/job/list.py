"""
TODO
"""

import collections
from typing import Any, Iterable, List


class UniqueList(collections.UserList):
    """maintains order containing only unique values"""

    def __init__(self, data: Iterable):
        super().__init__(_to_unique(data))

    def append(self, item):
        if item not in self.data:
            super().append(item)


def _to_unique(items: Iterable) -> List[Any]:
    """Returns a list with only unique values, regardles if hashable"""
    result = []
    for item in items:
        if item not in result:
            result.append(item)
    return result
