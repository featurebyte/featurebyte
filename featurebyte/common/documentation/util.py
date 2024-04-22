"""
Util functions.
"""

from typing import Any, List, Optional


def _filter_none_from_list(input_list: List[Optional[Any]]) -> List[Any]:
    return [item for item in input_list if item is not None]
