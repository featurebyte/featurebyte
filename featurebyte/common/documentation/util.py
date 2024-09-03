"""
Util functions.
"""

from typing import Any, Optional


def _filter_none_from_list(input_list: list[Optional[Any]]) -> list[Any]:
    return [item for item in input_list if item is not None]
