"""
Dictionary related common utility function
"""

from __future__ import annotations

from typing import Any


def get_field_path_value(doc_dict: dict[str, Any], field_path: list[str]) -> dict[str, Any] | Any:
    """
    Traverse dictionary to retrieve value using the given field_path parameter

    Parameters
    ----------
    doc_dict: dict[str, Any]
        Document in dictionary format
    field_path: list[str]
        List of str or int used to traverse the document

    Returns
    -------
    Any
    """
    if field_path:
        return get_field_path_value(doc_dict[field_path[0]], field_path[1:])
    return doc_dict
