"""
Utility functions for query graph node module
"""

from typing import Sequence


def subset_frame_column_expr(frame_name: str, column_name: str) -> str:
    """
    Subset frame column expression

    Parameters
    ----------
    frame_name: str
        Frame name
    column_name: str
        Column name

    Returns
    -------
    str
        Subset frame column expression
    """
    return f"{frame_name}[{repr(column_name)}]"


def subset_frame_columns_expr(frame_name: str, column_names: Sequence[str]) -> str:
    """
    Subset frame columns expression

    Parameters
    ----------
    frame_name: str
        Frame column
    column_names: Sequence[str]
        Column names

    Returns
    -------
    str
        Subset frame columns expression
    """
    return f"{frame_name}[{repr(column_names)}]"


def filter_series_or_frame_expr(series_or_frame_name: str, filter_expression: str) -> str:
    """
    Filter series or frame expression

    Parameters
    ----------
    series_or_frame_name: str
        Series or frame name
    filter_expression: str
        Filter expression

    Returns
    -------
    str
        Filter series or frame expression
    """
    return f"{series_or_frame_name}[{filter_expression}]"
