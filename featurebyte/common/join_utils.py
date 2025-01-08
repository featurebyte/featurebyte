"""
Join utils class
"""

import copy
from typing import List, Optional, Set

from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.dtype import DBVarTypeInfo


def apply_modifiers_to_column_name(
    column_name: str,
    rsuffix: Optional[str],
    rprefix: Optional[str],
) -> str:
    """
    Updates the column name by including rsuffix and rprefix when provided

    Parameters
    ----------
    column_name: str
        column to update
    rsuffix: Optional[str]
        the suffix to attach on
    rprefix: Optional[str]
        the prefix to attach on

    Returns
    -------
    str
        Updated column name
    """
    return f"{rprefix if rprefix else ''}{column_name}{rsuffix if rsuffix else ''}"


def apply_column_name_modifiers_columns_info(
    column_infos: List[ColumnInfo], rsuffix: Optional[str], rprefix: Optional[str]
) -> List[ColumnInfo]:
    """
    Updates the column infos by appending the rsuffix to the column names.

    Parameters
    ----------
    column_infos: List[ColumnInfo]
        column infos to update
    rsuffix: Optional[str]
        the suffix to attach on
    rprefix: Optional[str]
        the prefix to attach on

    Returns
    -------
    List[ColumnInfo]
    """
    if rsuffix is None and rprefix is None:
        return column_infos
    updated_column_info = []
    column_map = {}
    for col_info in column_infos:
        new_col_info = col_info.model_copy()
        new_col_info.name = apply_modifiers_to_column_name(
            col_info.name, rsuffix=rsuffix, rprefix=rprefix
        )
        column_map[col_info.name] = new_col_info.name
        updated_column_info.append(new_col_info)

    for col_info in updated_column_info:
        if col_info.dtype_metadata:
            dtype_metadata = (
                DBVarTypeInfo(dtype=col_info.dtype, metadata=col_info.dtype_metadata)
                .remap_column_name(column_map)
                .metadata
            )
            col_info.dtype_metadata = dtype_metadata
    return updated_column_info


def filter_columns(columns: List[str], exclude_columns: List[str]) -> List[str]:
    """
    Filters a list of columns. This is used to remove columns such as the join key from the other
    view's columns so that we don't duplicate information in the resulting view.

    Parameters
    ----------
    columns: List[str]
        columns for a view
    exclude_columns: List[str]
        column names to be excluded

    Returns
    -------
    List[str]
        filtered list of columns
    """
    exclude_columns_set = set(exclude_columns)
    return [col for col in columns if col not in exclude_columns_set]


def apply_column_name_modifiers(
    columns: List[str], rsuffix: Optional[str], rprefix: Optional[str]
) -> List[str]:
    """
    Appends the rsuffix to columns if a rsuffix is provided.

    Parameters
    ----------
    columns: List[str]
        columns to update
    rsuffix: Optional[str]
        the suffix to attach on
    rprefix: Optional[str]
        the prefix to attach on

    Returns
    -------
    List[str]
        updated columns with rsuffix, or original columns if none were provided
    """
    if not rsuffix and not rprefix:
        return columns
    return [
        apply_modifiers_to_column_name(col, rsuffix=rsuffix, rprefix=rprefix) for col in columns
    ]


def filter_columns_info(col_info: List[ColumnInfo], exclude_columns: List[str]) -> List[ColumnInfo]:
    """
    Filters out column info that matches the join key.

    Parameters
    ----------
    col_info: List[ColumnInfo]
        Column info's
    exclude_columns: List[str]
        Column names to be removed from columns info

    Returns
    -------
    List[ColumnInfo]
        filtered column info's
    """
    exclude_columns_set = set(exclude_columns)
    return [col_info for col_info in col_info if col_info.name not in exclude_columns_set]


def combine_column_info_of_views(
    columns_a: List[ColumnInfo], columns_b: List[ColumnInfo], filter_set: Optional[Set[str]] = None
) -> List[ColumnInfo]:
    """
    Combine two column info views.

    If the filter_set is not provided, we'll not do any filtering.

    Parameters
    ----------
    columns_a: List[ColumnInfo]
        one list of columns
    columns_b: List[ColumnInfo]
        another list of columns
    filter_set: Optional[Set[str]]
        column names to filter columns_b on

    Returns
    -------
    List[ColumnInfo]
        combined columns
    """
    joined_columns_info = copy.deepcopy(columns_a)
    columns_b = copy.deepcopy(columns_b)
    for column_info in columns_b:
        if filter_set is None or column_info.name in filter_set:
            joined_columns_info.append(column_info)
    return joined_columns_info


def is_column_name_in_columns(column_name: str, columns_info: List[ColumnInfo]) -> bool:
    """
    Checks to see if a column name is in the list of ColumnInfo's provided.

    Parameters
    ----------
    column_name: str
        the column name to check
    columns_info: List[ColumnInfo]
        list of column info's

    Returns
    -------
    bool
        whether the column name is in the list of column info's
    """
    for col in columns_info:
        if column_name == col.name:
            return True
    return False
