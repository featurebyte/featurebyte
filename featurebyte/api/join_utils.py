"""
Join utils class
"""
from typing import List, Optional, Set

import copy

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnInfo


def append_rsuffix_to_column_info(
    column_infos: List[ColumnInfo], rsuffix: Optional[str]
) -> List[ColumnInfo]:
    """
    Updates the column infos by appending the rsuffix to the column names.

    Parameters
    ----------
    column_infos: List[ColumnInfo]
        column infos to update
    rsuffix: Optional[str]
        the suffix to attach on

    Returns
    -------
    List[ColumnInfo]
    """
    if not rsuffix:
        return column_infos
    updated_column_info = []
    for col_info in column_infos:
        new_col_info = col_info.copy()
        new_col_info.name = f"{col_info.name}{rsuffix}"
        updated_column_info.append(new_col_info)
    return updated_column_info


def filter_join_key_from_column(columns: List[str], join_key: str) -> List[str]:
    """
    Filters the join key from a list of columns. This is used to remove the join key from the other view's columns
    so that we don't duplicate information in the resulting view.

    Parameters
    ----------
    columns: List[str]
        columns for a view
    join_key: str
        join key column

    Returns
    -------
    List[str]
        filtered list of columns
    """
    return [col for col in columns if col != join_key]


def append_rsuffix_to_columns(columns: List[str], rsuffix: Optional[str]) -> List[str]:
    """
    Appends the rsuffix to columns if a rsuffix is provided.

    Parameters
    ----------
    columns: List[str]
        columns to update
    rsuffix: Optional[str]
        the suffix to attach on

    Returns
    -------
    List[str]
        updated columns with rsuffix, or original columns if none were provided
    """
    if not rsuffix:
        return columns
    return [f"{col}{rsuffix}" for col in columns]


def filter_join_key_from_column_info(col_info: List[ColumnInfo], join_key: str) -> List[ColumnInfo]:
    """
    Filters out column info that matches the join key.

    Parameters
    ----------
    col_info: List[ColumnInfo]
        colum info's
    join_key: str
        join key

    Returns
    -------
    List[ColumnInfo]
        filtered column info's
    """
    return [col_info for col_info in col_info if col_info.name != join_key]


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


def join_tabular_data_ids(
    data_ids_a: List[PydanticObjectId], data_ids_b: List[PydanticObjectId]
) -> List[PydanticObjectId]:
    """
    Joins two list of data IDs.

    Parameters
    ----------
    data_ids_a: List[PydanticObjectId]
        data IDs A
    data_ids_b: List[PydanticObjectId]
        data IDs B

    Returns
    -------
    List[PydanticObjectId]
    """
    return sorted(set(data_ids_a + data_ids_b))


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
