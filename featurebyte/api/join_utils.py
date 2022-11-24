"""
Join utils class
"""
from typing import Dict, List, Optional, Tuple

import copy

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import ColumnInfo


def append_rsuffix_to_columns(columns: list[str], rsuffix: Optional[str]) -> list[str]:
    """
    Appends the rsuffix to columns if a rsuffix is provided.

    Parameters
    ----------
    columns: list[str]
        columns to update
    rsuffix: Optional[str]
        the suffix to attach on

    Returns
    -------
    list[str]
        updated columns with rsuffix, or original columns if none were provided
    """
    if not rsuffix:
        return columns
    return [f"{col}{rsuffix}" for col in columns]


def combine_column_info_of_views(
    columns_a: list[ColumnInfo], columns_b: list[ColumnInfo], filter_set=None
) -> list[ColumnInfo]:
    """
    Combine two column info views

    Parameters
    ----------
    columns_a: list[ColumnInfo]
        one list of columns
    columns_b: list[ColumnInfo]
        another list of columns
    filter_set: set[str]
        set of column names to filter columns_b on

    Returns
    -------
    list[ColumnInfo]
        combined columns
    """
    if filter_set is None:
        filter_set = set()
    joined_columns_info = copy.deepcopy(columns_a)
    for column_info in columns_b:
        if column_info.name in filter_set:
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


def join_column_lineage_map(
    current_map: Dict[str, Tuple[str, ...]],
    other_map: Dict[str, Tuple[str, ...]],
    column_filter: list[str],
) -> Dict[str, Tuple[str, ...]]:
    return current_map
