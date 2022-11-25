"""
Join utils class
"""
from typing import Dict, List, Optional, Tuple

import copy

from featurebyte.core.util import append_to_lineage
from featurebyte.logger import logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import ColumnInfo


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


def combine_column_info_of_views(
    columns_a: List[ColumnInfo], columns_b: List[ColumnInfo], filter_set: set[str] = frozenset()
) -> List[ColumnInfo]:
    """
    Combine two column info views.

    If the filter_set provided is empty / not overridden, we'll not do any filtering.

    Parameters
    ----------
    columns_a: List[ColumnInfo]
        one list of columns
    columns_b: List[ColumnInfo]
        another list of columns
    filter_set: set[str]
        set of column names to filter columns_b on

    Returns
    -------
    List[ColumnInfo]
        combined columns
    """
    joined_columns_info = copy.deepcopy(columns_a)
    for column_info in columns_b:
        if len(filter_set) == 0 or column_info.name in filter_set:
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
    column_filter: List[str],
    node_name: str,
) -> Dict[str, Tuple[str, ...]]:
    """
    Joins the column lineage

    Parameters
    ----------
    current_map: Dict[str, Tuple[str, ...]]
        current view's column lineage map
    other_map: Dict[str, Tuple[str, ...]]
        other view's column lineage map
    column_filter: List[str]
        list of columns we want to filter the other_map on
    node_name: str
        name of the node

    Returns
    -------
    Dict[str, Tuple[str, ...]]
        joined column lineage map
    """
    joined_column_lineage_map = copy.deepcopy(current_map)
    for col in column_filter:
        if col in other_map:
            joined_column_lineage_map[col] = other_map[col]
        else:
            logger.debug(f"col {col} not present in other_map, ignoring this column name.")
    for col, lineage in joined_column_lineage_map.items():
        joined_column_lineage_map[col] = append_to_lineage(lineage, node_name)
    return joined_column_lineage_map
