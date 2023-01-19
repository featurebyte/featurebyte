"""
This module contains column info related models.
"""
from typing import List, Optional

from pydantic import Field

from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.node.schema import ColumnSpec


class ColumnInfo(ColumnSpec):
    """
    ColumnInfo for storing column information

    name: str
        Column name
    dtype: DBVarType
        Variable type of the column
    entity_id: Optional[PydanticObjectId]
        Entity id associated with the column
    critical_data_info: Optional[CriticalDataInfo]
        Critical data info of the column
    """

    entity_id: Optional[PydanticObjectId] = Field(default=None)
    semantic_id: Optional[PydanticObjectId] = Field(default=None)
    critical_data_info: Optional[CriticalDataInfo] = Field(default=None)


def validate_columns_info(columns_info: List[ColumnInfo]) -> None:
    """
    Validate list of ColumnInfo used in table

    Parameters
    ----------
    columns_info: List[ColumnInfo]
        List of columns info used in data table

    Raises
    ------
    ValueError
        If a column name appears more than one time
    """
    # check column name uniqueness
    column_names = set()
    for column_info in columns_info:
        if column_info.name in column_names:
            raise ValueError(f'Column name "{column_info.name}" is duplicated.')
        column_names.add(column_info.name)
