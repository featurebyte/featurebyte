"""
This module contains common table related models.
"""
from typing import Any, List, Literal

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.node.schema import TableDetails


class TabularSource(FeatureByteBaseModel):
    """Model for tabular source"""

    feature_store_id: PydanticObjectId
    table_details: TableDetails


SPECIFIC_DATA_TABLES = []
DATA_TABLES = []


class BaseTableData(FeatureByteBaseModel):
    """Base data model used to capture input node info"""

    type: Literal[
        TableDataType.GENERIC,
        TableDataType.EVENT_DATA,
        TableDataType.ITEM_DATA,
        TableDataType.DIMENSION_DATA,
        TableDataType.SCD_DATA,
    ]
    columns_info: List[ColumnInfo]
    tabular_source: TabularSource

    def __init_subclass__(cls, **kwargs: Any):
        # add table into DATA_TABLES & SPECIFIC_DATA_TABLES (if not generic type)
        table_type = cls.__fields__["type"]
        if repr(table_type.type_).startswith("typing.Literal"):
            DATA_TABLES.append(cls)
        if table_type.default != TableDataType.GENERIC:
            SPECIFIC_DATA_TABLES.append(cls)
