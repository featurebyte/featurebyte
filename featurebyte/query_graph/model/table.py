"""
This module contains table related models.
"""
from typing import List, Literal, Optional

from pydantic import StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.model.column_info import ColumnInfo


class TableDetails(FeatureByteBaseModel):
    """Model for table"""

    database_name: Optional[StrictStr]
    schema_name: Optional[StrictStr]
    table_name: StrictStr


class TabularSource(FeatureByteBaseModel):
    """Model for tabular source"""

    feature_store_id: PydanticObjectId
    table_details: TableDetails


class DatabaseTableModel(FeatureByteBaseModel):
    """Model for a database table used in a feature store"""

    tabular_source: TabularSource


class BaseDataModel(DatabaseTableModel):
    """Base data model used to capture input node info"""

    type: Literal[
        TableDataType.EVENT_DATA,
        TableDataType.ITEM_DATA,
        TableDataType.DIMENSION_DATA,
        TableDataType.SCD_DATA,
    ]
    columns_info: List[ColumnInfo]
