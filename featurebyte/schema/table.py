"""
Table model's attribute payload schema
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.common.validator import columns_info_validator
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.feature_store import TableStatus
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.query_graph.model.column_info import ColumnInfo, ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class TableCreate(FeatureByteBaseModel):
    """
    TableService create schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    tabular_source: TabularSource
    columns_info: List[ColumnSpecWithDescription]
    record_creation_timestamp_column: Optional[StrictStr]
    description: Optional[StrictStr]

    # pydantic validators
    _columns_info_validator = validator("columns_info", allow_reuse=True)(columns_info_validator)

    @classmethod
    def _special_column_validator(cls, column_name: str, values: Dict[str, Any]) -> str:
        """
        Check if column name specified for a special column field exists in the table create columns info

        Parameters
        ----------
        column_name: str
            Special column name
        values: Dict[str, Any]
            Dict of values

        Raises
        ------
        ValueError
            If column name specified for a special column field does not exist in the table create columns info

        Returns
        -------
        str
        """
        columns_info = values.get("columns_info")
        # columns_info is None if validation failed for columns_info - skip validation in this case
        if column_name is not None and columns_info is not None:
            columns_info = set(column_info.name for column_info in columns_info)
            if column_name not in columns_info:
                raise ValueError(f"Column not found in table: {column_name}")
        return column_name


class TableUpdate(FeatureByteBaseModel):
    """
    Update table payload schema
    """

    status: Optional[TableStatus]
    record_creation_timestamp_column: Optional[StrictStr]

    # Update of columns info is deprecated and will be removed in release 0.5.0
    # See https://featurebyte.atlassian.net/browse/DEV-2000
    columns_info: Optional[List[ColumnInfo]]


class TableServiceUpdate(TableUpdate, BaseDocumentServiceUpdateSchema):
    """
    TableService update schema
    """

    columns_info: Optional[List[ColumnInfo]]

    # pydantic validators
    _columns_info_validator = validator("columns_info", allow_reuse=True)(columns_info_validator)


class TableList(PaginationMixin):
    """
    TableList used to deserialize list document output
    """

    data: Sequence[ProxyTableModel]

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs in the table model list

        Returns
        -------
        List[PydanticObjectId]
        """
        output = set()
        for table in self.data:
            output.update(table.entity_ids)
        return list(output)

    @property
    def semantic_ids(self) -> List[PydanticObjectId]:
        """
        List of semantic IDs in the table model list

        Returns
        -------
        List[PydanticObjectId]
        """
        output = set()
        for table in self.data:
            output.update(table.semantic_ids)
        return list(output)


class ColumnCriticalDataInfoUpdate(FeatureByteBaseModel):
    """
    Column critical data info update payload schema
    """

    column_name: NameStr
    critical_data_info: Optional[CriticalDataInfo]


class ColumnEntityUpdate(FeatureByteBaseModel):
    """
    Column entity update payload schema
    """

    column_name: NameStr
    entity_id: Optional[PydanticObjectId]


class ColumnDescriptionUpdate(FeatureByteBaseModel):
    """
    Column description update payload schema
    """

    column_name: NameStr
    description: Optional[StrictStr]


class ColumnSemanticUpdate(FeatureByteBaseModel):
    """
    Column semantic update payload schema
    """

    column_name: NameStr
    semantic_id: Optional[PydanticObjectId]
