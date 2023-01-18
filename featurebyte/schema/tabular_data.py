"""
Data model's attribute payload schema
"""
from __future__ import annotations

from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store import DataStatus
from featurebyte.models.tabular_data import TabularDataModel
from featurebyte.query_graph.model.column_info import ColumnInfo, validate_columns_info
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class DataCreate(FeatureByteBaseModel):
    """
    DataService create schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    tabular_source: TabularSource
    columns_info: List[ColumnInfo]
    record_creation_date_column: Optional[StrictStr]

    @validator("columns_info")
    @classmethod
    def _validate_columns_info(cls, values: List[ColumnInfo]) -> List[ColumnInfo]:
        validate_columns_info(columns_info=values)
        return values


class DataUpdate(FeatureByteBaseModel):
    """
    Update data payload schema
    """

    columns_info: Optional[List[ColumnInfo]]
    status: Optional[DataStatus]
    record_creation_date_column: Optional[StrictStr]

    @validator("columns_info")
    @classmethod
    def _validate_columns_info(
        cls, values: Optional[List[ColumnInfo]]
    ) -> Optional[List[ColumnInfo]]:
        if values is not None:
            validate_columns_info(columns_info=values)
        return values


class DataServiceUpdate(DataUpdate, BaseDocumentServiceUpdateSchema):
    """
    DataService update schema
    """

    graph: Optional[QueryGraphModel]
    node_name: Optional[str]


class TabularDataList(PaginationMixin):
    """
    TabularDataList used to deserialize list document output
    """

    data: List[TabularDataModel]

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs in the data model list

        Returns
        -------
        List[PydanticObjectId]
        """
        output = set()
        for tabular_data in self.data:
            output.update(tabular_data.entity_ids)
        return list(output)

    @property
    def semantic_ids(self) -> List[PydanticObjectId]:
        """
        List of semantic IDs in the data model list

        Returns
        -------
        List[PydanticObjectId]
        """
        output = set()
        for tabular_data in self.data:
            output.update(tabular_data.semantic_ids)
        return list(output)
