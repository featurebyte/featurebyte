"""
Data model's attribute payload schema
"""
from __future__ import annotations

from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.common.validator import columns_info_validator
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store import DataStatus
from featurebyte.models.tabular_data import TabularDataModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
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

    # pydantic validators
    _columns_info_validator = validator("columns_info", allow_reuse=True)(columns_info_validator)


class DataUpdate(FeatureByteBaseModel):
    """
    Update data payload schema
    """

    columns_info: Optional[List[ColumnInfo]]
    status: Optional[DataStatus]
    record_creation_date_column: Optional[StrictStr]

    # pydantic validators
    _columns_info_validator = validator("columns_info", allow_reuse=True)(columns_info_validator)


class DataServiceUpdate(DataUpdate, BaseDocumentServiceUpdateSchema):
    """
    DataService update schema
    """


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
