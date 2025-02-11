"""
BatchFeatureTable API payload schema
"""

from __future__ import annotations

from typing import Any, List, Optional

from bson import ObjectId
from pydantic import BaseModel, Field, model_validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.batch_request_table import BatchRequestInput
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.materialized_table import BaseMaterializedTableListRecord


class BatchFeatureTableCreate(FeatureByteBaseModel):
    """
    BatchFeatureTableCreate creation payload
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    feature_store_id: PydanticObjectId
    batch_request_table_id: Optional[PydanticObjectId] = Field(default=None)
    request_input: Optional[BatchRequestInput] = Field(default=None)
    deployment_id: PydanticObjectId

    @model_validator(mode="after")
    def _validate_input(self) -> "BatchFeatureTableCreate":
        if self.batch_request_table_id is None and self.request_input is None:
            raise ValueError("Either batch_request_table_id or request_input must be provided")
        if self.batch_request_table_id is not None and self.request_input is not None:
            raise ValueError("Only one of batch_request_table_id or request_input must be provided")
        return self


class BatchFeatureTableList(PaginationMixin):
    """
    Schema for listing batch feature tables
    """

    data: List[BatchFeatureTableModel]


class BatchFeatureTableListRecord(BaseMaterializedTableListRecord):
    """
    Schema for listing historical feature tables as a DataFrame
    """

    feature_store_id: PydanticObjectId
    batch_request_table_id: PydanticObjectId

    @model_validator(mode="before")
    @classmethod
    def _extract(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values
