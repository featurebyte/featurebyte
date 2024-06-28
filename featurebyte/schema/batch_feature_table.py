"""
BatchFeatureTable API payload schema
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, model_validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.materialized_table import BaseMaterializedTableListRecord


class BatchFeatureTableCreate(FeatureByteBaseModel):
    """
    BatchFeatureTableCreate creation payload
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    feature_store_id: PydanticObjectId
    batch_request_table_id: PydanticObjectId
    deployment_id: PydanticObjectId


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
    def _extract(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values
