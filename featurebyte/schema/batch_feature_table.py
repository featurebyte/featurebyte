"""
BatchFeatureTable API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.common.base import BaseInfo, PaginationMixin


class BatchFeatureTableCreate(FeatureByteBaseModel):
    """
    BatchFeatureTableCreate creation payload
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_store_id: PydanticObjectId
    batch_request_table_id: PydanticObjectId
    deployment_id: PydanticObjectId


class BatchFeatureTableList(PaginationMixin):
    """
    Schema for listing batch feature tables
    """

    data: List[BatchFeatureTableModel]


class BatchFeatureTableInfo(BaseInfo):
    """
    Schema for batch feature table info
    """

    batch_request_table_name: str
    deployment_name: str
