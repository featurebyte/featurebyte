"""
PredictionTable API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.prediction_table import PredictionTableModel
from featurebyte.schema.common.base import PaginationMixin


class PredictionTableCreate(FeatureByteBaseModel):
    """
    PredictionTable creation payload
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_store_id: PydanticObjectId
    observation_table_id: PydanticObjectId
    feature_list_id: PydanticObjectId


class PredictionTableList(PaginationMixin):
    """
    Schema for listing prediction tables
    """

    data: List[PredictionTableModel]
