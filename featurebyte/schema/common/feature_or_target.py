"""
Feature or target schema
"""
from typing import Dict, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId


class FeatureOrTargetTableCreate(FeatureByteBaseModel):
    """
    FeatureOrTargetTableCreate
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_store_id: PydanticObjectId
    observation_table_id: Optional[PydanticObjectId]


class ComputeRequest(FeatureByteBaseModel):
    """
    Compute request
    """

    serving_names_mapping: Optional[Dict[str, str]]
