"""
Feature or target schema
"""

from typing import Dict, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId


class FeatureOrTargetTableCreate(FeatureByteBaseModel):
    """
    FeatureOrTargetTableCreate
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    feature_store_id: PydanticObjectId
    observation_table_id: Optional[PydanticObjectId] = Field(default=None)


class ComputeRequest(FeatureByteBaseModel):
    """
    Compute request
    """

    serving_names_mapping: Optional[Dict[str, str]] = Field(default=None)
