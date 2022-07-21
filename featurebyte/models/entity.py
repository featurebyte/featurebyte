"""
This module contains Entity related models
"""
from __future__ import annotations

from typing import List, Optional

from datetime import datetime

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel


class EntityNameHistoryEntry(FeatureByteBaseModel):
    """
    Model for an entry in name history
    """

    created_at: datetime
    name: StrictStr


class EntityModel(FeatureByteBaseModel):
    """
    Model for Entity

    id: PydanticObjectId
        Entity id of the object
    name: str
        Name of the Entity
    serving_names: List[str]
        Name of the serving column
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    serving_names: List[StrictStr]
    name_history: List[EntityNameHistoryEntry] = Field(default_factory=list)
    created_at: Optional[datetime] = Field(default=None)
