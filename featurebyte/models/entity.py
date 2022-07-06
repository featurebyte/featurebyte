"""
This module contains Entity related models
"""
from __future__ import annotations

from typing import List

from beanie import PydanticObjectId
from bson import ObjectId
from pydantic import BaseModel, Field


class EntityModel(BaseModel):
    """
    Model for Entity

    id: PydanticObjectId
        Entity id of the object
    name: str
        Name of the Entity
    serving_column_name: List[str]
        Name of the serving column
    """

    id: PydanticObjectId = Field(default_factory=ObjectId)
    name: str
    serving_column_name: List[str]
