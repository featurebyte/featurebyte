"""
This module contains Entity related models
"""
from __future__ import annotations

from typing import List

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import BaseModel, Field


class EntityModel(BaseModel):
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
    name: str
    serving_names: List[str]

    class Config:
        """
        Configuration for EntityModel schema
        """

        # pylint: disable=too-few-public-methods

        json_encoders = {ObjectId: str}
