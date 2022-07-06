"""
Entity API payload schema
"""
from typing import List, Optional

import datetime

from beanie import PydanticObjectId
from bson import ObjectId
from pydantic import BaseModel

from featurebyte.models.entity import EntityModel


class Entity(EntityModel):
    """
    Entity Data Document Model
    """

    user_id: Optional[PydanticObjectId]
    created_at: datetime.datetime

    class Config:
        """
        Configuration for Entity schema
        """

        json_encoders = {ObjectId: str}


class EntityCreate(BaseModel):
    """
    Entity Data Creation schema
    """

    name: str
    serving_column_names: List[str]
