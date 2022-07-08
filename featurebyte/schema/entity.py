"""
Entity API payload schema
"""
from typing import List, Optional

import datetime

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import BaseModel

from featurebyte.models.entity import EntityModel
from featurebyte.routes.common.schema import PaginationMixin


class Entity(EntityModel):
    """
    Entity Document Model
    """

    user_id: Optional[PydanticObjectId]
    created_at: datetime.datetime

    class Config:
        """
        Configuration for Entity schema
        """

        # pylint: disable=too-few-public-methods

        json_encoders = {ObjectId: str}


class EntityCreate(BaseModel):
    """
    Entity Creation schema
    """

    name: str
    serving_name: str


class EntityList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[Entity]


class EntityUpdate(BaseModel):
    """
    Entity update schema
    """

    name: str
