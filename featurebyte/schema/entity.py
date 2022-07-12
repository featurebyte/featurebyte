"""
Entity API payload schema
"""
from typing import List, Optional

from beanie import PydanticObjectId
from pydantic import BaseModel, StrictStr

from featurebyte.models.entity import EntityModel
from featurebyte.routes.common.schema import PaginationMixin


class Entity(EntityModel):
    """
    Entity Document Model
    """

    user_id: Optional[PydanticObjectId]


class EntityCreate(BaseModel):
    """
    Entity Creation schema
    """

    name: StrictStr
    serving_name: StrictStr


class EntityList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[Entity]


class EntityUpdate(BaseModel):
    """
    Entity update schema
    """

    name: StrictStr
