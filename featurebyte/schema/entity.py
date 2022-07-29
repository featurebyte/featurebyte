"""
Entity API payload schema
"""
from typing import List

from pydantic import StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.entity import EntityModel
from featurebyte.routes.common.schema import PaginationMixin


class EntityCreate(FeatureByteBaseModel):
    """
    Entity Creation schema
    """

    name: StrictStr
    serving_name: StrictStr


class EntityList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[EntityModel]


class EntityUpdate(FeatureByteBaseModel):
    """
    Entity update schema
    """

    name: StrictStr
