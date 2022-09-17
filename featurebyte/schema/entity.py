"""
Entity API payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.entity import EntityModel
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.schema.common.base import BaseBriefInfo, BaseInfo
from featurebyte.schema.common.operation import DictProject


class EntityCreate(FeatureByteBaseModel):
    """
    Entity Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
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


class EntityBriefInfo(BaseBriefInfo):
    """
    Entity brief info schema
    """

    serving_names: List[str]


class EntityBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of entity brief info
    """

    __root__: List[EntityBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> EntityBriefInfoList:
        """
        Construct entity brief info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        EntityBriefInfoList
        """
        entity_project = DictProject(rule=("data", ["name", "serving_names"]))
        return EntityBriefInfoList(__root__=entity_project.project(paginated_data))


class EntityInfo(EntityBriefInfo, BaseInfo):
    """
    Entity info schema
    """
