"""
FeatureNamespace API pyaload schema
"""
from typing import List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import DefaultVersionMode, FeatureNamespaceModel, FeatureReadiness
from featurebyte.routes.common.schema import PaginationMixin


class FeatureNamespaceCreate(FeatureByteBaseModel):
    """
    Feature Namespace Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    description: Optional[StrictStr]
    version_ids: List[PydanticObjectId] = Field(default_factory=list)
    readiness: FeatureReadiness
    default_version_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)


class FeatureNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureNamespace
    """

    data: List[FeatureNamespaceModel]


class FeatureNamespaceUpdate(FeatureByteBaseModel):
    """
    FeatureNamespace update schema
    """

    version_id: Optional[PydanticObjectId]
    default_version_mode: Optional[DefaultVersionMode]
