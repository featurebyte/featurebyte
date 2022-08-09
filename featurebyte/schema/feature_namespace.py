"""
FeatureNamespace API pyaload schema
"""
from typing import List, Optional

from beanie import PydanticObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureNamespaceModel,
    FeatureReadiness,
    FeatureVersionIdentifier,
)
from featurebyte.routes.common.schema import PaginationMixin


class FeatureNamespaceCreate(FeatureByteBaseModel):
    """
    Feature Namespace Creation Schema
    """

    id: PydanticObjectId = Field(alias="_id")
    name: StrictStr
    description: Optional[StrictStr]
    version_ids: List[PydanticObjectId]
    versions: List[FeatureVersionIdentifier]
    readiness: FeatureReadiness
    default_version_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)


class FeatureNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureNamespace
    """

    data: List[FeatureNamespaceModel]
