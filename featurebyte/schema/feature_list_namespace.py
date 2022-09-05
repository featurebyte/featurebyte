"""
FeatureListNamespace API payload scheme
"""
from typing import List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.routes.common.schema import PaginationMixin


class FeatureListNamespaceCreate(FeatureByteBaseModel):
    """
    FeatureListNamespace Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_list_ids: List[PydanticObjectId] = Field(default_factory=list)
    default_feature_list_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)
    entity_ids: List[PydanticObjectId]
    event_data_ids: List[PydanticObjectId]


class FeatureListNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureListNamespace
    """

    data: List[FeatureListNamespaceModel]


class FeatureListNamespaceUpdate(FeatureByteBaseModel):
    """
    FeatureListNamespace update schema
    """

    feature_list_id: Optional[PydanticObjectId]
    default_version_mode: Optional[DefaultVersionMode]
