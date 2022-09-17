"""
FeatureListNamespace API payload scheme
"""
from typing import List, Optional

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import (
    FeatureListNamespaceModel,
    FeatureListStatus,
    FeatureTypeFeatureCount,
)
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.schema.feature_namespace import NamespaceInfo


class FeatureListNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureListNamespace
    """

    data: List[FeatureListNamespaceModel]


class FeatureListNamespaceUpdate(FeatureByteBaseModel):
    """
    FeatureListNamespace update schema
    """

    status: Optional[FeatureListStatus]
    default_version_mode: Optional[DefaultVersionMode]


class FeatureListNamespaceServiceUpdate(FeatureListNamespaceUpdate):
    """
    FeatureListNamespace service update schema
    """

    feature_list_id: Optional[PydanticObjectId]


class FeatureListNamespaceInfo(NamespaceInfo):
    """
    FeatureListNamespace info schema
    """

    dtype_distribution: List[FeatureTypeFeatureCount]
    default_feature_list_id: PydanticObjectId
    status: FeatureListStatus
    feature_count: int
