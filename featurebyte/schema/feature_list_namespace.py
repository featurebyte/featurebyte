"""
FeatureListNamespace API payload scheme
"""
from typing import List, Optional

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import (
    FeatureListNamespaceModel,
    FeatureListStatus,
    FeatureReadinessDistribution,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


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
    default_feature_list_id: Optional[PydanticObjectId]


class FeatureListNamespaceServiceUpdate(
    BaseDocumentServiceUpdateSchema, FeatureListNamespaceUpdate
):
    """
    FeatureListNamespace service update schema
    """

    feature_list_ids: Optional[List[PydanticObjectId]]
    deployed_feature_list_ids: Optional[List[PydanticObjectId]]
    readiness_distribution: Optional[FeatureReadinessDistribution]
    default_feature_list_id: Optional[PydanticObjectId]
