"""
FeatureListNamespace API payload scheme
"""
from typing import List, Optional

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_list import (
    FeatureListNamespaceModel,
    FeatureListStatus,
    FeatureReadinessDistribution,
    FeatureTypeFeatureCount,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class FeatureListNamespaceModelResponse(FeatureListNamespaceModel):
    """
    Extended FeatureListNamespace model
    """

    readiness_distribution: FeatureReadinessDistribution
    dtype_distribution: List[FeatureTypeFeatureCount]
    primary_entity_ids: List[PydanticObjectId]
    entity_ids: List[PydanticObjectId]
    table_ids: List[PydanticObjectId]


class FeatureListNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureListNamespace
    """

    data: List[FeatureListNamespaceModelResponse]


class FeatureListNamespaceUpdate(FeatureByteBaseModel):
    """
    FeatureListNamespace update schema
    """

    status: Optional[FeatureListStatus]


class FeatureListNamespaceServiceUpdate(
    BaseDocumentServiceUpdateSchema, FeatureListNamespaceUpdate
):
    """
    FeatureListNamespace service update schema
    """

    feature_list_ids: Optional[List[PydanticObjectId]]
    deployed_feature_list_ids: Optional[List[PydanticObjectId]]
    default_feature_list_id: Optional[PydanticObjectId]
