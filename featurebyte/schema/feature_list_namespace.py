"""
FeatureListNamespace API payload scheme
"""

from typing import List, Optional

from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_list import FeatureReadinessDistribution, FeatureTypeFeatureCount
from featurebyte.models.feature_list_namespace import FeatureListNamespaceModel, FeatureListStatus
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

    status: Optional[FeatureListStatus] = Field(default=None)


class FeatureListNamespaceServiceUpdate(
    BaseDocumentServiceUpdateSchema, FeatureListNamespaceUpdate
):
    """
    FeatureListNamespace service update schema
    """

    feature_list_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    deployed_feature_list_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    default_feature_list_id: Optional[PydanticObjectId] = Field(default=None)
