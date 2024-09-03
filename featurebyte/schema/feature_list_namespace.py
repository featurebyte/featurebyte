"""
FeatureListNamespace API payload scheme
"""

from typing import Optional

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
    dtype_distribution: list[FeatureTypeFeatureCount]
    primary_entity_ids: list[PydanticObjectId]
    entity_ids: list[PydanticObjectId]
    table_ids: list[PydanticObjectId]


class FeatureListNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureListNamespace
    """

    data: list[FeatureListNamespaceModelResponse]


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

    feature_list_ids: Optional[list[PydanticObjectId]] = Field(default=None)
    deployed_feature_list_ids: Optional[list[PydanticObjectId]] = Field(default=None)
    default_feature_list_id: Optional[PydanticObjectId] = Field(default=None)
