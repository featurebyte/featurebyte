"""
FeatureListNamespace API payload scheme
"""
from typing import List, Optional

from beanie import PydanticObjectId

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.routes.common.schema import PaginationMixin


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
