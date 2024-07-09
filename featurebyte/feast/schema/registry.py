"""
Feast registry related schemas
"""

from typing import List, Optional

from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema


class FeastRegistryCreate(FeatureByteBaseModel):
    """
    Feast registry create schema
    """

    feature_lists: List[FeatureListModel]
    deployment_id: PydanticObjectId


class FeastRegistryUpdate(BaseDocumentServiceUpdateSchema):
    """
    Feast registry update schema
    """

    feature_lists: Optional[List[FeatureListModel]] = Field(default=None)

    # these fields are not expected to be updated directly
    feature_store_id: Optional[PydanticObjectId] = Field(default=None)
    registry_path: Optional[str] = Field(default=None)
