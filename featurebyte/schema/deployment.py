"""
Pydantic schemas for handling API payloads for deployment routes
"""
from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, root_validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    VersionIdentifier,
)
from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class DeploymentCreate(FeatureByteBaseModel):
    """
    Schema for deployment creation
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: Optional[StrictStr]
    feature_list_id: PydanticObjectId


class DeploymentList(PaginationMixin):
    """
    Paginated list of Deployment
    """

    data: List[DeploymentModel]


class DeploymentUpdate(BaseDocumentServiceUpdateSchema):
    """
    Schema for deployment update
    """

    enabled: Optional[bool]


class DeploymentRead(FeatureByteBaseDocumentModel):
    """
    Schema for deployment read
    """

    feature_list_namespace_id: PydanticObjectId = Field(allow_mutation=False)
    feature_list_version: VersionIdentifier = Field(
        allow_mutation=False, description="Feature list version"
    )
    catalog_id: PydanticObjectId = Field(allow_mutation=False)
    num_feature: int = Field(allow_mutation=False)

    @root_validator(pre=True)
    @classmethod
    def populate_from_feature_list_doc(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Populate deployment fields from feature list document

        Parameters
        ----------
        values: Dict[str, Any]
            Values to validate

        Returns
        -------
        Dict[str, Any]
            Validated values
        """
        if "deployed" in values:
            assert values.get("deployed", False) is True
        if "feature_list_version" not in values:
            values["feature_list_version"] = values["version"]
        if "num_feature" not in values:
            values["num_feature"] = len(values["feature_ids"])
        return values

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = FeatureListModel.collection_name()


class DeploymentSummary(FeatureByteBaseModel):
    """
    Schema for deployment summary
    """

    num_feature_list: int
    num_feature: int
