"""
DeploymentSql API payload schema
"""

from typing import List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.deployment_sql import DeploymentSqlModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class DeploymentSqlCreate(FeatureByteBaseModel):
    """
    DeploymentSql creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    deployment_id: PydanticObjectId


class DeploymentSqlList(PaginationMixin):
    """
    Paginated list of deployment SQL
    """

    data: List[DeploymentSqlModel]


class DeploymentSqlUpdate(BaseDocumentServiceUpdateSchema):
    """
    DeploymentSql update schema
    """


class DeploymentSqlModelResponse(DeploymentSqlModel):
    """
    DeploymentSql model response schema
    """
