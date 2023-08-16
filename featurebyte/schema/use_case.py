"""
Use Case API payload schema
"""
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.use_case import UseCaseModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class UseCaseCreate(FeatureByteBaseModel):
    """
    Use Case creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    context_id: PydanticObjectId
    target_id: PydanticObjectId
    description: Optional[StrictStr]
    observation_table_ids: Optional[List[PydanticObjectId]] = Field(default_factory=list)


class UseCaseUpdate(BaseDocumentServiceUpdateSchema):
    """
    Use Case update schema
    """

    new_observation_table_id: Optional[PydanticObjectId]
    default_preview_table_id: Optional[PydanticObjectId]
    default_eda_table_id: Optional[PydanticObjectId]
    observation_table_ids: Optional[List[PydanticObjectId]]


class UseCaseList(PaginationMixin):
    """
    Paginated list of context
    """

    data: List[UseCaseModel]
