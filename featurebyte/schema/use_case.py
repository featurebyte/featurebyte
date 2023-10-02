"""
Use Case API payload schema
"""
from typing import List, Optional

from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.use_case import UseCaseModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class UseCaseCreate(FeatureByteBaseModel):
    """
    Use Case creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=PydanticObjectId, alias="_id")
    name: StrictStr
    target_id: PydanticObjectId
    context_id: PydanticObjectId
    description: Optional[StrictStr]


class UseCaseUpdate(BaseDocumentServiceUpdateSchema):
    """
    Use Case update schema
    """

    new_observation_table_id: Optional[PydanticObjectId]
    default_preview_table_id: Optional[PydanticObjectId]
    default_eda_table_id: Optional[PydanticObjectId]


class UseCaseList(PaginationMixin):
    """
    Paginated list of use case
    """

    data: List[UseCaseModel]
