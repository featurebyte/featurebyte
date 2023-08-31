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
    context_id: Optional[PydanticObjectId]
    description: Optional[StrictStr]


class UseCaseCreateTarget(UseCaseCreate):
    """
    Use Case creation schema with observation_table_ids. Not exposed to API.
    """

    observation_table_ids: List[PydanticObjectId] = Field(default_factory=list)


class UseCaseUpdate(BaseDocumentServiceUpdateSchema):
    """
    Use Case update schema
    """

    new_observation_table_id: Optional[PydanticObjectId]
    default_preview_table_id: Optional[PydanticObjectId]
    default_eda_table_id: Optional[PydanticObjectId]


class UseCaseUpdateTarget(UseCaseUpdate):
    """
    Use Case update schema with observation_table_ids. Not exposed to API.
    """

    observation_table_ids: List[PydanticObjectId] = Field(default_factory=list)


class UseCaseList(PaginationMixin):
    """
    Paginated list of use case
    """

    data: List[UseCaseModel]
