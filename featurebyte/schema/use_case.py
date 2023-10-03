"""
Use Case API payload schema
"""
from typing import Any, Dict, List, Optional

from pydantic import Field, StrictStr, root_validator

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.use_case import UseCaseModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class UseCaseCreate(FeatureByteBaseModel):
    """
    Use Case creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=PydanticObjectId, alias="_id")
    name: StrictStr
    target_id: Optional[PydanticObjectId]
    target_namespace_id: Optional[PydanticObjectId]
    context_id: PydanticObjectId
    description: Optional[StrictStr]

    @root_validator(pre=True)
    @classmethod
    def _validate_target(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        target_id = values.get("target_id", None)
        target_namespace_id = values.get("target_namespace_id", None)
        if not target_id and not target_namespace_id:
            raise ValueError("Either target_id or target_namespace_id must be specified.")
        return values


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
