"""
Use Case API payload schema
"""

from typing import Any, Dict, List, Optional

from pydantic import Field, StrictStr, root_validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.use_case import UseCaseModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class UseCaseCreate(FeatureByteBaseModel):
    """
    Use Case creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=PydanticObjectId, alias="_id")
    name: NameStr
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

    default_preview_table_id: Optional[PydanticObjectId]
    default_eda_table_id: Optional[PydanticObjectId]
    observation_table_id_to_remove: Optional[PydanticObjectId]

    remove_default_eda_table: Optional[bool]
    remove_default_preview_table: Optional[bool]

    name: Optional[NameStr]

    @root_validator(pre=True)
    @classmethod
    def _validate_input(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        default_preview_table_id = values.get("default_preview_table_id", None)
        default_eda_table_id = values.get("default_eda_table_id", None)
        observation_table_id_to_remove = values.get("observation_table_id_to_remove", None)

        if observation_table_id_to_remove:
            if (
                default_preview_table_id
                and default_preview_table_id == observation_table_id_to_remove
            ):
                raise ValueError(
                    "observation_table_id_to_remove cannot be the same as default_preview_table_id"
                )

            if default_eda_table_id and default_eda_table_id == observation_table_id_to_remove:
                raise ValueError(
                    "observation_table_id_to_remove cannot be the same as default_eda_table_id"
                )

        return values


class UseCaseList(PaginationMixin):
    """
    Paginated list of use case
    """

    data: List[UseCaseModel]
