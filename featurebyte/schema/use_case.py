"""
Use Case API payload schema
"""

from typing import Any, List, Optional

from pydantic import BaseModel, Field, StrictStr, model_validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.use_case import UseCaseModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class UseCaseCreate(FeatureByteBaseModel):
    """
    Use Case creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=PydanticObjectId, alias="_id")
    name: NameStr
    target_id: Optional[PydanticObjectId] = Field(default=None)
    target_namespace_id: Optional[PydanticObjectId] = Field(default=None)
    context_id: PydanticObjectId
    description: Optional[StrictStr] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _validate_target(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        target_id = values.get("target_id", None)
        target_namespace_id = values.get("target_namespace_id", None)
        if not target_id and not target_namespace_id:
            raise ValueError("Either target_id or target_namespace_id must be specified.")
        return values


class UseCaseUpdate(BaseDocumentServiceUpdateSchema):
    """
    Use Case update schema
    """

    default_preview_table_id: Optional[PydanticObjectId] = Field(default=None)
    default_eda_table_id: Optional[PydanticObjectId] = Field(default=None)
    observation_table_id_to_remove: Optional[PydanticObjectId] = Field(default=None)

    remove_default_eda_table: Optional[bool] = Field(default=None)
    remove_default_preview_table: Optional[bool] = Field(default=None)

    name: Optional[NameStr] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _validate_input(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

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
