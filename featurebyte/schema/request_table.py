"""
Base class for all request tables.
"""

from typing import Any, Optional

from bson import ObjectId
from pydantic import BaseModel, Field, model_validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.request_input import RequestInputType
from featurebyte.schema.materialized_table import BaseMaterializedTableListRecord


class BaseRequestTableCreate(FeatureByteBaseModel):
    """
    Base creation schema for all request tables
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    feature_store_id: PydanticObjectId
    context_id: Optional[PydanticObjectId] = Field(default=None)


class BaseRequestTableListRecord(BaseMaterializedTableListRecord):
    """
    This model determines the schema when listing tables.
    """

    feature_store_id: PydanticObjectId
    type: RequestInputType

    @model_validator(mode="before")
    @classmethod
    def _extract_base_request_table_fields(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        values["type"] = values["request_input"]["type"]
        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values
