"""
Base class for all request tables.
"""
from typing import Any, Dict, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, conint, root_validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
)
from featurebyte.models.request_input import RequestInputType


class BaseRequestTableCreate(FeatureByteBaseModel):
    """
    Base creation schema for all request tables
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_store_id: PydanticObjectId
    context_id: Optional[PydanticObjectId]
    sample_rows: Optional[conint(ge=0)]  # type: ignore[valid-type]


class BaseRequestTableListRecord(FeatureByteBaseDocumentModel):
    """
    This model determines the schema when listing tables.
    """

    feature_store_id: PydanticObjectId
    type: RequestInputType

    @root_validator(pre=True)
    @classmethod
    def _extract_location(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["type"] = values["request_input"]["type"]
        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values
