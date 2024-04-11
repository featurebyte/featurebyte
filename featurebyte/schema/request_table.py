"""
Base class for all request tables.
"""

from typing import Any, Dict, Optional

from bson import ObjectId
from pydantic import Field, root_validator

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
    context_id: Optional[PydanticObjectId]


class BaseRequestTableListRecord(BaseMaterializedTableListRecord):
    """
    This model determines the schema when listing tables.
    """

    feature_store_id: PydanticObjectId
    type: RequestInputType

    @root_validator(pre=True)
    @classmethod
    def _extract_base_request_table_fields(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["type"] = values["request_input"]["type"]
        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values
