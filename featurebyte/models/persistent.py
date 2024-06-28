"""
Pydantic Model for persistent storage
"""

from typing import Any, Dict, List, Mapping, Optional

import json
from datetime import datetime

from bson import ObjectId, json_util
from pydantic import Field, field_serializer

from featurebyte.common.model_util import get_utc_now
from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.schema.common.base import PaginationMixin

Document = Dict[str, Any]
QueryFilter = Dict[str, Any]
DocumentUpdate = Mapping[str, Any]


class AuditTransactionMode(StrEnum):
    """
    Audit logging mode
    """

    SINGLE = "SINGLE"
    MULTI = "MULTI"


class AuditActionType(StrEnum):
    """
    Audit action type
    """

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    REPLACE = "REPLACE"
    DELETE = "DELETE"


class AuditDocument(FeatureByteBaseModel):
    """
    Audit document
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id", frozen=True)
    user_id: Optional[PydanticObjectId] = None
    name: str
    document_id: Any = None
    action_at: datetime = Field(default_factory=get_utc_now)
    action_type: AuditActionType
    previous_values: Dict[str, Any]
    current_values: Dict[str, Any]

    @field_serializer("previous_values", "current_values", when_used="json")
    @staticmethod
    def _serialize_values(values: Dict[str, Any]) -> Dict[str, Any]:
        return json.loads(json_util.dumps(values))

    @field_serializer("document_id", when_used="json")
    @staticmethod
    def _serialize_document_id(document_id: Any) -> str:
        return str(document_id)


class AuditDocumentList(PaginationMixin):
    """
    Paginated list of Event Data
    """

    data: List[AuditDocument]


class FieldValueHistory(FeatureByteBaseModel):
    """
    Field value history
    """

    created_at: datetime
    value: Any = None
