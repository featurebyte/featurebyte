"""
Pydantic Model for persistent storage
"""

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from bson import ObjectId
from pydantic import Field, field_serializer

from featurebyte.common.model_util import get_utc_now
from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.utils import serialize_obj
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
    user_id: Optional[PydanticObjectId] = Field(default=None)
    name: str
    document_id: Any = Field(default=None)
    action_at: datetime = Field(default_factory=get_utc_now)
    action_type: AuditActionType
    previous_values: Dict[str, Any]
    current_values: Dict[str, Any]

    @field_serializer("previous_values", "current_values", when_used="json")
    @staticmethod
    def _serialize_values(values: Any) -> Any:
        return serialize_obj(values)

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
    value: Any = Field(default=None)
