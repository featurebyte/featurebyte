"""
Pydantic Model for persistent storage
"""

from typing import Any, Dict, List, Mapping, Optional

from datetime import datetime

from bson import ObjectId
from pydantic import Field

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

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id", allow_mutation=False)
    user_id: Optional[PydanticObjectId]
    name: str
    document_id: Any
    action_at: datetime = Field(default_factory=get_utc_now)
    action_type: AuditActionType
    previous_values: Dict[str, Any]
    current_values: Dict[str, Any]


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
    value: Any
