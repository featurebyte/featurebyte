"""
Pydantic Model for persistent storage
"""
from typing import Any, Dict, Mapping, MutableMapping, Optional

from datetime import datetime
from enum import Enum

from beanie import PydanticObjectId
from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.routes.common.util import get_utc_now

Document = MutableMapping[str, Any]
QueryFilter = MutableMapping[str, Any]
DocumentUpdate = Mapping[str, Any]


class AuditTransactionMode(str, Enum):
    """
    Audit logging mode
    """

    SINGLE = "SINGLE"
    MULTI = "MULTI"


class AuditActionType(str, Enum):
    """
    Database or data warehouse source type
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
