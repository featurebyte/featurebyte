"""
Pydantic Model for persistent storage
"""
from typing import Any, Dict, Optional

from datetime import datetime

from beanie import PydanticObjectId
from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.routes.common.util import get_utc_now


class AuditDocument(FeatureByteBaseModel):
    """
    Audit document
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id", allow_mutation=False)
    user_id: Optional[PydanticObjectId]
    name: str
    document_id: Any
    action_at: datetime = Field(default_factory=get_utc_now)
    action_type: str
    old_values: Dict[str, Any]
