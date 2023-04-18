"""
ObservationTableModel API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, conint, root_validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
)
from featurebyte.models.observation_table import ObservationInput, ObservationTableModel
from featurebyte.models.request_input import RequestInputType
from featurebyte.schema.common.base import PaginationMixin


class ObservationTableCreate(FeatureByteBaseModel):
    """
    ObservationTableModel creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_store_id: PydanticObjectId
    context_id: Optional[PydanticObjectId]
    request_input: ObservationInput
    sample_rows: Optional[conint(ge=0)]  # type: ignore[valid-type]


class ObservationTableList(PaginationMixin):
    """
    Schema for listing observation tables
    """

    data: List[ObservationTableModel]


class ObservationTableListRecord(FeatureByteBaseDocumentModel):
    """
    This model determines the schema when listing observation tables via ObservationTable.list()
    """

    feature_store_id: PydanticObjectId
    type: RequestInputType

    @root_validator(pre=True)
    @classmethod
    def _extract_location(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["type"] = values["request_input"]["type"]
        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values
