"""
ObservationTableModel API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, root_validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
)
from featurebyte.models.observation_table import ObservationInput, ObservationTableModel
from featurebyte.schema.common.base import PaginationMixin


class ObservationTableCreate(FeatureByteBaseModel):
    """
    ObservationTableModel creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_store_id: PydanticObjectId
    context_id: Optional[PydanticObjectId]
    observation_input: ObservationInput


class ObservationTableList(PaginationMixin):
    """
    Schema for listing observation tables
    """

    data: List[ObservationTableModel]


class ObservationTableListRecord(FeatureByteBaseDocumentModel):
    """
    This model determines the schema when listing observation tables via ObservationTable.list()
    """

    database_name: StrictStr
    schema_name: StrictStr
    table_name: StrictStr

    @root_validator(pre=True)
    @classmethod
    def _extract_location(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        location = values.pop("location")
        table_details = location["table_details"]
        values["database_name"] = table_details["database_name"]
        values["schema_name"] = table_details["schema_name"]
        values["table_name"] = table_details["table_name"]
        return values
