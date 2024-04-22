"""
ObservationTableModel API payload schema
"""

from __future__ import annotations

from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.observation_table import ObservationInput, ObservationTableModel, Purpose
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.request_table import BaseRequestTableCreate, BaseRequestTableListRecord


class ObservationTableCreate(BaseRequestTableCreate):
    """
    ObservationTableModel creation schema
    """

    sample_rows: Optional[int] = Field(ge=0)
    request_input: ObservationInput
    skip_entity_validation_checks: bool = Field(default=False)
    purpose: Optional[Purpose] = Field(default=None)
    primary_entity_ids: Optional[List[PydanticObjectId]]
    target_column: Optional[StrictStr] = Field(default=None)


class ObservationTableUpload(FeatureByteBaseModel):
    """
    ObservationTableUpload creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    purpose: Optional[Purpose] = Field(default=None)
    primary_entity_ids: List[PydanticObjectId]
    target_column: Optional[StrictStr] = Field(default=None)


class ObservationTableList(PaginationMixin):
    """
    Schema for listing observation tables
    """

    data: List[ObservationTableModel]


class ObservationTableListRecord(BaseRequestTableListRecord):
    """
    This model determines the schema when listing observation tables via ObservationTable.list()
    """


class ObservationTableUpdate(FeatureByteBaseModel):
    """
    ObservationTable Update schema
    """

    context_id: Optional[PydanticObjectId]
    context_id_to_remove: Optional[PydanticObjectId]
    use_case_id_to_add: Optional[PydanticObjectId]
    use_case_id_to_remove: Optional[PydanticObjectId]
    purpose: Optional[Purpose]
    name: Optional[NameStr]


class ObservationTableServiceUpdate(BaseDocumentServiceUpdateSchema, ObservationTableUpdate):
    """
    ObservationTable Update schema for service
    """

    use_case_ids: Optional[List[PydanticObjectId]]
