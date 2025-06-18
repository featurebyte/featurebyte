"""
ObservationTableModel API payload schema
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, field_validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.observation_table import ObservationInput, ObservationTableModel, Purpose
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.request_table import BaseRequestTableCreate, BaseRequestTableListRecord


class ObservationTableModelResponse(ObservationTableModel):
    """
    ObservationTableModel response schema
    """

    is_valid: bool


class ObservationTableCreate(BaseRequestTableCreate):
    """
    ObservationTableModel creation schema
    """

    sample_rows: Optional[int] = Field(ge=0, default=None)
    sample_from_timestamp: Optional[datetime] = Field(default=None)
    sample_to_timestamp: Optional[datetime] = Field(default=None)
    request_input: ObservationInput
    skip_entity_validation_checks: bool = Field(default=False)
    purpose: Optional[Purpose] = Field(default=None)
    primary_entity_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    target_column: Optional[StrictStr] = Field(default=None)

    # pydantic validators
    _sort_ids_validator = field_validator("primary_entity_ids")(construct_sort_validator())


class ObservationTableUpload(FeatureByteBaseModel):
    """
    ObservationTableUpload creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    purpose: Optional[Purpose] = Field(default=None)
    primary_entity_ids: List[PydanticObjectId]
    target_column: Optional[StrictStr] = Field(default=None)

    # pydantic validators
    _sort_ids_validator = field_validator("primary_entity_ids")(construct_sort_validator())


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

    context_id: Optional[PydanticObjectId] = Field(default=None)
    context_id_to_remove: Optional[PydanticObjectId] = Field(default=None)
    use_case_id_to_add: Optional[PydanticObjectId] = Field(default=None)
    use_case_id_to_remove: Optional[PydanticObjectId] = Field(default=None)
    purpose: Optional[Purpose] = Field(default=None)
    name: Optional[NameStr] = Field(default=None)


class ObservationTableServiceUpdate(BaseDocumentServiceUpdateSchema, ObservationTableUpdate):
    """
    ObservationTable Update schema for service
    """

    use_case_ids: Optional[List[PydanticObjectId]] = Field(default=None)
