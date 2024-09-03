"""
ObservationTableModel API payload schema
"""

from __future__ import annotations

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

    sample_rows: int | None = Field(ge=0, default=None)
    request_input: ObservationInput
    skip_entity_validation_checks: bool = Field(default=False)
    purpose: Purpose | None = Field(default=None)
    primary_entity_ids: list[PydanticObjectId] | None = Field(default=None)
    target_column: StrictStr | None = Field(default=None)


class ObservationTableUpload(FeatureByteBaseModel):
    """
    ObservationTableUpload creation schema
    """

    id: PydanticObjectId | None = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    purpose: Purpose | None = Field(default=None)
    primary_entity_ids: list[PydanticObjectId]
    target_column: StrictStr | None = Field(default=None)


class ObservationTableList(PaginationMixin):
    """
    Schema for listing observation tables
    """

    data: list[ObservationTableModel]


class ObservationTableListRecord(BaseRequestTableListRecord):
    """
    This model determines the schema when listing observation tables via ObservationTable.list()
    """


class ObservationTableUpdate(FeatureByteBaseModel):
    """
    ObservationTable Update schema
    """

    context_id: PydanticObjectId | None = Field(default=None)
    context_id_to_remove: PydanticObjectId | None = Field(default=None)
    use_case_id_to_add: PydanticObjectId | None = Field(default=None)
    use_case_id_to_remove: PydanticObjectId | None = Field(default=None)
    purpose: Purpose | None = Field(default=None)
    name: NameStr | None = Field(default=None)


class ObservationTableServiceUpdate(BaseDocumentServiceUpdateSchema, ObservationTableUpdate):
    """
    ObservationTable Update schema for service
    """

    use_case_ids: list[PydanticObjectId] | None = Field(default=None)
