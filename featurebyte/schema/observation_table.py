"""
ObservationTableModel API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import Field

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationInput, ObservationTableModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.request_table import BaseRequestTableCreate, BaseRequestTableListRecord


class ObservationTableCreate(BaseRequestTableCreate):
    """
    ObservationTableModel creation schema
    """

    sample_rows: Optional[int] = Field(ge=0)
    request_input: ObservationInput
    entity_ids: List[PydanticObjectId] = Field(default_factory=list)


class ObservationTableList(PaginationMixin):
    """
    Schema for listing observation tables
    """

    data: List[ObservationTableModel]


class ObservationTableListRecord(BaseRequestTableListRecord):
    """
    This model determines the schema when listing observation tables via ObservationTable.list()
    """


class ObservationTableUpdate(BaseDocumentServiceUpdateSchema):
    """
    ObservationTable Update Context schema
    """

    context_id: PydanticObjectId
