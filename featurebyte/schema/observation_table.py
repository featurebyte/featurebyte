"""
ObservationTableModel API payload schema
"""
from __future__ import annotations

from typing import List, Optional, Union

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import (
    MaterializedTable,
    ObservationTableModel,
    ViewObservationInput,
)
from featurebyte.schema.common.base import PaginationMixin


class ObservationTableCreateBase(MaterializedTable):
    """
    ObservationTableModel creation schema's common attributes
    """

    context_id: Optional[PydanticObjectId]


class ObservationTableCreateFromView(ObservationTableCreateBase):
    """
    ObservationTableModel creation schema for creating from a view
    """

    observation_input: ViewObservationInput


class ObservationTableCreateFromSourceTable(ObservationTableCreateBase):
    """
    ObservationTableModel creation schema for creating from a source table
    """

    observation_input: ObservationTableCreateFromSourceTable


ObservationTableCreate = Union[
    ObservationTableCreateFromView, ObservationTableCreateFromSourceTable
]


class ObservationTableList(PaginationMixin):
    """
    Schema for listing observation tables
    """

    data: List[ObservationTableModel]
