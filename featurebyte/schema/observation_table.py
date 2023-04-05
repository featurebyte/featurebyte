"""
ObservationTableModel API payload schema
"""
from __future__ import annotations

from typing import List, Optional, Union

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.observation_table import (
    ObservationTableModel,
    SourceTableObservationInput,
    ViewObservationInput,
)
from featurebyte.schema.common.base import PaginationMixin


class ObservationTableCreate(FeatureByteBaseModel):
    """
    ObservationTableModel creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_store_id: PydanticObjectId
    context_id: Optional[PydanticObjectId]
    observation_input: Union[ViewObservationInput, SourceTableObservationInput]


class ObservationTableList(PaginationMixin):
    """
    Schema for listing observation tables
    """

    data: List[ObservationTableModel]
