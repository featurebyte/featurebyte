"""
ObservationTableModel API payload schema
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr, field_validator, model_validator

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
    treatment_column: Optional[StrictStr] = Field(default=None)
    use_case_id: Optional[PydanticObjectId] = Field(default=None)

    # pydantic validators
    _sort_ids_validator = field_validator("primary_entity_ids")(construct_sort_validator())


class ObservationTableUpload(FeatureByteBaseModel):
    """
    ObservationTableUpload creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    purpose: Optional[Purpose] = Field(default=None)
    primary_entity_ids: Optional[List[PydanticObjectId]]
    target_column: Optional[StrictStr] = Field(default=None)
    treatment_column: Optional[StrictStr] = Field(default=None)
    context_id: Optional[PydanticObjectId] = Field(default=None)
    use_case_id: Optional[PydanticObjectId] = Field(default=None)

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


class ObservationTableSplit(FeatureByteBaseModel):
    """
    Schema for splitting an observation table into multiple non-overlapping tables
    """

    split_ratios: List[float] = Field(
        min_length=2,
        max_length=3,
        description="List of ratios for each split (must sum to 1.0). For example, [0.7, 0.3] for a 70/30 split.",
    )
    split_names: Optional[List[StrictStr]] = Field(
        default=None,
        description="Optional names for each split. If not provided, names will be auto-generated as '{source_table_name}_split_0', etc.",
    )
    seed: int = Field(
        default=1234,
        description="Random seed for reproducible splits",
    )

    @field_validator("split_ratios")
    @classmethod
    def _validate_split_ratios(cls, values: List[float]) -> List[float]:
        """Validate that ratios are valid and sum to 1.0"""
        for ratio in values:
            if ratio <= 0 or ratio > 1:
                raise ValueError(
                    f"Each split ratio must be between 0 and 1 (exclusive), got {ratio}"
                )
        total = sum(values)
        if abs(total - 1.0) > 1e-9:
            raise ValueError(f"Split ratios must sum to 1.0, got {total}")
        return values

    @model_validator(mode="after")
    def _validate_split_names_count(self) -> "ObservationTableSplit":
        """Validate that split_names count matches split_ratios count if provided"""
        if self.split_names is not None:
            if len(self.split_names) != len(self.split_ratios):
                raise ValueError(
                    f"Number of split_names ({len(self.split_names)}) must match number of split_ratios ({len(self.split_ratios)})"
                )
        return self
