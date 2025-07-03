"""
BatchFeatureTable API payload schema
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, List, Optional

from bson import ObjectId
from pydantic import BaseModel, Field, model_validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.models.batch_request_table import BatchRequestInput
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.materialized_table import BaseMaterializedTableListRecord


class BatchFeatureTableCreateBase(FeatureByteBaseModel):
    """
    BatchFeatureTableCreateBase creation payload
    """

    feature_store_id: PydanticObjectId
    batch_request_table_id: Optional[PydanticObjectId] = Field(default=None)
    request_input: Optional[BatchRequestInput] = Field(default=None)
    deployment_id: PydanticObjectId
    point_in_time: Optional[datetime] = Field(default=None)

    @model_validator(mode="after")
    def _validate_input(self) -> Any:
        if self.batch_request_table_id is None and self.request_input is None:
            raise ValueError("Either batch_request_table_id or request_input must be provided")
        if self.batch_request_table_id is not None and self.request_input is not None:
            raise ValueError("Only one of batch_request_table_id or request_input must be provided")
        return self


class BatchFeatureTableCreate(BatchFeatureTableCreateBase):
    """
    BatchFeatureTableCreate creation payload
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr


class OutputTableInfo(FeatureByteBaseModel):
    """
    Information about output table for batch feature table task
    """

    name: str = Field(
        default="batch_feature_table_output",
        description="Fully qualified name of the table to write features to",
    )
    snapshot_date_name: str = Field(
        default="snapshot_date",
        description="Name of the column that contains the snapshot date in the table",
    )
    snapshot_date: date = Field(
        default_factory=date.today,
        description="Snapshot date value to be used for the features to be written to the table",
    )


class BatchExternalFeatureTableCreate(BatchFeatureTableCreateBase):
    """
    BatchExternalFeatureTableCreate creation payload
    """

    output_table_info: OutputTableInfo


class BatchFeatureTableList(PaginationMixin):
    """
    Schema for listing batch feature tables
    """

    data: List[BatchFeatureTableModel]


class BatchFeatureTableListRecord(BaseMaterializedTableListRecord):
    """
    Schema for listing historical feature tables as a DataFrame
    """

    feature_store_id: PydanticObjectId
    batch_request_table_id: Optional[PydanticObjectId]

    @model_validator(mode="before")
    @classmethod
    def _extract(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        values["feature_store_id"] = values["location"]["feature_store_id"]
        return values
