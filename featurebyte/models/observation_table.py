"""
ObservationTableModel models
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Union

import pymongo
from bson import ObjectId
from pydantic import Field, StrictStr, field_validator
from typing_extensions import Annotated, Literal

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.models.request_input import (
    RequestInputType,
    SourceTableRequestInput,
    ViewRequestInput,
)
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.session.base import BaseSession


class ViewObservationInput(ViewRequestInput):
    """
    ViewObservationInput is a ViewRequestInput that is used to create an ObservationTableModel
    """

    definition: Optional[str] = Field(default=None)


class SourceTableObservationInput(SourceTableRequestInput):
    """
    SourceTableObservationInput is a SourceTableRequestInput that is used to create an ObservationTableModel
    """


class NoOpMaterializeMixin:
    """
    No-op materialize mixin adds a no-op materialize method to the class
    """

    async def materialize(
        self,
        session: BaseSession,
        destination: TableDetails,
        sample_rows: Optional[int],
        sample_from_timestamp: Optional[datetime] = None,
        sample_to_timestamp: Optional[datetime] = None,
        columns_to_exclude_missing_values: Optional[List[str]] = None,
        missing_data_table_details: Optional[TableDetails] = None,
    ) -> None:
        """
        No-op materialize. This method isn't needed for TargetInput since we materialize the target separately.
        As such, we can add a no-op version that throws an error if it is called. This is needed to satisfy the
        linter since some of the other classes under the `ObservationInput` union type have a materialize method.
        Will consider refactoring this in the future.

        Parameters
        ----------
        session: BaseSession
            The session to use to materialize the target input
        destination: TableDetails
            The destination table to materialize the target input to
        sample_rows: Optional[int]
            The number of rows to sample from the target input
        sample_from_timestamp: Optional[datetime]
            The timestamp to sample from
        sample_to_timestamp: Optional[datetime]
            The timestamp to sample to
        columns_to_exclude_missing_values: Optional[List[str]
            The columns to exclude missing values from
        missing_data_table_details: Optional[TableDetails]
            Missing data table details
        """


class TargetInput(FeatureByteBaseModel, NoOpMaterializeMixin):
    """
    TargetInput is an input from a target that can be used to create an ObservationTableModel
    """

    target_id: Optional[PydanticObjectId] = Field(default=None)
    observation_table_id: Optional[PydanticObjectId] = Field(default=None)
    type: Literal[RequestInputType.OBSERVATION_TABLE, RequestInputType.DATAFRAME]


class UploadedFileInput(FeatureByteBaseModel, NoOpMaterializeMixin):
    """
    UploadedFileInput is an input from an uploaded file that can be used to create an ObservationTableModel.
    """

    type: Literal[RequestInputType.UPLOADED_FILE]
    file_name: Optional[str] = Field(default=None)


ObservationInput = Annotated[
    Union[ViewObservationInput, SourceTableObservationInput, TargetInput, UploadedFileInput],
    Field(discriminator="type"),
]


class Purpose(StrEnum):
    """
    Purpose of the observation table
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Purpose")

    PREVIEW = "preview"
    EDA = "eda"
    TRAINING = "training"
    VALIDATION_TEST = "validation_test"
    OTHER = "other"


class ObservationTableModel(MaterializedTableModel):
    """
    ObservationTableModel is a table that can be used to request historical features

    request_input: ObservationInput
        The input that defines how the observation table is created
    context_id: Optional[PydanticObjectId]
        The id of the context that the observation table is associated with
    use_case_ids: Optional[List[PydanticObjectId]]
        The ids of the use cases that the observation table is associated with
    purpose: Optional[Purpose]
        The purpose of the observation table, which accepts one of: preview, eda, training, validation_test, other.
    primary_entity_ids: Optional[List[PydanticObjectId]]
        The ids of the primary entities the observation table is associated with
    """

    request_input: ObservationInput
    most_recent_point_in_time: StrictStr
    context_id: Optional[PydanticObjectId] = Field(default=None)
    use_case_ids: List[PydanticObjectId] = Field(default_factory=list)
    purpose: Optional[Purpose] = Field(default=None)
    least_recent_point_in_time: Optional[StrictStr] = Field(default=None)
    entity_column_name_to_count: Optional[Dict[str, int]] = Field(default_factory=dict)
    min_interval_secs_between_entities: Optional[float] = Field(default=None)
    primary_entity_ids: Optional[List[PydanticObjectId]] = Field(default_factory=list)
    has_row_index: Optional[bool] = Field(default=False)
    target_namespace_id: Optional[PydanticObjectId] = Field(default=None)
    sample_rows: Optional[int] = Field(default=None)
    sample_from_timestamp: Optional[datetime] = Field(default=None)
    sample_to_timestamp: Optional[datetime] = Field(default=None)
    table_with_missing_data: Optional[TableDetails] = Field(default=None)

    _sort_primary_entity_ids_validator = field_validator("primary_entity_ids")(
        construct_sort_validator()
    )

    @property
    def warehouse_tables(self) -> list[TableDetails]:
        tables = super().warehouse_tables
        if self.table_with_missing_data:
            tables.append(self.table_with_missing_data)
        return tables

    @property
    def target_id(self) -> Optional[ObjectId]:
        """
        The target id associated with the observation table

        Returns
        -------
        Optional[ObjectId]
            The target id associated with the observation table
        """
        if isinstance(self.request_input, TargetInput):
            return self.request_input.target_id
        return None

    def check_table_is_valid(self) -> bool:
        """
        Check if the observation table is valid

        Returns
        -------
        bool
            True if the observation table is valid, False otherwise
        """
        if isinstance(self.request_input, TargetInput):
            # NOTE: since the target table does not filter out missing data, we need to check if the table with
            # missing data exists or not. If it does exist, then the observation table is not valid.
            return self.table_with_missing_data is None
        return True

    @property
    def invalid_reason(self) -> Optional[str]:
        """
        Get the reason why the observation table is invalid

        Returns
        -------
        Optional[str]
            The reason why the observation table is invalid
        """
        if self.check_table_is_valid():
            return None

        if isinstance(self.request_input, TargetInput):
            reason = (
                "The table has missing data. Please check the table with missing data for more details. "
                "Please review the table for details and create a new table with all required data filled in."
            )
            return reason
        return ""

    @field_validator("most_recent_point_in_time", "least_recent_point_in_time")
    @classmethod
    def _validate_most_recent_point_in_time(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        # Check that most_recent_point_in_time is a valid ISO 8601 datetime
        _ = datetime.fromisoformat(value)
        return value

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "observation_table"

        indexes = MaterializedTableModel.Settings.indexes + [
            pymongo.operations.IndexModel("context_id"),
            pymongo.operations.IndexModel("use_case_ids"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
