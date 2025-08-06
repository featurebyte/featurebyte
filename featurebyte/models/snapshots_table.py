"""
This module contains SnapshotsTable related models
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Tuple, Type

from pydantic import Field, model_validator

from featurebyte.common.validator import ColumnToTimestampSchema, construct_data_model_validator
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.table import SnapshotsTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata


class SnapshotsTableModel(SnapshotsTableData, TableModel):
    """
    Model for SnapshotsTable

    id: PydanticObjectId
        TimeSeriesTable id of the object
    name : str
        Name of the TimeSeriesTable
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of time series table columns
    snapshot_id_column: Optional[str]
        Snapshot ID column name
    snapshot_datetime_column: str
        Snapshot datetime column name
    snapshot_datetime_schema: TimestampSchema
        Snapshot datetime schema
    time_interval: TimeInterval
        Time interval between consecutive records in each series
    default_feature_job_setting : Optional[FeatureJobSetting]
        Default feature job setting
    status : TableStatus
        Status of the TimeSeriesTable
    created_at : Optional[datetime]
        Datetime when the TimeSeriesTable was first saved or published
    updated_at: Optional[datetime]
        Datetime when the TimeSeriesTable object was last updated
    """

    default_feature_job_setting: Optional[CronFeatureJobSetting] = Field(default=None)
    _table_data_class: ClassVar[Type[SnapshotsTableData]] = SnapshotsTableData

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("snapshot_datetime_column", DBVarType.supported_ts_datetime_types()),
                ("snapshot_datetime_schema", {DBVarType.VARCHAR}),
                ("record_creation_timestamp_column", DBVarType.supported_timestamp_types()),
                ("snapshot_id_column", DBVarType.supported_id_types()),
            ],
            column_to_timestamp_schema_pairs=[
                ColumnToTimestampSchema("snapshot_datetime_column", "snapshot_datetime_schema"),
            ],
        ),
    )

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.snapshot_id_column]

    @property
    def special_columns(self) -> List[str]:
        cols = [
            self.snapshot_datetime_column,
            self.snapshot_id_column,
            self.record_creation_timestamp_column,
        ]
        return [col for col in cols if col]

    def create_view_graph_node(
        self, input_node: InputNode, metadata: ViewMetadata, **kwargs: Any
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        table_data = SnapshotsTableData(**self.model_dump(by_alias=True)).clone(
            column_cleaning_operations=metadata.column_cleaning_operations
        )
        return table_data.construct_snapshots_view_graph_node(
            snapshots_table_node=input_node,
            drop_column_names=metadata.drop_column_names,
            metadata=metadata,
        )
