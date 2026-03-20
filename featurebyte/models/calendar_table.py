"""
This module contains CalendarTable related models
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Tuple, Type

from pydantic import model_validator

from featurebyte.common.validator import ColumnToTimestampSchema, construct_data_model_validator
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.table import CalendarTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata


class CalendarTableModel(CalendarTableData, TableModel):
    """
    Model for CalendarTable

    id: PydanticObjectId
        CalendarTable id of the object
    name : str
        Name of the CalendarTable
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of calendar table columns
    series_id_column: Optional[str]
        Series ID column name (optional)
    calendar_datetime_column: str
        Calendar datetime column name
    calendar_datetime_schema: TimestampSchema
        Calendar datetime schema
    status : TableStatus
        Status of the CalendarTable
    created_at : Optional[datetime]
        Datetime when the CalendarTable was first saved or published
    updated_at: Optional[datetime]
        Datetime when the CalendarTable object was last updated
    """

    _table_data_class: ClassVar[Type[CalendarTableData]] = CalendarTableData

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("calendar_datetime_column", DBVarType.supported_ts_datetime_types()),
                ("calendar_datetime_schema", {DBVarType.VARCHAR}),
                ("record_creation_timestamp_column", DBVarType.supported_timestamp_types()),
                ("series_id_column", DBVarType.supported_id_types()),
            ],
            column_to_timestamp_schema_pairs=[
                ColumnToTimestampSchema("calendar_datetime_column", "calendar_datetime_schema"),
            ],
        ),
    )

    @model_validator(mode="after")
    def _validate_calendar_datetime_schema(self) -> "CalendarTableModel":
        schema = self.calendar_datetime_schema
        if schema.is_utc_time is True:
            raise ValueError(
                "calendar_datetime_schema: is_utc_time must not be True for CalendarTable"
            )
        if schema.timezone is not None:
            raise ValueError(
                "calendar_datetime_schema: timezone is not supported for CalendarTable"
            )
        return self

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.series_id_column] if self.series_id_column else []

    @property
    def special_columns(self) -> List[str]:
        cols = [
            self.calendar_datetime_column,
            self.series_id_column,
            self.record_creation_timestamp_column,
        ]
        return [col for col in cols if col]

    def create_view_graph_node(
        self, input_node: InputNode, metadata: ViewMetadata, **kwargs: Any
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        table_data = CalendarTableData(**self.model_dump(by_alias=True)).clone(
            column_cleaning_operations=metadata.column_cleaning_operations
        )
        return table_data.construct_calendar_view_graph_node(
            calendar_table_node=input_node,
            drop_column_names=metadata.drop_column_names,
            metadata=metadata,
        )
