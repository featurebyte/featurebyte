"""
This module contains ForecastTable related models
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Tuple, Type

from pydantic import model_validator

from featurebyte.common.validator import ColumnToTimestampSchema, construct_data_model_validator
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import TableModel
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.table import ForecastTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata


class ForecastTableModel(ForecastTableData, TableModel):
    """
    Model for ForecastTable

    id: PydanticObjectId
        ForecastTable id of the object
    name : str
        Name of the ForecastTable
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of forecast table columns
    natural_key_column: Optional[str]
        Natural key column name (optional)
    effective_timestamp_column: str
        Effective timestamp column name (SCD-style as-of lookup)
    effective_timestamp_schema: Optional[TimestampSchema]
        Effective timestamp schema
    forecast_timestamp_column: str
        Forecast horizon/point column name
    forecast_timestamp_schema: Optional[TimestampSchema]
        Forecast timestamp schema
    status : TableStatus
        Status of the ForecastTable
    created_at : Optional[datetime]
        Datetime when the ForecastTable was first saved or published
    updated_at: Optional[datetime]
        Datetime when the ForecastTable object was last updated
    """

    _table_data_class: ClassVar[Type[ForecastTableData]] = ForecastTableData

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="columns_info",
            expected_column_field_name_type_pairs=[
                ("natural_key_column", DBVarType.supported_id_types()),
                ("effective_timestamp_column", DBVarType.supported_datetime_types()),
                ("forecast_timestamp_column", DBVarType.supported_datetime_types()),
                ("record_creation_timestamp_column", DBVarType.supported_timestamp_types()),
            ],
            column_to_timestamp_schema_pairs=[
                ColumnToTimestampSchema("effective_timestamp_column", "effective_timestamp_schema"),
                ColumnToTimestampSchema("forecast_timestamp_column", "forecast_timestamp_schema"),
            ],
        ),
    )

    @property
    def special_columns(self) -> List[str]:
        cols = [
            self.natural_key_column,
            self.effective_timestamp_column,
            self.forecast_timestamp_column,
            self.record_creation_timestamp_column,
        ]
        return [col for col in cols if col]

    def create_view_graph_node(
        self, input_node: InputNode, metadata: ViewMetadata, **kwargs: Any
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        table_data = ForecastTableData(**self.model_dump(by_alias=True)).clone(
            column_cleaning_operations=metadata.column_cleaning_operations
        )
        return table_data.construct_forecast_view_graph_node(
            forecast_table_node=input_node,
            drop_column_names=metadata.drop_column_names,
            metadata=metadata,
        )
