"""
ForecastTable class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Type, cast

from bson import ObjectId
from pydantic import Field, StrictStr, model_validator
from typing_extensions import Literal

from featurebyte.api.base_table import TableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.forecast_table import ForecastTableModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.table import (
    AllTableDataT,
    ForecastTableData,
)
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata
from featurebyte.schema.forecast_table import ForecastTableCreate, ForecastTableUpdate

if TYPE_CHECKING:
    from featurebyte.api.forecast_view import ForecastView


class ForecastTable(TableApiObject):
    """
    A ForecastTable object represents a table of forecasted values indexed by both a
    point-in-time (effective timestamp) and a forecast horizon (forecast timestamp).
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.ForecastTable",
        skip_params_and_signature_in_class_docs=True,
    )
    _route: ClassVar[str] = "/forecast_table"
    _update_schema_class: ClassVar[Any] = ForecastTableUpdate
    _create_schema_class: ClassVar[Any] = ForecastTableCreate
    _get_schema: ClassVar[Any] = ForecastTableModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = ForecastTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.FORECAST_TABLE] = TableDataType.FORECAST_TABLE

    # pydantic instance variable (internal use)
    internal_natural_key_column: Optional[StrictStr] = Field(
        alias="natural_key_column", default=None
    )
    internal_effective_timestamp_column: StrictStr = Field(alias="effective_timestamp_column")
    internal_effective_timestamp_schema: Optional[TimestampSchema] = Field(
        alias="effective_timestamp_schema", default=None
    )
    internal_forecast_timestamp_column: StrictStr = Field(alias="forecast_timestamp_column")
    internal_forecast_timestamp_schema: Optional[TimestampSchema] = Field(
        alias="forecast_timestamp_schema", default=None
    )

    # pydantic validators
    _model_validator = model_validator(mode="after")(
        construct_data_model_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                (
                    "internal_record_creation_timestamp_column",
                    DBVarType.supported_timestamp_types(),
                ),
                (
                    "internal_natural_key_column",
                    DBVarType.supported_id_types(),
                ),
                (
                    "internal_effective_timestamp_column",
                    DBVarType.supported_datetime_types(),
                ),
                (
                    "internal_forecast_timestamp_column",
                    DBVarType.supported_datetime_types(),
                ),
            ],
        )
    )

    def get_view(
        self,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> ForecastView:
        """
        Gets a ForecastView object from a ForecastTable object.

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use. When auto, the view will be constructed with cleaning operations from
            the table, and the record creation timestamp column will be dropped.
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only).
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            List of cleaning operations to apply per column in manual mode only. Each element in
            the list indicates the cleaning operations for a specific column.

        Returns
        -------
        ForecastView
            ForecastView object constructed from the source table.

        Examples
        --------
        Get a ForecastView in automated mode.

        >>> forecast_table = catalog.get_table("FORECAST")  # doctest: +SKIP
        >>> forecast_view = forecast_table.get_view()  # doctest: +SKIP
        """
        from featurebyte.api.forecast_view import ForecastView

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and self.record_creation_timestamp_column:
            drop_column_names.append(self.record_creation_timestamp_column)

        data_node = self.frame.node
        assert isinstance(data_node, InputNode)
        forecast_table_data = cast(ForecastTableData, self.table_data)
        (
            forecast_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=forecast_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        view_graph_node, columns_info = forecast_table_data.construct_forecast_view_graph_node(
            forecast_table_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                table_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        columns_info = self._prepare_columns_info_for_view(
            view_node=inserted_graph_node, columns_info=columns_info
        )
        return ForecastView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            natural_key_column=self.natural_key_column,
        )

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column name of the ForecastTable (effective timestamp)

        Returns
        -------
        Optional[str]
        """
        return self.effective_timestamp_column

    @property
    def natural_key_column(self) -> Optional[str]:
        """
        Natural key column name of the ForecastTable

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.natural_key_column
        except RecordRetrievalException:
            return self.internal_natural_key_column

    @property
    def effective_timestamp_column(self) -> str:
        """
        Effective timestamp column name of the ForecastTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.effective_timestamp_column
        except RecordRetrievalException:
            return self.internal_effective_timestamp_column

    @property
    def effective_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Schema of the effective timestamp column

        Returns
        -------
        Optional[TimestampSchema]
        """
        try:
            return self.cached_model.effective_timestamp_schema
        except RecordRetrievalException:
            return self.internal_effective_timestamp_schema

    @property
    def forecast_timestamp_column(self) -> str:
        """
        Forecast timestamp column name of the ForecastTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.forecast_timestamp_column
        except RecordRetrievalException:
            return self.internal_forecast_timestamp_column

    @property
    def forecast_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Schema of the forecast timestamp column

        Returns
        -------
        Optional[TimestampSchema]
        """
        try:
            return self.cached_model.forecast_timestamp_schema
        except RecordRetrievalException:
            return self.internal_forecast_timestamp_schema

    @classmethod
    def get_by_id(
        cls,
        id: ObjectId,
    ) -> ForecastTable:
        """
        Returns a ForecastTable object by its unique identifier (ID).

        Parameters
        ----------
        id: ObjectId
            ForecastTable unique identifier ID.

        Returns
        -------
        ForecastTable
            ForecastTable object.

        Examples
        --------
        Get a ForecastTable object that is already saved.

        >>> fb.ForecastTable.get_by_id(<forecast_table_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)
