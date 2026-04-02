"""
ForecastView class
"""

from __future__ import annotations

from typing import Any, ClassVar, Optional, cast

from pydantic import Field

from featurebyte.api.view import RawMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.exception import JoinViewMismatchError
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.input import ForecastTableInputNodeParameters, InputNode


class ForecastViewColumn(ViewColumn):
    """
    ForecastViewColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()


class ForecastView(View, RawMixin):
    """
    A ForecastView object is a modified version of the ForecastTable object that provides additional
    capabilities for transforming data. With a ForecastView, you can create and transform columns
    and filter records prior to feature declaration.

    See Also
    --------
    - [forecast_table#get_view](/reference/featurebyte.api.forecast_table.ForecastTable.get_view/): get forecast view from a `ForecastTable`
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.ForecastView",
        skip_params_and_signature_in_class_docs=True,
    )
    _series_class: ClassVar[Any] = ForecastViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.FORECAST_VIEW

    # pydantic instance variables
    natural_key_column: Optional[str] = Field(
        frozen=True,
        default=None,
        description="Represents the natural key column of the forecast table.",
    )

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the forecast table (effective timestamp)

        Returns
        -------
        str
        """
        return self._get_forecast_table_node_parameters().effective_timestamp_column

    @property
    def effective_timestamp_column(self) -> str:
        """
        Effective timestamp column of the forecast table

        Returns
        -------
        str
        """
        return self._get_forecast_table_node_parameters().effective_timestamp_column

    @property
    def effective_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Effective timestamp schema of the forecast table

        Returns
        -------
        Optional[TimestampSchema]
        """
        return self._get_forecast_table_node_parameters().effective_timestamp_schema

    @property
    def forecast_timestamp_column(self) -> str:
        """
        Forecast timestamp column of the forecast table

        Returns
        -------
        str
        """
        return self._get_forecast_table_node_parameters().forecast_timestamp_column

    @property
    def forecast_timestamp_schema(self) -> Optional[TimestampSchema]:
        """
        Forecast timestamp schema of the forecast table

        Returns
        -------
        Optional[TimestampSchema]
        """
        return self._get_forecast_table_node_parameters().forecast_timestamp_schema

    def _get_forecast_table_node_parameters(self) -> ForecastTableInputNodeParameters:
        input_node = next(
            node
            for node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.INPUT)
            if cast(InputNode, node).parameters.type == TableDataType.FORECAST_TABLE
        )
        return cast(ForecastTableInputNodeParameters, input_node.parameters)

    def _get_additional_inherited_columns(self) -> set[str]:
        return {self.effective_timestamp_column, self.forecast_timestamp_column}

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        out = super().protected_attributes + [
            "effective_timestamp_column",
            "forecast_timestamp_column",
        ]
        if self.natural_key_column is not None:
            out.append("natural_key_column")
        return out

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update({"natural_key_column": self.natural_key_column})
        return params

    def validate_join(self, other_view: View) -> None:
        """
        Validate join should be implemented by view classes that have extra requirements.

        Parameters
        ----------
        other_view: View
            the other view that we are joining with

        Raises
        ------
        JoinViewMismatchError
            raised when ForecastView is used as the left-hand side of a join
        """
        raise JoinViewMismatchError("ForecastView cannot be used as the left-hand side of a join")

    def _get_join_column(self) -> Optional[str]:
        return self.natural_key_column
