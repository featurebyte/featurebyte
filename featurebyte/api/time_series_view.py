"""
TimeSeriesView class
"""

from __future__ import annotations

from typing import Any, ClassVar, Optional, cast

from pydantic import Field

from featurebyte.api.lag import LaggableViewColumn
from featurebyte.api.view import GroupByMixin, RawMixin, View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
from featurebyte.query_graph.node.input import InputNode, TimeSeriesTableInputNodeParameters


class TimeSeriesViewColumn(LaggableViewColumn):
    """
    TimeSeriesViewColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()


class TimeSeriesView(View, GroupByMixin, RawMixin):
    """
    An TimeSeriesView object is a modified version of the TimeSeriesTable object that provides additional capabilities for
    transforming data. With an TimeSeriesView, you can create and transform columns, extract lags and filter records
    prior to feature declaration.

    See Also
    --------
    - [time_series_table#get_view](/reference/featurebyte.api.time_series_table.TimeSeriesTable.get_view/): get time series view from an `TimeSeriesTable`
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.TimeSeriesView",
        skip_params_and_signature_in_class_docs=True,
    )
    _series_class: ClassVar[Any] = TimeSeriesViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.TIME_SERIES_VIEW

    # pydantic instance variables
    default_feature_job_setting: Optional[CronFeatureJobSetting] = Field(
        frozen=True,
        description="Returns the default feature job setting for the view.\n\n"
        "The Default Feature Job Setting establishes the default setting used by "
        "features that aggregate data in the view, ensuring consistency of the "
        "Feature Job Setting across features created by different team members. "
        "While it's possible to override the setting during feature declaration, "
        "using the Default Feature Job Setting simplifies the process of setting "
        "up the Feature Job Setting for each feature.",
    )
    series_id_column: Optional[str] = Field(
        frozen=True,
        description="Returns the name of the column representing the series key of the Time Series view.",
    )

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the time series table

        Returns
        -------
        str
        """
        return self._get_time_series_table_node_parameters().reference_datetime_column

    @property
    def reference_datetime_column(self) -> str:
        """
        Reference datetime column of the time series table

        Returns
        -------
        str
        """
        return self._get_time_series_table_node_parameters().reference_datetime_column

    @property
    def reference_datetime_schema(self) -> TimestampSchema:
        """
        Reference datetime schema of the time series table

        Returns
        -------
        TimestampSchema
        """
        return self._get_time_series_table_node_parameters().reference_datetime_schema

    @property
    def time_interval(self) -> TimeInterval:
        """
        Time interval of the time series table

        Returns
        -------
        TimeInterval
        """
        return self._get_time_series_table_node_parameters().time_interval

    def _get_time_series_table_node_parameters(self) -> TimeSeriesTableInputNodeParameters:
        input_node = next(
            node
            for node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.INPUT)
            if cast(InputNode, node).parameters.type == TableDataType.TIME_SERIES_TABLE
        )
        return cast(TimeSeriesTableInputNodeParameters, input_node.parameters)

    def _get_additional_inherited_columns(self) -> set[str]:
        columns = {self.timestamp_column}
        if self.reference_datetime_schema is not None:
            if isinstance(self.reference_datetime_schema.timezone, TimeZoneColumn):
                columns.add(self.reference_datetime_schema.timezone.column_name)
        return columns

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        out = super().protected_attributes + ["reference_datetime_column"]
        if self.reference_datetime_schema is not None:
            out.append("reference_datetime_schema")
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
        params.update({
            "default_feature_job_setting": self.default_feature_job_setting,
            "series_id_column": self.series_id_column,
        })
        return params

    def get_join_column(self) -> str:
        join_column = self._get_join_column()
        assert join_column is not None, "Series ID column is not available."
        return join_column

    def _get_join_column(self) -> Optional[str]:
        return self.series_id_column

    def get_additional_lookup_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        _ = offset
        return {
            "time_series_parameters": {
                "reference_datetime_column": self.reference_datetime_column,
            }
        }
