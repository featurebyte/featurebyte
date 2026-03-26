"""
CalendarView class
"""

from __future__ import annotations

from typing import Any, ClassVar, Optional, cast

from pydantic import Field

from featurebyte.api.snapshots_helper import validate_offset_for_view
from featurebyte.api.view import RawMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.exception import JoinViewMismatchError
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.generic import SnapshotsDatetimeTransform
from featurebyte.query_graph.node.input import CalendarTableInputNodeParameters, InputNode
from featurebyte.typing import OffsetType


class CalendarViewColumn(ViewColumn):
    """
    CalendarViewColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()


class CalendarView(View, RawMixin):
    """
    A CalendarView object is a modified version of the CalendarTable object that provides additional
    capabilities for transforming data. With a CalendarView, you can create and transform columns
    and filter records prior to feature declaration. Calendar views can also be used to enrich views
    of other tables through joins.

    See Also
    --------
    - [calendar_table#get_view](/reference/featurebyte.api.calendar_table.CalendarTable.get_view/): get calendar view from a `CalendarTable`
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.CalendarView",
        skip_params_and_signature_in_class_docs=True,
    )
    _series_class: ClassVar[Any] = CalendarViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.CALENDAR_VIEW

    # pydantic instance variables
    series_id_column: Optional[str] = Field(
        frozen=True,
        default=None,
        description="Represents the entity identifier column of the calendar table.",
    )

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the calendar table

        Returns
        -------
        str
        """
        return self._get_calendar_table_node_parameters().calendar_datetime_column

    @property
    def calendar_datetime_column(self) -> str:
        """
        Calendar datetime column of the calendar table

        Returns
        -------
        str
        """
        return self._get_calendar_table_node_parameters().calendar_datetime_column

    @property
    def calendar_datetime_schema(self) -> TimestampSchema:
        """
        Calendar datetime schema of the calendar table

        Returns
        -------
        TimestampSchema
        """
        return self._get_calendar_table_node_parameters().calendar_datetime_schema

    def _get_calendar_table_node_parameters(self) -> CalendarTableInputNodeParameters:
        input_node = next(
            node
            for node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.INPUT)
            if cast(InputNode, node).parameters.type == TableDataType.CALENDAR_TABLE
        )
        return cast(CalendarTableInputNodeParameters, input_node.parameters)

    def _get_additional_inherited_columns(self) -> set[str]:
        return {self.calendar_datetime_column}

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        out = super().protected_attributes + ["calendar_datetime_column"]
        if self.series_id_column is not None:
            out.append("series_id_column")
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
        params.update({"series_id_column": self.series_id_column})
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
            raised when CalendarView is used as the left-hand side of a join
        """
        raise JoinViewMismatchError("CalendarView cannot be used as the left-hand side of a join")

    def _get_join_parameters(self, calling_view: View) -> dict[str, Any]:
        """
        Get join parameters when another view (left) triggered a join with CalendarView (right)

        Parameters
        ----------
        calling_view : View
            The view that is joining with this CalendarView

        Returns
        -------
        dict[str, Any]
            Dictionary containing join parameters including snapshots_datetime_join_keys

        Raises
        ------
        JoinViewMismatchError
            If joining a CalendarView to the given view type is not supported
        """
        from featurebyte.api.event_view import EventView
        from featurebyte.api.snapshots_view import SnapshotsView
        from featurebyte.api.time_series_view import TimeSeriesView

        left_view = calling_view
        params: dict[str, Any] = {
            "snapshots_datetime_join_keys": {
                "right_key": {
                    "column_name": self.calendar_datetime_column,
                    "transform": None,
                }
            }
        }
        # The join should transform the left view's (EventView, TimeSeriesView, etc) timestamp
        # column into its local time, truncated to day, and match with CalendarView's date column.
        if isinstance(left_view, EventView):
            original_timestamp_schema = left_view.event_timestamp_schema
            column_name = left_view.timestamp_column
        elif isinstance(left_view, TimeSeriesView):
            original_timestamp_schema = left_view.reference_datetime_schema
            column_name = left_view.reference_datetime_column
        elif isinstance(left_view, SnapshotsView):
            original_timestamp_schema = left_view.snapshot_datetime_schema
            column_name = left_view.snapshot_datetime_column
        else:
            raise JoinViewMismatchError(
                f"Joining a CalendarView to {type(left_view).__name__} is not supported"
            )
        transform = SnapshotsDatetimeTransform(
            original_timestamp_schema=original_timestamp_schema,
            snapshot_timezone_name=None,
            snapshot_time_interval=TimeInterval(unit="DAY", value=1),
            snapshot_format_string=self.calendar_datetime_schema.format_string,
            snapshot_feature_job_setting=None,
            allow_exact_match_with_current_interval=True,
            use_original_local_timezone=True,
        )
        params["snapshots_datetime_join_keys"]["left_key"] = {
            "column_name": column_name,
            "transform": transform,
        }
        return params

    def _get_join_column(self) -> Optional[str]:
        return self.series_id_column

    def validate_offset(self, offset: Optional[OffsetType]) -> None:
        validate_offset_for_view(offset, view_type_name="CalendarView")

    def get_additional_lookup_parameters(
        self, offset: Optional[OffsetType] = None
    ) -> dict[str, Any]:
        if offset is not None:
            assert isinstance(offset, int)
            offset_size = offset
        else:
            offset_size = None
        return {
            "calendar_parameters": {
                "calendar_datetime_column": self.calendar_datetime_column,
                "calendar_datetime_metadata": DBVarTypeMetadata(
                    timestamp_schema=self.calendar_datetime_schema,
                ),
                "offset_size": offset_size,
            }
        }
