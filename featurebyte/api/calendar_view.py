"""
CalendarView class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Optional, cast

from pydantic import Field

from featurebyte.api.view import RawMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.input import CalendarTableInputNodeParameters, InputNode

if TYPE_CHECKING:
    pass


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

    def _get_join_column(self) -> Optional[str]:
        return self.series_id_column
