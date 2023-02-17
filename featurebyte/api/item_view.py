"""
ItemView class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Optional, cast

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.item_data import ItemData
from featurebyte.api.join_utils import (
    append_rsuffix_to_column_info,
    append_rsuffix_to_columns,
    combine_column_info_of_views,
    filter_columns,
    join_tabular_data_ids,
)
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.exception import RepeatedColumnNamesError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import JoinNodeParameters
from featurebyte.query_graph.node.metadata.operation import DerivedDataColumn


class ItemViewColumn(ViewColumn):
    """
    ItemViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class ItemView(View, GroupByMixin):
    """
    ItemViews allow users to transform ItemData to support the data preparation necessary before creating features.

    When an ItemView is created, the event_timestamp and the entities of the event data the item data is associated
    with are automatically added. Users can join more columns from the event data if desired.

    Transformations supported are the same as for EventView except for:\n
    - lag (and inter event time) can be computed only for entities that are not inherited from the event data

    Features can be easily created from ItemViews in a similar way as for features created from EventViews, with a
    few differences:\n
    - features for the event_id and its children (item_id) are not time based. Features for other entities are time
      based like for EventViews.\n
    - columns imported from the event data or their derivatives can not be aggregated per an entity inherited from
      the event data. Those features should be engineered directly from the event data.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.ItemView",
    )

    # class variables
    _series_class = ItemViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.ITEM_VIEW

    # pydantic instance variables
    event_id_column: str = Field(allow_mutation=False)
    item_id_column: str = Field(allow_mutation=False)
    event_data_id: PydanticObjectId = Field(allow_mutation=False)
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(allow_mutation=False)
    event_view: EventView = Field(allow_mutation=False)
    timestamp_column_name: str = Field(allow_mutation=False)

    @classmethod
    def _validate_and_prepare_join_parameters(
        cls,
        item_view_columns: list[str],
        item_view_event_id_column: str,
        event_view: EventView,
        columns_to_join: list[str],
        event_suffix: Optional[str] = None,
    ) -> JoinNodeParameters:
        """
        Validate the join_event_data_attributes method & return the joined columns' name

        Parameters
        ----------
        item_view_columns: list[str]
            ItemView object's columns
        item_view_event_id_column: str
            ItemView object's event_id_column
        event_view: EventView
            EventView object
        columns_to_join: list[str]
            List of column names to be join from EventView
        event_suffix: Optional[str]
            Suffix to append to the column names from EventView

        Returns
        -------
        JoinNodeParameters

        Raises
        ------
        ValueError
            If the any of the provided columns does not exist in the EventData
        RepeatedColumnNamesError
            raised when there are overlapping columns, but no event_suffix has been provided
        """
        for col in columns_to_join:
            if col not in event_view.columns:
                raise ValueError(f"Column does not exist in EventData: {col}")

        # ItemData columns
        right_on = item_view_event_id_column
        right_input_columns = item_view_columns
        right_output_columns = item_view_columns

        # EventData columns
        left_on = cast(str, event_view.event_id_column)
        # EventData's event_id_column will be excluded from the result since that would be same as
        # ItemView's event_id_column. There is no need to specify event_suffix if the
        # event_id_column is the only common column name between EventData and ItemView.
        columns_excluding_event_id = filter_columns(columns_to_join, [left_on])
        renamed_event_view_columns = append_rsuffix_to_columns(
            columns_excluding_event_id, event_suffix
        )
        left_output_columns = renamed_event_view_columns

        repeated_column_names = sorted(set(right_output_columns).intersection(left_output_columns))
        if repeated_column_names:
            raise RepeatedColumnNamesError(
                f"Duplicate column names {repeated_column_names} found between EventData and"
                f" ItemView. Consider setting the event_suffix parameter to disambiguate the"
                f" resulting columns."
            )

        return JoinNodeParameters(
            left_on=left_on,
            right_on=right_on,
            left_input_columns=columns_excluding_event_id,
            left_output_columns=left_output_columns,
            right_input_columns=right_input_columns,
            right_output_columns=right_output_columns,
            join_type="inner",
        )

    @classmethod
    def _join_event_data_attributes(
        cls,
        item_view_columns_info: list[ColumnInfo],
        item_view_event_id_column: str,
        item_view_tabular_data_ids: list[PydanticObjectId],
        event_view: EventView,
        graph: QueryGraph | GraphNode,
        event_view_node: Node,
        item_view_node: Node,
        columns: list[str],
        event_suffix: Optional[str] = None,
    ) -> tuple[Node, list[ColumnInfo], list[PydanticObjectId], JoinNodeParameters]:
        join_parameters = cls._validate_and_prepare_join_parameters(
            item_view_columns=[col.name for col in item_view_columns_info],
            item_view_event_id_column=item_view_event_id_column,
            event_view=event_view,
            columns_to_join=columns,
            event_suffix=event_suffix,
        )

        node = graph.add_operation(
            node_type=NodeType.JOIN,
            node_params=join_parameters.dict(),
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[event_view_node, item_view_node],
        )

        # Construct new columns_info
        renamed_event_view_columns = join_parameters.left_output_columns
        joined_columns_info = combine_column_info_of_views(
            item_view_columns_info,
            append_rsuffix_to_column_info(event_view.columns_info, rsuffix=event_suffix),
            filter_set=set(renamed_event_view_columns),
        )

        # Construct new tabular_data_ids
        joined_tabular_data_ids = join_tabular_data_ids(
            item_view_tabular_data_ids, event_view.tabular_data_ids
        )
        return node, joined_columns_info, joined_tabular_data_ids, join_parameters

    @classmethod
    @typechecked
    def from_item_data(cls, item_data: ItemData, event_suffix: Optional[str] = None) -> ItemView:
        """
        Construct an ItemView object

        Parameters
        ----------
        item_data : ItemData
            ItemData object used to construct ItemView object
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventData

        Returns
        -------
        ItemView
            constructed ItemView object
        """
        event_data = EventData.get_by_id(item_data.event_data_id)
        event_view = EventView.from_event_data(event_data)

        # construct view graph node for item data, the final graph looks like:
        #     +-----------------------+    +----------------------------+
        #     | InputNode(type:event) | -->| GraphNode(type:event_view) | ---+
        #     +-----------------------+    +----------------------------+    |
        #                                                                    v
        #                            +----------------------+    +---------------------------+
        #                            | InputNode(type:item) | -->| GraphNode(type:item_view) |
        #                            +----------------------+    +---------------------------+
        view_graph_node, proxy_input_nodes, data_node = cls._construct_view_graph_node(
            data=item_data, other_input_nodes=[event_view.node]
        )
        item_view_node, event_view_node = proxy_input_nodes
        item_view_columns_info = cls._prepare_view_columns_info(data=item_data)
        (
            _,
            joined_columns_info,
            joined_tabular_data_ids,
            join_parameters,
        ) = cls._join_event_data_attributes(
            item_view_columns_info=item_view_columns_info,
            item_view_event_id_column=item_data.event_id_column,
            item_view_tabular_data_ids=[item_data.id],
            event_view=event_view,
            graph=view_graph_node,
            event_view_node=event_view_node,
            item_view_node=item_view_node,
            columns=[event_view.timestamp_column] + event_view.entity_columns,
            event_suffix=event_suffix,
        )
        inserted_graph_node = GlobalQueryGraph().add_node(
            node=view_graph_node, input_nodes=[data_node, event_view.node]
        )

        # left output columns is the renamed event timestamp column
        # (see `columns` parameter in above `_join_event_data_attributes` method)
        renamed_event_data_columns = join_parameters.left_output_columns

        return cls(
            feature_store=item_data.feature_store,
            tabular_source=item_data.tabular_source,
            columns_info=joined_columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=joined_tabular_data_ids,
            event_id_column=item_data.event_id_column,
            item_id_column=item_data.item_id_column,
            event_data_id=item_data.event_data_id,
            default_feature_job_setting=item_data.default_feature_job_setting,
            event_view=event_view,
            timestamp_column_name=renamed_event_data_columns[0],
        )

    def join_event_data_attributes(
        self,
        columns: list[str],
        event_suffix: Optional[str] = None,
    ) -> None:
        """
        Join additional attributes from the related EventData

        Parameters
        ----------
        columns : list[str]
            List of column names to include from the EventData
        event_suffix : Optional[str]
            A suffix to append on to the columns from the EventData
        """
        (
            node,
            joined_columns_info,
            joined_tabular_data_ids,
            join_parameters,
        ) = self._join_event_data_attributes(
            item_view_columns_info=self.columns_info,
            item_view_event_id_column=self.event_id_column,
            item_view_tabular_data_ids=self.tabular_data_ids,
            event_view=self.event_view,
            graph=self.graph,
            event_view_node=self.event_view.node,
            item_view_node=self.node,
            columns=columns,
            event_suffix=event_suffix,
        )

        # Update timestamp_column_name if event view's timestamp column is joined
        metadata_kwargs = {}
        for left_in_col, left_out_col in zip(
            join_parameters.left_input_columns, join_parameters.left_output_columns
        ):
            if left_in_col == self.event_view.timestamp_column:
                metadata_kwargs["timestamp_column_name"] = left_out_col

        # Update metadata only after validation is done & join node is inserted
        self._update_metadata(
            node.name, joined_columns_info, joined_tabular_data_ids, **metadata_kwargs
        )

    @property
    def timestamp_column(self) -> str:
        """
        Event timestamp column

        Returns
        -------
        """
        return self.timestamp_column_name

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + [
            "event_id_column",
            "item_id_column",
            "timestamp_column",
        ]

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update(
            {
                "event_id_column": self.event_id_column,
                "item_id_column": self.item_id_column,
                "event_data_id": self.event_data_id,
                "default_feature_job_setting": self.default_feature_job_setting,
                "event_view": self.event_view,
                "timestamp_column_name": self.timestamp_column_name,
            }
        )
        return params

    def validate_aggregate_over_parameters(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Check whether aggregate_over parameters are valid for ItemView.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            raised when groupby keys contains the event ID column
        """
        if self.event_id_column in keys:
            raise ValueError(
                f"GroupBy keys must NOT contain the event ID column ({self.event_id_column}) when performing "
                "aggregate_over functions."
            )

        self._assert_not_all_columns_are_from_event_data(keys, value_column)

    def validate_simple_aggregate_parameters(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Check whether aggregation parameters are valid for ItemView

        Columns imported from the EventData or their derivatives can not be aggregated per an entity
        inherited from the EventData. Those features should be engineered directly from the
        EventData.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            If aggregation is using an EventData derived column and the groupby key is an Entity
            from EventData
        """
        if self.event_id_column not in keys:
            raise ValueError(
                f"GroupBy keys must contain the event ID column ({self.event_id_column}) to prevent time leakage "
                "when performing simple aggregates."
            )

        self._assert_not_all_columns_are_from_event_data(keys, value_column)

    def _assert_not_all_columns_are_from_event_data(
        self, keys: list[str], value_column: Optional[str]
    ) -> None:
        """
        Helper method to validate whether columns are from event data.

        Parameters
        ----------
        keys: list[str]
            keys
        value_column: Optional[str]
            Column to be aggregated

        Raises
        ------
        ValueError
            raised when all columns are found in event data
        """
        if value_column is None:
            return

        columns_to_check = [*keys, value_column]
        if self._are_columns_derived_only_from_event_data(columns_to_check):
            raise ValueError(
                "Columns imported from EventData and their derivatives should be aggregated in"
                " EventView"
            )

    def _are_columns_derived_only_from_event_data(self, column_names: List[str]) -> bool:
        """
        Check if column is derived using only EventData's columns

        Parameters
        ----------
        column_names : List[str]
            Column names in ItemView to check

        Returns
        -------
        bool
        """
        operation_structure = self.graph.extract_operation_structure(self.node)
        for column_name in column_names:
            column_structure = next(
                column for column in operation_structure.columns if column.name == column_name
            )
            if isinstance(column_structure, DerivedDataColumn):
                if not all(
                    input_column.tabular_data_type == TableDataType.EVENT_DATA
                    for input_column in column_structure.columns
                ):
                    return False
                continue
            # column_structure is a SourceDataColumn
            if column_structure.tabular_data_type != TableDataType.EVENT_DATA:
                return False
        return True

    def get_join_column(self) -> str:
        return self.item_id_column

    def _get_as_feature_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        _ = offset
        return {
            "event_parameters": {
                "event_timestamp_column": self.timestamp_column,
            }
        }
