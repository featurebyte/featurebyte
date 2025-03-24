"""
This module contains specialized table related models.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from bson import ObjectId
from pydantic import Field, StrictStr, field_validator
from typing_extensions import Annotated, Literal

from featurebyte.common.join_utils import (
    apply_column_name_modifiers,
    apply_column_name_modifiers_columns_info,
    combine_column_info_of_views,
    filter_columns,
)
from featurebyte.common.model_util import construct_serialize_function
from featurebyte.enum import TableDataType
from featurebyte.exception import RepeatedColumnNamesError
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import (
    DATA_TABLES,
    SPECIFIC_DATA_TABLES,
    BaseTableData,
)
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    JoinEventTableAttributesMetadata,
    JoinNodeParameters,
)
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ChangeViewMetadata, ItemViewMetadata, ViewMetadata
from featurebyte.query_graph.node.schema import FeatureStoreDetails


class SourceTableData(BaseTableData):
    """SourceTableData class"""

    type: Literal[TableDataType.SOURCE_TABLE] = TableDataType.SOURCE_TABLE

    @property
    def primary_key_columns(self) -> List[str]:
        return []

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": None,
                "feature_store_details": {"type": feature_store_details.type},
                **self._get_common_input_node_parameters(),
            },
        )


class EventTableData(BaseTableData):
    """EventTableData class"""

    type: Literal[TableDataType.EVENT_TABLE] = TableDataType.EVENT_TABLE
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    event_timestamp_column: StrictStr
    event_id_column: Optional[StrictStr]
    event_timestamp_timezone_offset: Optional[StrictStr] = Field(default=None)
    event_timestamp_timezone_offset_column: Optional[StrictStr] = Field(default=None)
    event_timestamp_schema: Optional[TimestampSchema] = Field(default=None)

    @property
    def primary_key_columns(self) -> List[str]:
        if self.event_id_column:
            return [self.event_id_column]
        return []  # DEV-556: event_id_column should not be empty

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "timestamp_column": self.event_timestamp_column,
                "id_column": self.event_id_column,
                "feature_store_details": {"type": feature_store_details.type},
                "event_timestamp_timezone_offset": self.event_timestamp_timezone_offset,
                "event_timestamp_timezone_offset_column": self.event_timestamp_timezone_offset_column,
                "event_timestamp_schema": self.event_timestamp_schema,
                **self._get_common_input_node_parameters(),
            },
        )

    def construct_event_view_graph_node(
        self,
        event_table_node: InputNode,
        drop_column_names: List[str],
        metadata: ViewMetadata,
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        """
        Construct a graph node & columns info for EventView of this event table.

        Parameters
        ----------
        event_table_node: InputNode
            Event table node
        drop_column_names: List[str]
            List of column names to drop from the event table
        metadata: ViewMetadata
            Metadata to be added to the graph node

        Returns
        -------
        Tuple[GraphNode, List[ColumnInfo]]
        """
        view_graph_node, _ = self.construct_view_graph_node(
            graph_node_type=GraphNodeType.EVENT_VIEW,
            data_node=event_table_node,
            other_input_nodes=[],
            drop_column_names=drop_column_names,
            metadata=metadata,
        )
        columns_info = self.prepare_view_columns_info(drop_column_names=drop_column_names)
        return view_graph_node, columns_info


class ItemTableData(BaseTableData):
    """ItemTableData class"""

    type: Literal[TableDataType.ITEM_TABLE] = TableDataType.ITEM_TABLE
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    event_id_column: StrictStr
    item_id_column: Optional[StrictStr]
    event_table_id: PydanticObjectId

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.item_id_column] if self.item_id_column else []

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "id_column": self.item_id_column,
                "event_table_id": self.event_table_id,
                "event_id_column": self.event_id_column,
                "feature_store_details": {"type": feature_store_details.type},
                **self._get_common_input_node_parameters(),
            },
        )

    @classmethod
    def _prepare_join_with_event_view_columns_parameters(
        cls,
        item_view_columns: List[str],
        item_view_event_id_column: str,
        event_view_columns: List[str],
        event_view_event_id_column: str,
        columns_to_join: List[str],
        event_suffix: Optional[str] = None,
    ) -> JoinNodeParameters:
        for col in columns_to_join:
            if col not in event_view_columns:
                raise ValueError(f"Column does not exist in EventTable: {col}")

        # ItemTable columns
        left_on = item_view_event_id_column
        left_input_columns = item_view_columns
        left_output_columns = item_view_columns

        # EventTable columns
        right_on = event_view_event_id_column
        # EventTable's event_id_column will be excluded from the result since that would be same as
        # ItemView's event_id_column. There is no need to specify event_suffix if the
        # event_id_column is the only common column name between EventTable and ItemView.
        columns_excluding_event_id = filter_columns(columns_to_join, [right_on])
        renamed_event_view_columns = apply_column_name_modifiers(
            columns_excluding_event_id, rsuffix=event_suffix, rprefix=None
        )
        right_output_columns = renamed_event_view_columns

        repeated_column_names = sorted(set(left_output_columns).intersection(right_output_columns))
        if repeated_column_names:
            raise RepeatedColumnNamesError(
                f"Duplicate column names {repeated_column_names} found between EventTable and"
                f" ItemView. Consider setting the event_suffix parameter to disambiguate the"
                f" resulting columns."
            )

        return JoinNodeParameters(
            left_on=left_on,
            right_on=right_on,
            left_input_columns=left_input_columns,
            left_output_columns=left_output_columns,
            right_input_columns=columns_excluding_event_id,
            right_output_columns=right_output_columns,
            join_type="inner",
            metadata=JoinEventTableAttributesMetadata(
                columns=columns_to_join,
                event_suffix=event_suffix,
            ),
        )

    @classmethod
    def join_event_view_columns(
        cls,
        graph: Union[QueryGraph, GraphNode],
        item_view_node: Node,
        item_view_columns_info: List[ColumnInfo],
        item_view_event_id_column: str,
        event_view_node: Node,
        event_view_columns_info: List[ColumnInfo],
        event_view_event_id_column: str,
        event_suffix: Optional[str],
        columns_to_join: List[str],
    ) -> Tuple[Node, List[ColumnInfo], JoinNodeParameters]:
        """
        Join EventView columns to ItemView

        Parameters
        ----------
        graph: Union[QueryGraph, GraphNode]
            QueryGraph or GraphNode to add the join operation to
        item_view_node: Node
            ItemView node
        item_view_columns_info: List[ColumnInfo]
            ItemView columns info
        item_view_event_id_column: str
            ItemView event_id_column name
        event_view_node: Node
            EventView node
        event_view_columns_info: List[ColumnInfo]
            EventView columns info
        event_view_event_id_column: str
            EventView event_id_column name
        event_suffix: Optional[str]
            Suffix to append to joined EventView columns
        columns_to_join: List[str]
            List of columns to join from EventView

        Returns
        -------
        Tuple[Node, List[ColumnInfo], JoinNodeParameters]
        """
        join_parameters = cls._prepare_join_with_event_view_columns_parameters(
            item_view_columns=[col.name for col in item_view_columns_info],
            item_view_event_id_column=item_view_event_id_column,
            event_view_columns=[col.name for col in event_view_columns_info],
            event_view_event_id_column=event_view_event_id_column,
            columns_to_join=columns_to_join,
            event_suffix=event_suffix,
        )
        node = graph.add_operation(
            node_type=NodeType.JOIN,
            node_params=join_parameters.model_dump(),
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[item_view_node, event_view_node],
        )
        # Construct new columns_info
        renamed_event_view_columns = join_parameters.right_output_columns
        joined_columns_info = combine_column_info_of_views(
            item_view_columns_info,
            apply_column_name_modifiers_columns_info(
                event_view_columns_info, rsuffix=event_suffix, rprefix=None
            ),
            filter_set=set(renamed_event_view_columns),
        )
        return node, joined_columns_info, join_parameters

    def construct_item_view_graph_node(
        self,
        item_table_node: InputNode,
        columns_to_join: List[str],
        event_view_node: Node,
        event_view_columns_info: List[ColumnInfo],
        event_view_event_id_column: str,
        event_suffix: Optional[str],
        drop_column_names: List[str],
        metadata: ItemViewMetadata,
        to_auto_resolve_column_conflict: bool = False,
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        """
        Construct ItemView graph node

        Parameters
        ----------
        item_table_node: InputNode
            Item table node
        columns_to_join: List[str]
            List of columns to join from EventView
        event_view_node: Node
            EventView node
        event_view_columns_info: List[ColumnInfo]
            EventView columns info
        event_view_event_id_column: str
            EventView event_id_column name
        event_suffix: Optional[str]
            Suffix to append to joined EventView columns
        drop_column_names: List[str]
            List of columns to drop from the item table
        metadata: ItemViewMetadata
            Metadata to add to the graph node
        to_auto_resolve_column_conflict: bool
            Flag to auto resolve column conflicts

        Returns
        -------
        Tuple[GraphNode, List[ColumnInfo]]
        """
        if to_auto_resolve_column_conflict:
            item_table_columns = set(col.name for col in item_table_node.parameters.columns)
            has_conflict = item_table_columns.intersection(metadata.event_join_column_names)
            if has_conflict:
                columns_to_join = [
                    col for col in metadata.event_join_column_names if col not in item_table_columns
                ]
                metadata.event_join_column_names = columns_to_join

        view_graph_node, proxy_input_nodes = self.construct_view_graph_node(
            graph_node_type=GraphNodeType.ITEM_VIEW,
            data_node=item_table_node,
            other_input_nodes=[event_view_node],
            drop_column_names=drop_column_names,
            metadata=metadata,
        )
        (
            proxy_item_table_node,
            proxy_event_view_node,
        ) = proxy_input_nodes
        item_view_columns_info = self.prepare_view_columns_info(drop_column_names=drop_column_names)
        _, columns_info, _ = self.join_event_view_columns(
            graph=view_graph_node,
            item_view_node=proxy_item_table_node,
            item_view_columns_info=item_view_columns_info,
            item_view_event_id_column=self.event_id_column,
            event_view_node=proxy_event_view_node,
            event_view_columns_info=event_view_columns_info,
            event_view_event_id_column=event_view_event_id_column,
            columns_to_join=columns_to_join,
            event_suffix=event_suffix,
        )

        return view_graph_node, columns_info


class DimensionTableData(BaseTableData):
    """DimensionTableData class"""

    type: Literal[TableDataType.DIMENSION_TABLE] = TableDataType.DIMENSION_TABLE
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    dimension_id_column: StrictStr

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.dimension_id_column]

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "id_column": self.dimension_id_column,
                "feature_store_details": {"type": feature_store_details.type},
                **self._get_common_input_node_parameters(),
            },
        )

    def construct_dimension_view_graph_node(
        self,
        dimension_table_node: InputNode,
        drop_column_names: List[str],
        metadata: ViewMetadata,
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        """
        Construct DimensionView graph node

        Parameters
        ----------
        dimension_table_node: InputNode
            Dimension table node
        drop_column_names: List[str]
            List of columns to drop from the dimension table
        metadata: ViewMetadata
            Metadata to add to the graph node

        Returns
        -------
        Tuple[GraphNode, List[ColumnInfo]]
        """
        view_graph_node, _ = self.construct_view_graph_node(
            graph_node_type=GraphNodeType.DIMENSION_VIEW,
            data_node=dimension_table_node,
            other_input_nodes=[],
            drop_column_names=drop_column_names,
            metadata=metadata,
        )
        columns_info = self.prepare_view_columns_info(drop_column_names=drop_column_names)
        return view_graph_node, columns_info


@dataclass
class ChangeViewColumnNames:
    """
    Representation of column names to use in the change view
    """

    previous_tracked_column_name: str
    new_tracked_column_name: str
    previous_valid_from_column_name: str
    new_valid_from_column_name: str


class SCDTableData(BaseTableData):
    """SCDTableData class"""

    type: Literal[TableDataType.SCD_TABLE] = TableDataType.SCD_TABLE
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    natural_key_column: Optional[StrictStr]
    effective_timestamp_column: StrictStr
    surrogate_key_column: Optional[StrictStr]
    end_timestamp_column: Optional[StrictStr] = Field(default=None)
    current_flag_column: Optional[StrictStr] = Field(default=None)
    effective_timestamp_schema: Optional[TimestampSchema] = Field(default=None)
    end_timestamp_schema: Optional[TimestampSchema] = Field(default=None)

    @property
    def primary_key_columns(self) -> List[str]:
        return [self.natural_key_column] if self.natural_key_column else []

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "natural_key_column": self.natural_key_column,
                "effective_timestamp_column": self.effective_timestamp_column,
                "surrogate_key_column": self.surrogate_key_column,
                "end_timestamp_column": self.end_timestamp_column,
                "current_flag_column": self.current_flag_column,
                "feature_store_details": {"type": feature_store_details.type},
                **self._get_common_input_node_parameters(),
            },
        )

    def construct_scd_view_graph_node(
        self,
        scd_table_node: InputNode,
        drop_column_names: List[str],
        metadata: ViewMetadata,
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        """
        Construct SCDView graph node

        Parameters
        ----------
        scd_table_node: InputNode
            Slowly changing dimension table node
        drop_column_names: List[str]
            List of columns to drop from the SCD table
        metadata: ViewMetadata
            Metadata to add to the graph node

        Returns
        -------
        Tuple[GraphNode, List[ColumnInfo]]
        """
        view_graph_node, _ = self.construct_view_graph_node(
            graph_node_type=GraphNodeType.SCD_VIEW,
            data_node=scd_table_node,
            other_input_nodes=[],
            drop_column_names=drop_column_names,
            metadata=metadata,
        )
        columns_info = self.prepare_view_columns_info(drop_column_names=drop_column_names)
        return view_graph_node, columns_info

    @staticmethod
    def get_new_column_names(
        tracked_column: str,
        timestamp_column: str,
        prefixes: Optional[Tuple[Optional[str], Optional[str]]],
    ) -> ChangeViewColumnNames:
        """
        Helper method to return the tracked column names.

        Parameters
        ----------
        tracked_column: str
            column we want to track
        timestamp_column: str
            column denoting the timestamp
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.

        Returns
        -------
        ChangeViewColumnNames
            column names to use in the change view
        """
        old_prefix = "past_"
        new_prefix = "new_"
        if prefixes is not None:
            if prefixes[0] is not None:
                old_prefix = prefixes[0]
            if prefixes[1] is not None:
                new_prefix = prefixes[1]

        past_col_name = f"{old_prefix}{tracked_column}"
        new_col_name = f"{new_prefix}{tracked_column}"
        past_timestamp_col_name = f"{old_prefix}{timestamp_column}"
        new_timestamp_col_name = f"{new_prefix}{timestamp_column}"
        return ChangeViewColumnNames(
            previous_tracked_column_name=past_col_name,
            new_tracked_column_name=new_col_name,
            previous_valid_from_column_name=past_timestamp_col_name,
            new_valid_from_column_name=new_timestamp_col_name,
        )

    def _add_change_view_operations(
        self,
        view_graph_node: GraphNode,
        track_changes_column: str,
        proxy_input_nodes: List[Node],
        column_names: ChangeViewColumnNames,
        effective_timestamp_metadata: Optional[DBVarTypeMetadata],
    ) -> GraphNode:
        frame_node = proxy_input_nodes[0]
        view_graph_node.add_operation(
            node_type=NodeType.TRACK_CHANGES,
            node_params={
                "natural_key_column": self.natural_key_column,
                "effective_timestamp_column": self.effective_timestamp_column,
                "tracked_column": track_changes_column,
                "previous_tracked_column_name": column_names.previous_tracked_column_name,
                "new_tracked_column_name": column_names.new_tracked_column_name,
                "previous_valid_from_column_name": column_names.previous_valid_from_column_name,
                "new_valid_from_column_name": column_names.new_valid_from_column_name,
                "effective_timestamp_metadata": effective_timestamp_metadata,
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[frame_node],
        )
        return view_graph_node

    def _prepare_change_view_columns_info(
        self,
        column_names: ChangeViewColumnNames,
        track_changes_column: str,
    ) -> List[ColumnInfo]:
        time_col_info, track_col_info, natural_key_col_info = None, None, None
        for col in self.columns_info:
            if col.name == self.natural_key_column:
                natural_key_col_info = col
            if col.name == self.effective_timestamp_column:
                time_col_info = col
            if col.name == track_changes_column:
                track_col_info = col

        assert time_col_info, f"{self.effective_timestamp_column} is not in columns_info"
        assert track_col_info, f"{track_changes_column} is not in columns_info"
        assert natural_key_col_info, f"{self.natural_key_column} is not in columns_info"
        return [
            natural_key_col_info,
            ColumnInfo(**{
                **time_col_info.model_dump(),
                "name": column_names.new_valid_from_column_name,
            }),
            ColumnInfo(
                name=column_names.previous_valid_from_column_name, dtype=time_col_info.dtype
            ),
            ColumnInfo(**{
                **track_col_info.model_dump(),
                "name": column_names.new_tracked_column_name,
            }),
            ColumnInfo(name=column_names.previous_tracked_column_name, dtype=track_col_info.dtype),
        ]

    def construct_change_view_graph_node(
        self,
        scd_table_node: InputNode,
        track_changes_column: str,
        prefixes: Optional[Tuple[Optional[str], Optional[str]]],
        drop_column_names: List[str],
        metadata: ChangeViewMetadata,
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        """
        Construct a graph node for a change view.

        Parameters
        ----------
        scd_table_node: InputNode
            Slowly changing dimension table node
        track_changes_column: str
            Column name of the column that tracks changes
        prefixes: Tuple[Optional[str], Optional[str]]
            Prefixes for the new and previous columns
        drop_column_names: List[str]
            Column names to drop from the slow changing dimension table
        metadata: ChangeViewMetadata
            Metadata for the graph node

        Returns
        -------
        Tuple[GraphNode, List[ColumnInfo]]
        """
        view_graph_node, proxy_input_nodes = self.construct_view_graph_node(
            graph_node_type=GraphNodeType.CHANGE_VIEW,
            data_node=scd_table_node,
            other_input_nodes=[],
            drop_column_names=drop_column_names,
            metadata=metadata,
        )
        column_names = self.get_new_column_names(
            tracked_column=track_changes_column,
            timestamp_column=self.effective_timestamp_column,
            prefixes=prefixes,
        )
        effective_timestamp_metadata = None
        for col in self.columns_info:
            if col.name == self.effective_timestamp_column and col.dtype_metadata:
                effective_timestamp_metadata = col.dtype_metadata
        view_graph_node = self._add_change_view_operations(
            view_graph_node=view_graph_node,
            track_changes_column=track_changes_column,
            proxy_input_nodes=proxy_input_nodes,
            column_names=column_names,
            effective_timestamp_metadata=effective_timestamp_metadata,
        )
        columns_info = self._prepare_change_view_columns_info(
            column_names=column_names, track_changes_column=track_changes_column
        )
        return view_graph_node, columns_info


class TimeSeriesTableData(BaseTableData):
    """TimeSeriesTableData class"""

    type: Literal[TableDataType.TIME_SERIES_TABLE] = TableDataType.TIME_SERIES_TABLE
    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    series_id_column: Optional[StrictStr]
    reference_datetime_column: StrictStr
    reference_datetime_schema: TimestampSchema
    time_interval: TimeInterval

    @property
    def primary_key_columns(self) -> List[str]:
        if self.series_id_column:
            return [self.series_id_column]
        return []

    @field_validator("time_interval")
    def validate_time_interval(cls, value: TimeInterval) -> TimeInterval:
        if value.value != 1:
            raise ValueError(
                "Only intervals defined with a single time unit (e.g., 1 hour, 1 day) are supported."
            )
        return value

    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        return InputNode(
            name="temp",
            parameters={
                "id": self.id,
                "id_column": self.series_id_column,
                "feature_store_details": {"type": feature_store_details.type},
                "reference_datetime_column": self.reference_datetime_column,
                "reference_datetime_schema": self.reference_datetime_schema,
                "time_interval": self.time_interval,
                **self._get_common_input_node_parameters(),
            },
        )

    def construct_time_series_view_graph_node(
        self,
        time_series_table_node: InputNode,
        drop_column_names: List[str],
        metadata: ViewMetadata,
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        """
        Construct a graph node & columns info for TimeSeriesView of this time series table.

        Parameters
        ----------
        time_series_table_node: InputNode
            TimeSeries table node
        drop_column_names: List[str]
            List of column names to drop from the  time series table
        metadata: ViewMetadata
            Metadata to be added to the graph node

        Returns
        -------
        Tuple[GraphNode, List[ColumnInfo]]
        """
        view_graph_node, _ = self.construct_view_graph_node(
            graph_node_type=GraphNodeType.TIME_SERIES_VIEW,
            data_node=time_series_table_node,
            other_input_nodes=[],
            drop_column_names=drop_column_names,
            metadata=metadata,
        )
        columns_info = self.prepare_view_columns_info(drop_column_names=drop_column_names)
        return view_graph_node, columns_info


if TYPE_CHECKING:
    AllTableDataT = BaseTableData
    SpecificTableDataT = BaseTableData
else:
    AllTableDataT = Union[tuple(DATA_TABLES)]
    SpecificTableDataT = Annotated[Union[tuple(SPECIFIC_DATA_TABLES)], Field(discriminator="type")]


# construct function for specific data table deserialization
construct_specific_data_table = construct_serialize_function(
    all_types=SPECIFIC_DATA_TABLES,
    annotated_type=SpecificTableDataT,
    discriminator_key="type",
)


class SpecificTableData(BaseTableData):
    """
    Pseudo TableData class to support multiple table types.
    This class basically parses the dictionary into proper type based on its type parameter value.
    """

    def __new__(cls, **kwargs: Any) -> Any:
        return construct_specific_data_table(**kwargs)


class TableSpec(FeatureByteBaseModel):
    """
    Table specification with description
    """

    name: str
    description: Optional[str] = Field(default=None)


class TableDetails(FeatureByteBaseModel):
    """
    Table specification with additional details
    """

    details: Dict[str, Any] = Field(default_factory=dict)
    fully_qualified_name: str
    description: Optional[str] = Field(default=None)
