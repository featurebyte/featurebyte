"""
This module contains common table related models.
"""

from abc import abstractmethod
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar, cast

from pydantic import Field, field_validator
from typing_extensions import Literal

from featurebyte.common.validator import columns_info_validator
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.cleaning_operation import (
    CleaningOperation,
    ColumnCleaningOperation,
)
from featurebyte.query_graph.node.generic import ProjectNode
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata
from featurebyte.query_graph.node.schema import ColumnSpec, FeatureStoreDetails, TableDetails


class TabularSource(FeatureByteBaseModel):
    """Model for tabular source"""

    feature_store_id: PydanticObjectId
    table_details: TableDetails


SPECIFIC_DATA_TABLES = []
DATA_TABLES = []
TableDataT = TypeVar("TableDataT", bound="BaseTableData")


class BaseTableData(FeatureByteBaseModel):
    """Base table model used to capture input node info"""

    type: Literal[
        TableDataType.SOURCE_TABLE,
        TableDataType.EVENT_TABLE,
        TableDataType.ITEM_TABLE,
        TableDataType.DIMENSION_TABLE,
        TableDataType.SCD_TABLE,
        TableDataType.TIME_SERIES_TABLE,
    ] = Field(
        description="Table type. Either source_table, event_table, item_table, dimension_table or scd_table"
    )

    columns_info: List[ColumnInfo]
    tabular_source: TabularSource
    managed_view_id: Optional[PydanticObjectId] = Field(default=None)

    # pydantic validators
    _validator = field_validator("columns_info")(columns_info_validator)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        # add table into DATA_TABLES & SPECIFIC_DATA_TABLES (if not generic type)
        table_type = cls.model_fields["type"]
        if "Literal" in repr(table_type.annotation):
            DATA_TABLES.append(cls)
        if table_type.default != TableDataType.SOURCE_TABLE:
            SPECIFIC_DATA_TABLES.append(cls)

    @property
    def column_cleaning_operations(self) -> List[ColumnCleaningOperation]:
        """
        Get column cleaning operations from column info's critical data info

        Returns
        -------
        List[ColumnCleaningOperation]
        """
        return [
            ColumnCleaningOperation(
                column_name=col.name, cleaning_operations=col.critical_data_info.cleaning_operations
            )
            for col in self.columns_info
            if col.critical_data_info is not None and col.critical_data_info.cleaning_operations
        ]

    def clone(
        self: TableDataT, column_cleaning_operations: List[ColumnCleaningOperation]
    ) -> TableDataT:
        """
        Create a new table with the specified column cleaning operations

        Parameters
        ----------
        column_cleaning_operations: List[ColumnCleaningOperation]
            Column cleaning operations

        Returns
        -------
        TableDataT
        """
        columns_info = []
        col_to_cleaning_ops = {
            col_clean_op.column_name: col_clean_op.cleaning_operations
            for col_clean_op in column_cleaning_operations
        }
        for col in self.columns_info:
            if col.name in col_to_cleaning_ops:
                col.critical_data_info = CriticalDataInfo(
                    cleaning_operations=col_to_cleaning_ops[col.name]
                )
            else:
                col.critical_data_info = None
            columns_info.append(col)
        return type(self)(**{**self.model_dump(by_alias=True), "columns_info": columns_info})

    def _get_common_input_node_parameters(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "columns": [ColumnSpec(**col.model_dump()) for col in self.columns_info],
            "table_details": self.tabular_source.table_details,
        }

    def _iterate_column_info_with_cleaning_operations(self) -> Iterable[ColumnInfo]:
        """
        Iterate column info with non-empty cleaning operations

        Yields
        ------
        ColumnInfo
            Column info with non-empty cleaning operations
        """
        for col_info in self.columns_info:
            critical_data_info = col_info.critical_data_info
            if critical_data_info is not None and critical_data_info.cleaning_operations:
                yield col_info

    @staticmethod
    def _add_cleaning_operations(
        cleaning_operations: List[CleaningOperation],
        graph_node: GraphNode,
        frame_node: Node,
        project_node: ProjectNode,
        output_column_name: str,
        dtype: DBVarType,
    ) -> Node:
        """
        Add cleaning operations to the graph node

        Parameters
        ----------
        cleaning_operations: List[CleaningOperation]
            List of cleaning operations
        graph_node: GraphNode
            Graph node
        frame_node: Node
            Input frame node
        project_node: ProjectNode
            Column projection node
        output_column_name: str
            Output column name
        dtype: DBVarType
            Data type that output column will be casted to

        Returns
        -------
        Node
        """
        input_node: Node = project_node
        for cleaning_operation in cleaning_operations:
            input_node = cleaning_operation.add_cleaning_operation(
                graph_node=graph_node, input_node=input_node, dtype=dtype
            )

        return graph_node.add_operation(
            node_type=NodeType.ASSIGN,
            node_params={"name": output_column_name, "value": None},
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[frame_node, input_node],
        )

    def construct_cleaning_recipe_node(
        self, input_node: InputNode, skip_column_names: List[str]
    ) -> Optional[GraphNode]:
        """
        Construct cleaning recipe graph node

        Parameters
        ----------
        input_node: InputNode
            Input node for the cleaning recipe
        skip_column_names: List[str]
            List of column names to skip

        Returns
        -------
        Optional[GraphNode]
        """
        graph_node: Optional[GraphNode] = None
        proxy_input_nodes: List[BaseNode] = []
        frame_node: Node
        for col_info in self._iterate_column_info_with_cleaning_operations():
            if col_info.name in skip_column_names:
                continue

            if graph_node is None:
                graph_node, proxy_input_nodes = GraphNode.create(
                    node_type=NodeType.PROJECT,
                    node_params={"columns": [col_info.name]},
                    node_output_type=NodeOutputType.SERIES,
                    input_nodes=[input_node],
                    graph_node_type=GraphNodeType.CLEANING,
                )
                frame_node = proxy_input_nodes[0]

            proj_col_node = graph_node.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [col_info.name]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=proxy_input_nodes,
            )

            assert col_info.critical_data_info is not None
            frame_node = self._add_cleaning_operations(
                cleaning_operations=col_info.critical_data_info.cleaning_operations,
                graph_node=graph_node,
                project_node=cast(ProjectNode, proj_col_node),
                frame_node=frame_node,
                output_column_name=col_info.name,
                dtype=col_info.dtype,
            )

        return graph_node

    def prepare_view_columns_info(self, drop_column_names: List[str]) -> List[ColumnInfo]:
        """
        Prepare view columns info

        Parameters
        ----------
        drop_column_names: List[str]
            List of column names to drop

        Returns
        -------
        List[ColumnInfo]
        """
        columns_info: List[ColumnInfo] = []
        unwanted_column_names = set(drop_column_names)
        for col_info in self.columns_info:
            if col_info.name not in unwanted_column_names:
                columns_info.append(col_info)
        return columns_info

    def construct_view_graph_node(
        self,
        graph_node_type: GraphNodeType,
        data_node: InputNode,
        other_input_nodes: List[Node],
        drop_column_names: List[str],
        metadata: ViewMetadata,
    ) -> Tuple[GraphNode, List[Node]]:
        """
        Construct view graph node from table. The output of any view should be a single graph node
        that is based on a single table node and optionally other input node(s). By using graph node, we can add
        some metadata to the node to help with the SDK code reconstruction from the graph. Note that introducing
        a graph node does not change the tile & aggregation hash ID if the final flatten graph is the same. Metadata
        added to the graph node also won't affect the tile & aggregation hash ID.

        Parameters
        ----------
        graph_node_type: GraphNodeType
            Graph node type
        data_node: InputNode
            Input node for the view
        other_input_nodes: List[Node]
            Other input nodes for the view (used to construct additional proxy input nodes in the nested graph)
        drop_column_names: List[str]
            List of column names to drop
        metadata: ViewMetadata
            Metadata for the view graph node

        Returns
        -------
        Tuple[GraphNode, List[Node]]
        """
        # prepare graph node's inputs
        view_graph_input_nodes: List[Node] = [data_node]
        if other_input_nodes:
            view_graph_input_nodes.extend(other_input_nodes)

        # prepare project columns
        columns_info = self.prepare_view_columns_info(drop_column_names=drop_column_names)
        project_columns = [col.name for col in columns_info]

        # project node assume single input only and view_graph_input_nodes could have more than 1 item.
        # therefore, nested_node_input_indices is used to specify the input node index for the project node
        # without using all the proxy input nodes.
        view_graph_node, proxy_input_nodes = GraphNode.create(
            node_type=NodeType.PROJECT,
            node_params={"columns": project_columns},
            node_output_type=NodeOutputType.FRAME,
            input_nodes=view_graph_input_nodes,
            graph_node_type=graph_node_type,
            nested_node_input_indices=[0],
            metadata=metadata,
        )

        # prepare view graph node
        cleaning_graph_node = self.construct_cleaning_recipe_node(
            input_node=data_node, skip_column_names=drop_column_names
        )
        if cleaning_graph_node:
            # cleaning graph node only requires single input
            view_graph_node.add_operation(
                node_type=NodeType.GRAPH,
                node_params=cleaning_graph_node.parameters.model_dump(by_alias=True),
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[view_graph_node.output_node],
            )

        # prepare nested input nodes
        # first input node is the cleaning graph node output node (apply cleaning recipe)
        # other input nodes are the proxy input nodes (used to construct additional proxy input nodes
        # in the nested graph)
        nested_input_nodes: List[Node] = [view_graph_node.output_node, *proxy_input_nodes[1:]]
        assert len(proxy_input_nodes) == len(nested_input_nodes)
        return view_graph_node, nested_input_nodes

    @property
    @abstractmethod
    def primary_key_columns(self) -> List[str]:
        """
        List of primary key columns of the table

        Returns
        -------
        List[str]
        """

    @abstractmethod
    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        """
        Construct input node based on table info

        Parameters
        ----------
        feature_store_details: FeatureStoreDetails
            Feature store details

        Returns
        -------
        InputNode
        """
