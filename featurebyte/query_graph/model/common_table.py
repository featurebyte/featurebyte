"""
This module contains common table related models.
"""
from typing import Any, Dict, Iterable, List, Literal, Optional, cast

from abc import abstractmethod

from pydantic import validator

from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo, validate_columns_info
from featurebyte.query_graph.model.critical_data_info import CleaningOperation
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.generic import InputNode, ProjectNode
from featurebyte.query_graph.node.schema import ColumnSpec, FeatureStoreDetails, TableDetails


class TabularSource(FeatureByteBaseModel):
    """Model for tabular source"""

    feature_store_id: PydanticObjectId
    table_details: TableDetails


SPECIFIC_DATA_TABLES = []
DATA_TABLES = []


class BaseTableData(FeatureByteBaseModel):
    """Base data model used to capture input node info"""

    type: Literal[
        TableDataType.GENERIC,
        TableDataType.EVENT_DATA,
        TableDataType.ITEM_DATA,
        TableDataType.DIMENSION_DATA,
        TableDataType.SCD_DATA,
    ]
    columns_info: List[ColumnInfo]
    tabular_source: TabularSource

    # pydantic validators
    _validator = validator("columns_info", allow_reuse=True)(validate_columns_info)

    def __init_subclass__(cls, **kwargs: Any):
        # add table into DATA_TABLES & SPECIFIC_DATA_TABLES (if not generic type)
        table_type = cls.__fields__["type"]
        if repr(table_type.type_).startswith("typing.Literal"):
            DATA_TABLES.append(cls)
        if table_type.default != TableDataType.GENERIC:
            SPECIFIC_DATA_TABLES.append(cls)

    def _get_common_input_node_parameters(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "columns": [ColumnSpec(**col.dict()) for col in self.columns_info],
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

    def construct_cleaning_recipe_node(self, input_node: InputNode) -> Optional[GraphNode]:
        """
        Construct cleaning recipe graph node

        Parameters
        ----------
        input_node: InputNode
            Input node for the cleaning recipe

        Returns
        -------
        Optional[GraphNode]
        """
        graph_node: Optional[GraphNode] = None
        proxy_input_nodes: List[BaseNode] = []
        frame_node: Node
        for col_info in self._iterate_column_info_with_cleaning_operations():
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

    @abstractmethod
    def construct_input_node(self, feature_store_details: FeatureStoreDetails) -> InputNode:
        """
        Construct input node based on data info

        Parameters
        ----------
        feature_store_details: FeatureStoreDetails
            Feature store details

        Returns
        -------
        InputNode
        """
