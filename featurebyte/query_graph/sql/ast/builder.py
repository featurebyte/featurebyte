"""
Module for SQL syntax tree builder
"""
from __future__ import annotations

from typing import Any

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupbyNode
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNode, TableNode
from featurebyte.query_graph.sql.ast.binary import (
    BINARY_OPERATION_NODE_TYPES,
    make_binary_operation_node,
)
from featurebyte.query_graph.sql.ast.generic import (
    AliasNode,
    handle_filter_node,
    make_assign_node,
    make_conditional_node,
    make_project_node,
)
from featurebyte.query_graph.sql.ast.input import make_input_node
from featurebyte.query_graph.sql.ast.tile import handle_groupby_node
from featurebyte.query_graph.sql.ast.unary import (
    SUPPORTED_EXPRESSION_NODE_TYPES,
    make_expression_node,
)
from featurebyte.query_graph.sql.common import SQLType


class SQLOperationGraph:
    """Construct a tree of SQL operations given a QueryGraph

    Parameters
    ----------
    query_graph : QueryGraph
        Query Graph representing user's intention
    sql_type : SQLType
        Type of SQL to generate
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, query_graph: QueryGraph, sql_type: SQLType) -> None:
        self.sql_nodes: dict[str, SQLNode | TableNode] = {}
        self.query_graph = query_graph
        self.sql_type = sql_type

    def build(self, target_node: Node) -> Any:
        """Build the graph from a given query Node, working backwards

        Parameters
        ----------
        target_node : Node
            Dict representation of Query Graph Node. Build graph from this node backwards. This is
            typically the last node in the Query Graph, but can also be an intermediate node.

        Returns
        -------
        SQLNode
        """
        if isinstance(target_node, GroupbyNode):
            groupby_keys = target_node.parameters.keys
        else:
            groupby_keys = None
        sql_node = self._construct_sql_nodes(target_node, groupby_keys=groupby_keys)  # type: ignore
        return sql_node

    def _construct_sql_nodes(self, cur_node: Node, groupby_keys: list[str] | None) -> Any:
        """Recursively construct the nodes

        Parameters
        ----------
        cur_node : Node
            Dictionary representation of Query Graph Node
        groupby_keys : list[str] | None
            Groupby keys in the current context. Used when sql_type is BUILD_TILE_ON_DEMAND

        Returns
        -------
        SQLNode

        Raises
        ------
        NotImplementedError
            If a query node is not yet supported
        """
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-branches
        cur_node_id = cur_node.name
        assert cur_node_id not in self.sql_nodes

        # Recursively build input sql nodes first
        inputs = self.query_graph.backward_edges_map[cur_node_id]
        input_sql_nodes = []
        for input_node_id in inputs:
            if input_node_id not in self.sql_nodes:
                input_node = self.query_graph.get_node_by_name(input_node_id)
                # Note: In the lineage leading to any features or intermediate outputs, there can be
                # only one groupby operation (there can be parallel groupby operations, but not
                # consecutive ones)
                if groupby_keys is None and isinstance(input_node, GroupbyNode):
                    groupby_keys = input_node.parameters.keys  # type: ignore
                self._construct_sql_nodes(input_node, groupby_keys=groupby_keys)
            input_sql_node = self.sql_nodes[input_node_id]
            input_sql_nodes.append(input_sql_node)

        # Now that input sql nodes are ready, build the current sql node
        node_id = cur_node.name
        node_type = cur_node.type
        parameters = cur_node.parameters.dict()
        output_type = cur_node.output_type

        sql_node: Any
        if node_type == NodeType.INPUT:
            sql_node = make_input_node(parameters, self.sql_type, groupby_keys)

        elif node_type == NodeType.ASSIGN:
            sql_node = make_assign_node(input_sql_nodes, parameters)

        elif node_type == NodeType.PROJECT:
            sql_node = make_project_node(input_sql_nodes, parameters, output_type)

        elif node_type == NodeType.ALIAS:
            expr_node = input_sql_nodes[0]
            assert isinstance(expr_node, ExpressionNode)
            sql_node = AliasNode(
                table_node=expr_node.table_node, name=parameters["name"], expr_node=expr_node
            )

        elif node_type in BINARY_OPERATION_NODE_TYPES:
            sql_node = make_binary_operation_node(node_type, input_sql_nodes, parameters)

        elif node_type in SUPPORTED_EXPRESSION_NODE_TYPES:
            sql_node = make_expression_node(input_sql_nodes, node_type, parameters)

        elif node_type == NodeType.FILTER:
            sql_node = handle_filter_node(input_sql_nodes, output_type)

        elif node_type == NodeType.CONDITIONAL:
            sql_node = make_conditional_node(input_sql_nodes, cur_node)

        elif node_type == NodeType.GROUPBY:
            sql_node = handle_groupby_node(
                groupby_node=cur_node,
                parameters=parameters,
                input_sql_nodes=input_sql_nodes,
                sql_type=self.sql_type,
            )
        else:
            raise NotImplementedError(f"SQLNode not implemented for {cur_node}")

        self.sql_nodes[node_id] = sql_node
        return sql_node
