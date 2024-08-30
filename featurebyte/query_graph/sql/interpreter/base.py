"""
Base class for SQL interpreter.
"""

from __future__ import annotations

from typing import Tuple, cast

from sqlglot import expressions

from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.base import ExpressionNode, TableNode
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType, construct_cte_sql, sql_to_string
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer


class BaseGraphInterpreter:
    """Interprets a given Query Graph and generates SQL for different purposes

    Parameters
    ----------
    query_graph : QueryGraphModel
        Query graph
    source_info: SourceInfo
        Data source type information
    """

    def __init__(self, query_graph: QueryGraphModel, source_info: SourceInfo):
        self.query_graph, self.node_name_map = GraphFlatteningTransformer(
            graph=query_graph
        ).transform()
        self.source_info = source_info
        self.adapter = get_sql_adapter(source_info)

    def get_flattened_node(self, node_name: str) -> Node:
        """Get node in the flattened graph

        Parameters
        ----------
        node_name : str
            Query graph node name (before flattening)

        Returns
        -------
        Node
        """
        return self.query_graph.get_node_by_name(self.node_name_map[node_name])

    def construct_shape_sql(self, node_name: str) -> Tuple[str, int]:
        """Construct SQL to get row count from a given node

        Parameters
        ----------
        node_name : str
            Query graph node name

        Returns
        -------
        Tuple[str, int]
            SQL code to execute, and column count
        """
        flat_node = self.get_flattened_node(node_name)
        operation_structure = QueryGraph(
            **self.query_graph.model_dump()
        ).extract_operation_structure(flat_node, keep_all_source_columns=True)
        sql_node = SQLOperationGraph(
            self.query_graph, sql_type=SQLType.MATERIALIZE, source_info=self.source_info
        ).build(flat_node)
        assert isinstance(sql_node, (TableNode, ExpressionNode))
        if isinstance(sql_node, TableNode):
            sql_tree = sql_node.sql
        else:
            sql_tree = sql_node.sql_standalone

        sql_tree = (
            construct_cte_sql([("data", sql_tree)])
            .select(expressions.alias_(expressions.Count(this="*"), "count", quoted=True))
            .from_("data")
        )
        return (
            sql_to_string(sql_tree, source_type=self.source_info.source_type),
            len(operation_structure.columns),
        )

    def construct_materialize_expr(self, node_name: str) -> expressions.Select:
        """
        Construct SQL to materialize a given node representing a View object

        Parameters
        ----------
        node_name : str
            Query graph node name

        Returns
        -------
        Select
        """
        flat_node = self.get_flattened_node(node_name)
        sql_graph = SQLOperationGraph(
            self.query_graph, sql_type=SQLType.MATERIALIZE, source_info=self.source_info
        )
        sql_node = sql_graph.build(flat_node)
        assert isinstance(sql_node, TableNode)
        return cast(expressions.Select, sql_node.sql)
