"""
Base class for SQL interpreter.
"""
from __future__ import annotations

from typing import Tuple, cast

from sqlglot import expressions

from featurebyte.enum import SourceType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.base import TableNode
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType, construct_cte_sql, sql_to_string
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer


class BaseGraphInterpreter:
    """Interprets a given Query Graph and generates SQL for different purposes

    Parameters
    ----------
    query_graph : QueryGraphModel
        Query graph
    source_type : SourceType
        Data source type information
    """

    def __init__(self, query_graph: QueryGraphModel, source_type: SourceType):
        self.query_graph = query_graph
        self.source_type = source_type
        self.adapter = get_sql_adapter(source_type)

    def flatten_graph(self, node_name: str) -> Tuple[QueryGraphModel, Node]:
        """
        Flatten the query graph (replace those graph node with flattened nodes)

        Parameters
        ----------
        node_name: str
            Target node name

        Returns
        -------
        Tuple[QueryGraphModel, str]
        """
        graph, node_name_map = GraphFlatteningTransformer(graph=self.query_graph).transform()
        node = graph.get_node_by_name(node_name_map[node_name])
        return graph, node

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
        flat_graph, flat_node = self.flatten_graph(node_name=node_name)
        operation_structure = QueryGraph(**flat_graph.dict()).extract_operation_structure(
            flat_node, keep_all_source_columns=True
        )
        sql_tree = (
            SQLOperationGraph(
                flat_graph, sql_type=SQLType.MATERIALIZE, source_type=self.source_type
            )
            .build(flat_node)
            .sql
        )
        sql_tree = (
            construct_cte_sql([("data", sql_tree)])
            .select(expressions.alias_(expressions.Count(this="*"), "count", quoted=True))
            .from_("data")
        )
        return (
            sql_to_string(sql_tree, source_type=self.source_type),
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
        flat_graph, flat_node = self.flatten_graph(node_name=node_name)
        sql_graph = SQLOperationGraph(
            flat_graph, sql_type=SQLType.MATERIALIZE, source_type=self.source_type
        )
        sql_node = sql_graph.build(flat_node)
        assert isinstance(sql_node, TableNode)
        return cast(expressions.Select, sql_node.sql)
