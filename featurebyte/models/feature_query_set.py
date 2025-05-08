"""
FeatureQuerySet related classes
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from sqlglot.expressions import Select

from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.batch_helper import (
    construct_join_feature_sets_query,
    get_feature_names,
    split_nodes,
)
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.feature_compute import FeatureQuery
from featurebyte.query_graph.sql.source_info import SourceInfo


class FeatureQueryGenerator(ABC):
    """
    FeatureQueryGenerator is used by execute_feature_query_set to generate FeatureQuery objects for
    a given query graph and nodes.
    """

    @abstractmethod
    def get_query_graph(self) -> QueryGraph:
        """
        Get the underlying query graph
        """

    @abstractmethod
    def get_nodes(self) -> list[Node]:
        """
        Get the list of nodes for the features

        Returns
        -------
        list[Node]
        """

    @abstractmethod
    def generate_feature_query(self, node_names: list[str], table_name: str) -> FeatureQuery:
        """
        Generate a FeatureQuery object for the given node names

        Parameters
        ----------
        node_names: list[str]
            List of node names
        table_name: str
            Table name to be used for the materialized table in the FeatureQuery

        Returns
        -------
        FeatureQuery
        """

    def get_node_names(self) -> list[str]:
        """
        Get a list of node names

        Returns
        -------
        list[str]
        """
        return [node.name for node in self.get_nodes()]

    def get_feature_names(self, node_names: list[str]) -> list[str]:
        """
        Get a list of feature names for the given node names

        Parameters
        ----------
        node_names: list[str]
            List of node names

        Returns
        -------
        list[str]
        """
        graph = self.get_query_graph()
        nodes = [graph.get_node_by_name(node_name) for node_name in node_names]
        return get_feature_names(graph, nodes)

    def split_nodes(
        self, node_names: list[str], num_features_per_query: int, source_info: SourceInfo
    ) -> list[list[Node]]:
        """
        Split nodes into multiple lists, each containing at most `num_features_per_query` nodes.
        Nodes within the same group after splitting will be executed in the same query.

        Parameters
        ----------
        node_names : list[str]
            List of node names
        num_features_per_query : int
            Number of features per query
        source_info: SourceInfo
            Source information

        Returns
        -------
        list[list[Node]]
        """
        graph = self.get_query_graph()
        nodes = [graph.get_node_by_name(node_name) for node_name in node_names]
        return split_nodes(graph, nodes, num_features_per_query, source_info)


class FeatureQuerySet:
    """
    FeatureQuerySet contains the information required to materialize features in a batched
    manner using execute_feature_query_set
    """

    def __init__(
        self,
        feature_query_generator: FeatureQueryGenerator,
        request_table_name: str,
        request_table_columns: list[str],
        output_table_details: Optional[TableDetails],
        output_feature_names: list[str],
        output_include_row_index: bool,
        progress_message: str,
    ):
        self.feature_query_generator = feature_query_generator
        self.request_table_name = request_table_name
        self.request_table_columns = request_table_columns
        self.output_table_details = output_table_details
        self.output_feature_names = output_feature_names
        self.output_include_row_index = output_include_row_index
        self.progress_message = progress_message
        self.completed_feature_queries: list[FeatureQuery] = []

    def add_completed_feature_query(self, feature_query: FeatureQuery) -> None:
        """
        Add a completed FeatureQuery to the LazyFeatureQuerySet

        Parameters
        ----------
        feature_query: FeatureQuery
            FeatureQuery object
        """
        self.completed_feature_queries.append(feature_query)

    def construct_join_feature_sets_query(self) -> Select:
        """
        Construct the output query that joins the completed feature queries

        Returns
        -------
        Select
        """
        return construct_join_feature_sets_query(
            feature_queries=self.completed_feature_queries,
            output_feature_names=self.output_feature_names,
            request_table_name=self.request_table_name,
            request_table_columns=self.request_table_columns,
            output_include_row_index=self.output_include_row_index,
        )

    def construct_output_query(self, source_info: SourceInfo) -> str:
        """
        Construct the output query that joins the completed feature queries

        Parameters
        ----------
        source_info: SourceInfo
            Source information

        Returns
        -------
        str
        """
        output_expr = self.construct_join_feature_sets_query()
        if self.output_table_details is not None:
            output_expr = get_sql_adapter(source_info).create_table_as(  # type: ignore[assignment]
                table_details=self.output_table_details,
                select_expr=output_expr,
            )
        return sql_to_string(output_expr, source_type=source_info.source_type)

    def get_num_nodes(self) -> int:
        """
        Get the number of nodes in the FeatureQueryGenerator

        Returns
        -------
        int
        """
        return len(self.feature_query_generator.get_node_names())

    def get_completed_node_names(self) -> list[str]:
        """
        Get the list of completed node names

        Returns
        -------
        list[str]
        """
        node_names = []
        for query in self.completed_feature_queries:
            node_names.extend(query.node_names)
        return node_names

    def get_pending_node_names(self) -> list[str]:
        """
        Get the list of pending node names

        Returns
        -------
        list[str]
        """
        completed_node_names = set(self.get_completed_node_names())
        pending_node_names = []
        for node_name in self.feature_query_generator.get_node_names():
            if node_name not in completed_node_names:
                pending_node_names.append(node_name)
        return pending_node_names

    @property
    def output_table_name(self) -> Optional[str]:
        """
        Get the output table name

        Returns
        -------
        str
        """
        if self.output_table_details is not None:
            return self.output_table_details.table_name
        return None
