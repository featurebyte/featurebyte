"""
Module for SQL syntax tree builder
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Optional, Type

from featurebyte.common.path_util import import_submodules
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.ast.base import SQLNode, SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.generic import (
    handle_filter_node,
    make_assign_node,
    make_project_node,
)
from featurebyte.query_graph.sql.common import (
    EventTableTimestampFilter,
    OnDemandEntityFilters,
    PartitionColumnFilters,
    SQLType,
)
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.specs import AggregationSpec


class NodeRegistry:
    """Registry of mapping between query graph Node and SQLNode"""

    def __init__(self) -> None:
        self.ast_classes_by_query: dict[NodeType, set[Type[SQLNode]]] = defaultdict(set)
        for _, cls in self.iterate_sql_nodes():
            self.register_sql_node(cls)

    def add(self, node_type: NodeType, ast_node_cls: Type[SQLNode]) -> None:
        """Add a query graph node to SQLNode mapping

        Parameters
        ----------
        node_type : NodeType
            Node Type
        ast_node_cls : Type[SQLNode]
            A subclass of SQLNode
        """
        self.ast_classes_by_query[node_type].add(ast_node_cls)

    def get_sql_node_classes(self, node_type: NodeType) -> set[Type[SQLNode]] | None:
        """Retrieve a set of candidate classes corresponding to the query node type

        Parameters
        ----------
        node_type : NodeType
            Query graph node type

        Returns
        -------
        set[Type[SQLNode]]
        """
        return self.ast_classes_by_query.get(node_type, set())

    def register_sql_node(self, sql_node_cls: Type[SQLNode]) -> None:
        """Register a SQLNode subclass

        Parameters
        ----------
        sql_node_cls : Type[SQLNode]
            A subclass of SQLNode
        """
        if sql_node_cls.query_node_type is None:
            # query_node_type is None when the class 1) is an abstract base class; or 2) if the
            # class deliberately chooses not to register the mapping in NodeRegistry (e.g.
            # ParsedExpressionNode, which can be instantiated in many ways, not just from a single
            # query node type)
            return
        if not isinstance(sql_node_cls.query_node_type, (list, tuple)):
            query_node_types = [sql_node_cls.query_node_type]
        else:
            query_node_types = sql_node_cls.query_node_type
        for node_type in query_node_types:
            self.add(node_type, sql_node_cls)

    @classmethod
    def iterate_sql_nodes(cls) -> Iterable[tuple[str, Type[SQLNode]]]:
        """Iterate subclasses of SQLNode in the ast subpackage

        Yields
        ------
        tuple[str, Type[SQLNode]]
            Tuples of class name and SQLNode subclass
        """
        for _, mod in import_submodules("featurebyte.query_graph.sql.ast").items():
            for name, sql_node_cls in mod.__dict__.items():
                if isinstance(sql_node_cls, type) and issubclass(sql_node_cls, SQLNode):
                    yield name, sql_node_cls


NODE_REGISTRY = NodeRegistry()


class SQLOperationGraph:
    """Construct a tree of SQL operations given a QueryGraph

    Parameters
    ----------
    query_graph : QueryGraphModel
        Query Graph representing user's intention
    sql_type : SQLType
        Type of SQL to generate
    """

    def __init__(
        self,
        query_graph: QueryGraphModel,
        sql_type: SQLType,
        source_info: SourceInfo,
        to_filter_scd_by_current_flag: bool = False,
        event_table_timestamp_filter: Optional[EventTableTimestampFilter] = None,
        aggregation_specs: Optional[dict[str, list[AggregationSpec]]] = None,
        on_demand_entity_filters: Optional[OnDemandEntityFilters] = None,
        partition_column_filters: Optional[PartitionColumnFilters] = None,
    ) -> None:
        self.sql_nodes: dict[str, SQLNode | TableNode] = {}
        self.query_graph = query_graph
        self.sql_type = sql_type
        self.source_info = source_info
        self.to_filter_scd_by_current_flag = to_filter_scd_by_current_flag
        self.event_table_timestamp_filter = event_table_timestamp_filter
        self.aggregation_specs = aggregation_specs
        self.on_demand_entity_filters = on_demand_entity_filters
        self.partition_column_filters = partition_column_filters

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
        sql_node = self._construct_sql_nodes(target_node)
        return sql_node

    def _construct_sql_nodes(self, cur_node: Node) -> Any:
        """Recursively construct the nodes

        Parameters
        ----------
        cur_node : Node
            Dictionary representation of Query Graph Node

        Returns
        -------
        SQLNode

        Raises
        ------
        NotImplementedError
            If a query node is not yet supported
        """

        cur_node_id = cur_node.name
        assert cur_node_id not in self.sql_nodes

        # Recursively build input sql nodes first
        inputs = self.query_graph.backward_edges_map.get(cur_node_id, [])
        input_sql_nodes = []
        for input_node_id in inputs:
            if input_node_id not in self.sql_nodes:
                input_node = self.query_graph.get_node_by_name(input_node_id)
                self._construct_sql_nodes(input_node)
            input_sql_node = self.sql_nodes[input_node_id]
            input_sql_nodes.append(input_sql_node)

        # Now that input sql nodes are ready, build the current sql node
        node_id = cur_node.name
        node_type = cur_node.type

        sql_node: Any = None
        sql_node_classes = NODE_REGISTRY.get_sql_node_classes(node_type)
        context = SQLNodeContext(
            graph=self.query_graph,
            query_node=cur_node,
            sql_type=self.sql_type,
            source_info=self.source_info,
            input_sql_nodes=input_sql_nodes,
            to_filter_scd_by_current_flag=self.to_filter_scd_by_current_flag,
            event_table_timestamp_filter=self.event_table_timestamp_filter,
            aggregation_specs=self.aggregation_specs,
            on_demand_entity_filters=self.on_demand_entity_filters,
            partition_column_filters=self.partition_column_filters,
        )

        # Construct an appropriate SQLNode based on the candidates defined in NodeRegistry
        if sql_node_classes:
            for sql_node_cls in sql_node_classes:
                sql_node = sql_node_cls.build(context)
                if sql_node is not None:
                    break

        # Nodes that can be mapped to different SQLNode. Instead of splitting the factory logic into
        # multiple SQLNode subclasses, helper functions like these are clearer.
        if sql_node is None:
            if node_type == NodeType.ASSIGN:
                sql_node = make_assign_node(context)

            elif node_type == NodeType.PROJECT:
                sql_node = make_project_node(context)

            elif node_type == NodeType.FILTER:
                sql_node = handle_filter_node(context)

            else:
                raise NotImplementedError(f"SQLNode not implemented for {cur_node}")

        self.sql_nodes[node_id] = sql_node
        return sql_node
