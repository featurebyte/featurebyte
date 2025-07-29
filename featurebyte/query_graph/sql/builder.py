"""
Module for SQL syntax tree builder
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Optional, Type

from bson import ObjectId

from featurebyte.common.path_util import import_submodules
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    BaseWindowAggregateParameters,
)
from featurebyte.query_graph.sql.ast.base import SQLNode, SQLNodeContext, TableNode
from featurebyte.query_graph.sql.ast.generic import (
    handle_filter_node,
    make_assign_node,
    make_project_node,
)
from featurebyte.query_graph.sql.common import (
    DevelopmentDatasets,
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


class SQLNodeKey:
    """Key for SQLNode, used to identify nodes in the SQL operation graph"""

    def __init__(self, node_name: str, primary_table_ids: Optional[list[ObjectId]]):
        self.node_name = node_name
        self.primary_table_ids = tuple(primary_table_ids) if primary_table_ids is not None else None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SQLNodeKey):
            return False
        return (
            self.node_name == other.node_name and self.primary_table_ids == other.primary_table_ids
        )

    def __hash__(self) -> int:
        return hash((self.node_name, self.primary_table_ids))


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
        development_datasets: Optional[DevelopmentDatasets] = None,
    ) -> None:
        self.sql_nodes: dict[SQLNodeKey, SQLNode | TableNode] = {}
        self.query_graph = query_graph
        self.sql_type = sql_type
        self.source_info = source_info
        self.to_filter_scd_by_current_flag = to_filter_scd_by_current_flag
        self.event_table_timestamp_filter = event_table_timestamp_filter
        self.aggregation_specs = aggregation_specs
        self.on_demand_entity_filters = on_demand_entity_filters
        self.partition_column_filters = partition_column_filters
        self.development_datasets = development_datasets
        aggregate_node_to_primary_table_ids: dict[str, list[ObjectId]] = {}
        for node in query_graph.nodes:
            if isinstance(node.parameters, BaseWindowAggregateParameters):
                aggregate_node = node
                primary_input_nodes = QueryGraph.get_primary_input_nodes_from_graph_model(
                    query_graph, aggregate_node.name
                )
                for input_node in primary_input_nodes:
                    parameters_dict = input_node.parameters.model_dump()
                    if "id" in parameters_dict:
                        table_id = ObjectId(parameters_dict["id"])
                        if aggregate_node.name not in aggregate_node_to_primary_table_ids:
                            aggregate_node_to_primary_table_ids[aggregate_node.name] = []
                        aggregate_node_to_primary_table_ids[aggregate_node.name].append(table_id)
        for node_id, table_ids in aggregate_node_to_primary_table_ids.items():
            aggregate_node_to_primary_table_ids[node_id] = sorted(table_ids)
        self.aggregate_node_to_primary_table_ids = aggregate_node_to_primary_table_ids

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
        if isinstance(target_node.parameters, BaseWindowAggregateParameters):
            primary_table_ids = self.aggregate_node_to_primary_table_ids.get(target_node.name)
        else:
            primary_table_ids = None
        sql_node = self._construct_sql_nodes(target_node, primary_table_ids)
        return sql_node

    def _construct_sql_nodes(
        self, cur_node: Node, primary_table_ids: Optional[list[ObjectId]]
    ) -> Any:
        """Recursively construct the nodes

        Parameters
        ----------
        cur_node : Node
            Current query graph node to construct SQLNode for
        primary_table_ids : Optional[list[ObjectId]]
            List of primary table ids for the current node. For post-aggregation nodes, this is set
            as None. For aggregation nodes and their inputs, this is set to the list of primary
            table ids. The resulting SQLNode is a function of both the current node and the
            primary_table_ids. This will influence partition column filters in the InputNode.

        Returns
        -------
        SQLNode

        Raises
        ------
        NotImplementedError
            If a query node is not yet supported
        """

        sql_node_key = SQLNodeKey(node_name=cur_node.name, primary_table_ids=primary_table_ids)
        assert sql_node_key not in self.sql_nodes

        # Recursively build input sql nodes first
        inputs = self.query_graph.backward_edges_map.get(cur_node.name, [])
        input_sql_nodes = []
        for input_node_id in inputs:
            input_node = self.query_graph.get_node_by_name(input_node_id)
            if primary_table_ids is None and isinstance(
                input_node.parameters, BaseWindowAggregateParameters
            ):
                # Initialize primary_table_ids for aggregate nodes
                next_primary_table_ids = self.aggregate_node_to_primary_table_ids.get(
                    input_node_id, None
                )
            else:
                next_primary_table_ids = primary_table_ids
            next_sql_node_key = SQLNodeKey(
                node_name=input_node_id, primary_table_ids=next_primary_table_ids
            )
            if next_sql_node_key not in self.sql_nodes:
                self._construct_sql_nodes(input_node, next_primary_table_ids)
            input_sql_node = self.sql_nodes[next_sql_node_key]
            input_sql_nodes.append(input_sql_node)

        # Now that input sql nodes are ready, build the current sql node
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
            development_datasets=self.development_datasets,
            primary_table_ids=primary_table_ids,
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

        self.sql_nodes[sql_node_key] = sql_node
        return sql_node
