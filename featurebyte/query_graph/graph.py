"""
Implement graph data structure for query graph
"""
from __future__ import annotations

from typing import Any

import json
from collections import defaultdict

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.util import hash_node


class SingletonMeta(type):
    """
    Singleton Metaclass for Singleton construction
    """

    _instances: dict[Any, Any] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Node(BaseModel):
    """
    Graph Node
    """

    name: str
    type: NodeType
    parameters: dict[str, Any]
    output_type: NodeOutputType


class Graph(BaseModel):
    """
    Graph data structure
    """

    edges: dict[str, list[str]] = Field(default=defaultdict(list))
    backward_edges: dict[str, list[str]] = Field(default=defaultdict(list))
    nodes: dict[str, dict[str, Any]] = Field(default_factory=dict)
    node_type_counter: dict[str, int] = Field(default=defaultdict(int))

    def add_edge(self, parent: Node, child: Node) -> None:
        """
        Add edge to the graph by specifying a parent node & a child node

        Parameters
        ----------
        parent: Node
            parent node
        child: Node
            child node

        """
        self.edges[parent.name].append(child.name)
        self.backward_edges[child.name].append(parent.name)

    def _generate_node_name(self, node_type: NodeType) -> str:
        self.node_type_counter[node_type] += 1
        return f"{node_type}_{self.node_type_counter[node_type]}"

    def add_node(
        self, node_type: NodeType, node_params: dict[str, Any], node_output_type: NodeOutputType
    ) -> Node:
        """
        Add node to the graph by specifying node type, parameters & output type

        Parameters
        ----------
        node_type: NodeType
            node type
        node_params: dict[str, Any]
            parameters in dictionary format
        node_output_type: NodeOutputType
            node output type

        Returns
        -------
        node: Node

        """
        node = Node(
            name=self._generate_node_name(node_type),
            type=node_type,
            parameters=node_params,
            output_type=node_output_type,
        )
        self.nodes[node.name] = node.dict()
        return node

    def __repr__(self) -> str:
        return json.dumps(self.dict(), indent=4)


class QueryGraph(Graph):
    """
    Query graph object
    """

    node_name_to_ref: dict[str, int] = Field(default_factory=dict)
    ref_to_node_name: dict[int, str] = Field(default_factory=dict)

    def get_node_by_name(self, node_name: str) -> Node:
        """
        Retrieve the node given node name

        Parameters
        ----------
        node_name: str
            Node name

        Returns
        -------
        Node

        """
        return Node(**self.nodes[node_name])

    def add_operation(
        self,
        node_type: NodeType,
        node_params: dict[str, Any],
        node_output_type: NodeOutputType,
        input_nodes: list[Node],
    ) -> Node:
        """
        Add operation to the query graph.

        Parameters
        ----------
        node_type: NodeType
            node type
        node_params: dict
            parameters used for the node operation
        node_output_type: NodeOutputType
            node output type
        input_nodes: list[Node]
            list of input nodes

        Returns
        -------
        Node
            operation node of the given input

        """
        input_node_refs = [self.node_name_to_ref[node.name] for node in input_nodes]
        node_ref = hash_node(node_type, node_params, node_output_type, input_node_refs)
        if node_ref not in self.ref_to_node_name:
            node = self.add_node(node_type, node_params, node_output_type)
            for input_node in input_nodes:
                self.add_edge(input_node, node)

            self.ref_to_node_name[node_ref] = node.name
            self.node_name_to_ref[node.name] = node_ref
        else:
            name = self.ref_to_node_name[node_ref]
            node = Node(**self.nodes[name])
        return node


class GlobalQueryGraphState(metaclass=SingletonMeta):
    """
    Global singleton to store query graph related attributes
    """

    edges: dict[str, list[str]] = defaultdict(list)
    backward_edges: dict[str, list[str]] = defaultdict(list)
    nodes: dict[str, dict[str, Any]] = {}
    node_type_counter: dict[str, int] = defaultdict(int)
    node_name_to_ref: dict[str, int] = {}
    ref_to_node_name: dict[int, str] = {}

    @classmethod
    def clear(cls):
        cls.edges: dict[str, list[str]] = defaultdict(list)
        cls.backward_edges: dict[str, list[str]] = defaultdict(list)
        cls.nodes: dict[str, dict[str, Any]] = {}
        cls.node_type_counter: dict[str, int] = defaultdict(int)
        cls.node_name_to_ref: dict[str, int] = {}
        cls.ref_to_node_name: dict[int, str] = {}

    @classmethod
    def get_edges(cls):
        return cls.edges

    @classmethod
    def get_backward_edges(cls):
        return cls.backward_edges

    @classmethod
    def get_nodes(cls):
        return cls.nodes

    @classmethod
    def get_node_type_counter(cls):
        return cls.node_type_counter

    @classmethod
    def get_node_name_to_ref(cls):
        return cls.node_name_to_ref

    @classmethod
    def get_ref_to_node_name(cls):
        return cls.ref_to_node_name


class GlobalQueryGraph(QueryGraph):
    """
    Global query graph used to store the core like operations for the SQL query construction
    """

    edges: dict[str, list[str]] = Field(default_factory=GlobalQueryGraphState.get_edges)
    backward_edges: dict[str, list[str]] = Field(
        default_factory=GlobalQueryGraphState.get_backward_edges
    )
    nodes: dict[str, dict[str, Any]] = Field(default_factory=GlobalQueryGraphState.get_nodes)
    node_type_counter: dict[str, int] = Field(
        default_factory=GlobalQueryGraphState.get_node_type_counter
    )
    node_name_to_ref: dict[str, int] = Field(
        default_factory=GlobalQueryGraphState.get_node_name_to_ref
    )
    ref_to_node_name: dict[int, str] = Field(
        default_factory=GlobalQueryGraphState.get_ref_to_node_name
    )

    def _prune(
        self,
        target_node: Node,
        target_columns: list[str],
        pruned_graph: QueryGraph,
        processed_node_names: set[str],
        node_name_map: dict[str, str],
    ) -> QueryGraph:
        # pruning: move backward from target node to the input node
        to_prune_target_node = False
        input_node_names = self.backward_edges.get(target_node.name, [])
        if target_node.type == NodeType.ASSIGN:
            assign_column_name = target_node.parameters["name"]
            if assign_column_name in target_columns:
                # remove matched name from the target_columns
                target_columns = [col for col in target_columns if col != assign_column_name]
            else:
                # remove series path if exists
                to_prune_target_node = True
                input_node_names = input_node_names[:1]
        elif target_node.type == NodeType.PROJECT:
            # if columns are needed for projection, add them to the target_columns
            target_columns.extend(target_node.parameters["columns"])

        for input_node_name in input_node_names:
            input_node = self.get_node_by_name(input_node_name)
            pruned_graph = self._prune(
                target_node=input_node,
                target_columns=target_columns,
                pruned_graph=pruned_graph,
                processed_node_names=processed_node_names,
                node_name_map=node_name_map,
            )

        if to_prune_target_node:
            # do not add the target_node to the pruned graph
            return pruned_graph

        # reconstruction: process the node from the input node towards the target node
        mapped_input_node_names = []
        for input_node_name in input_node_names:
            # if the input node get pruned, it will not exist in the processed_node_names.
            # in this case, keep finding the first parent node exists in the processed_node_names.
            while input_node_name not in processed_node_names:
                input_node_name = self.backward_edges[input_node_name][0]
            mapped_input_node_names.append(input_node_name)

        # add the node back to the pruned graph
        node_pruned = pruned_graph.add_operation(
            node_type=NodeType(target_node.type),
            node_params=target_node.parameters,
            node_output_type=NodeOutputType(target_node.output_type),
            input_nodes=[
                pruned_graph.get_node_by_name(node_name_map[node_name])
                for node_name in mapped_input_node_names
            ],
        )

        # update the container to store the mapped node name & processed nodes information
        node_name_map[target_node.name] = node_pruned.name
        processed_node_names.add(target_node.name)
        return pruned_graph

    def prune(self, target_node: Node, target_columns: list[str]) -> tuple[QueryGraph, Node]:
        """
        Prune the query graph and return the pruned graph & mapped node.

        To prune the graph, this function first traverses from the target node to the input node.
        The unused branches of the graph will get pruned in this step. After that, a new graph is
        reconstructed by adding the required nodes back.

        Parameters
        ----------
        target_node: Node
            target end node
        target_columns: list[str]
            list of target columns

        Returns
        -------
        QueryGraph, Node
        """
        node_name_map: dict[str, str] = {}
        pruned_graph = self._prune(
            target_node=target_node,
            target_columns=target_columns,
            pruned_graph=QueryGraph(),
            processed_node_names=set(),
            node_name_map=node_name_map,
        )
        mapped_node = pruned_graph.get_node_by_name(node_name_map[target_node.name])
        return pruned_graph, mapped_node
