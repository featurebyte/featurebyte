"""
This model contains query graph internal model structures
"""
from typing import Any, DefaultDict, Dict, Iterator, List, Optional, Tuple

import json
from collections import defaultdict

from pydantic import Field, root_validator, validator

from featurebyte.exception import GraphInconsistencyError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.algorithm import dfs_traversal, topological_sort
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node import Node, construct_node
from featurebyte.query_graph.node.generic import InputNode
from featurebyte.query_graph.util import hash_node


class Edge(FeatureByteBaseModel):
    """Edge of a graph"""

    source: str
    target: str


class QueryGraphModel(FeatureByteBaseModel):
    """
    QueryGraphModel is the graph model which have only following operations:
    - graph creation (attribute validation)
    - graph node retrieval (node iteration, node retrieval)
    - graph update (node insertion, edge insertion)
    """

    edges: List[Edge] = Field(default_factory=list)
    nodes: List[Node] = Field(default_factory=list)

    # non-serialized attributes (will be derived during deserialization)
    # NEVER store a non-serialized attributes that CAN'T BE DERIVED from serialized attributes
    nodes_map: Dict[str, Node] = Field(default_factory=dict, exclude=True)
    edges_map: DefaultDict[str, List[str]] = Field(default=defaultdict(list), exclude=True)
    backward_edges_map: DefaultDict[str, List[str]] = Field(default=defaultdict(list), exclude=True)
    node_type_counter: DefaultDict[str, int] = Field(default=defaultdict(int), exclude=True)
    node_name_to_ref: Dict[str, str] = Field(default_factory=dict, exclude=True)
    ref_to_node_name: Dict[str, str] = Field(default_factory=dict, exclude=True)

    def __repr__(self) -> str:
        return json.dumps(self.json_dict(), indent=4)

    def __str__(self) -> str:
        return repr(self)

    @property
    def node_topological_order_map(self) -> Dict[str, int]:
        """
        Node name to topological sort order index mapping

        Returns
        -------
        Dict[int, str]
        """
        sorted_node_names = topological_sort(list(self.nodes_map), self.edges_map)
        return {value: idx for idx, value in enumerate(sorted_node_names)}

    @staticmethod
    def _derive_nodes_map(
        nodes: List[Node], nodes_map: Optional[Dict[str, Node]]
    ) -> Dict[str, Node]:
        if nodes_map is None:
            nodes_map = {}
        for node in nodes:
            nodes_map[node.name] = node
        return nodes_map

    @staticmethod
    def _derive_edges_map(
        edges: List[Edge], edges_map: Optional[Dict[str, List[str]]]
    ) -> Dict[str, List[str]]:
        if edges_map is None:
            edges_map = defaultdict(list)
        for edge in edges:
            edges_map[edge.source].append(edge.target)
        return edges_map

    @staticmethod
    def _derive_backward_edges_map(
        edges: List[Edge], backward_edges_map: Optional[Dict[str, List[str]]]
    ) -> Dict[str, List[str]]:
        if backward_edges_map is None:
            backward_edges_map = defaultdict(list)
        for edge in edges:
            backward_edges_map[edge.target].append(edge.source)
        return backward_edges_map

    @staticmethod
    def _derive_node_type_counter(
        nodes: List[Node], node_type_counter: Optional[Dict[str, int]]
    ) -> Dict[str, int]:
        if node_type_counter is None:
            node_type_counter = defaultdict(int)
        for node in nodes:
            node_type_counter[node.type] += 1
        return node_type_counter

    @staticmethod
    def _derive_node_name_to_ref(
        nodes_map: Dict[str, Node],
        edges_map: Dict[str, List[str]],
        backward_edges_map: Dict[str, List[str]],
        node_name_to_ref: Optional[Dict[str, str]],
    ) -> Dict[str, str]:
        if node_name_to_ref is None:
            node_name_to_ref = {}
        sorted_node_names = topological_sort(list(nodes_map), edges_map)
        for node_name in sorted_node_names:
            input_node_refs = [
                node_name_to_ref[input_node_name]
                for input_node_name in backward_edges_map.get(node_name, [])
            ]
            node = nodes_map[node_name]
            node_name_to_ref[node_name] = hash_node(
                node.type,
                node.parameters.dict(),
                node.output_type,
                input_node_refs,
            )
        return node_name_to_ref

    @staticmethod
    def _derive_ref_to_node_name(
        node_name_to_ref: Dict[str, str], ref_to_node_name: Optional[Dict[str, str]]
    ) -> Dict[str, str]:
        if ref_to_node_name is None:
            ref_to_node_name = {}
        for node_name, ref in node_name_to_ref.items():
            ref_to_node_name[ref] = node_name
        return ref_to_node_name

    @root_validator
    @classmethod
    def _set_internal_variables(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # NOTE: During graph instantiation, this method will get called (including global query graph).
        # Only create a new dictionary/object when the value is None. Otherwise, it will cause issue
        # for the global query graph.
        nodes_map = values.get("nodes_map")
        if not nodes_map and isinstance(values.get("nodes"), list):
            values["nodes_map"] = cls._derive_nodes_map(values["nodes"], nodes_map)

        edges_map = values.get("edges_map")
        if not edges_map and isinstance(values.get("edges"), list):
            values["edges_map"] = cls._derive_edges_map(values["edges"], edges_map)

        backward_edges_map = values.get("backward_edges_map")
        if not backward_edges_map and isinstance(values.get("edges"), list):
            values["backward_edges_map"] = cls._derive_backward_edges_map(
                values["edges"], backward_edges_map
            )

        node_type_counter = values.get("node_type_counter")
        if not node_type_counter and isinstance(values.get("nodes"), list):
            values["node_type_counter"] = cls._derive_node_type_counter(
                values["nodes"], node_type_counter
            )

        node_name_to_ref = values.get("node_name_to_ref")
        if not node_name_to_ref:
            # edges_map & backward_edges_map is a defaultdict, accessing a new key will have side effect
            # construct a new backward_edges_map dictionary to avoid introducing side effect
            values["node_name_to_ref"] = cls._derive_node_name_to_ref(
                nodes_map=values["nodes_map"],
                edges_map=dict(values["edges_map"]),
                backward_edges_map=dict(values["backward_edges_map"]),
                node_name_to_ref=node_name_to_ref,
            )

        ref_to_node_name = values.get("ref_to_node_name")
        if not ref_to_node_name:
            values["ref_to_node_name"] = cls._derive_ref_to_node_name(
                node_name_to_ref=values["node_name_to_ref"], ref_to_node_name=ref_to_node_name
            )

        return values

    @validator("edges_map", "backward_edges_map")
    @classmethod
    def _make_default_dict_list(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        # make sure the output is default dict to list
        updated_dict = defaultdict(list, value)
        return updated_dict

    @validator("node_type_counter")
    @classmethod
    def _make_default_dict_int(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        # make sure the output is default dict to int
        updated_dict = defaultdict(int, value)
        return updated_dict

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
        return self.nodes_map[node_name]

    def get_input_node(self, node_name: str) -> InputNode:
        """
        Retrieve input node for a specified target node

        Parameters
        ----------
        node_name: str
            Name of node to get input node for

        Raises
        ------
        GraphInconsistencyError
            Invalid graph structure

        Returns
        -------
        InputNode
            InputNode object
        """
        target_node = self.get_node_by_name(node_name)
        for input_node in self.iterate_nodes(target_node=target_node, node_type=NodeType.INPUT):
            assert isinstance(input_node, InputNode)
            return input_node
        raise GraphInconsistencyError("Input node not found")

    def get_input_node_names(self, node: Node) -> List[str]:
        """
        Get the input node names of the given node
        Parameters
        ----------
        node: Node
            Node
        Returns
        -------
        List[str]
        """
        return self.backward_edges_map.get(node.name, [])

    def iterate_nodes(self, target_node: Node, node_type: NodeType) -> Iterator[Node]:
        """
        Iterate all specified nodes in this query graph

        Parameters
        ----------
        target_node: Node
            Node from which to start the backward search
        node_type: NodeType
            Specific node type to iterate

        Yields
        ------
        Node
            Query graph nodes of the specified node type
        """
        for node in dfs_traversal(self, target_node):
            if node.type == node_type:
                yield node

    def iterate_sorted_nodes(self) -> Iterator[Node]:
        """
        Iterate all nodes in topological sorted order

        Yields
        ------
        Node
            Topologically sorted query graph nodes
        """
        sorted_node_names = topological_sort(list(self.nodes_map), self.edges_map)
        for node_name in sorted_node_names:
            yield self.nodes_map[node_name]

    def _add_edge(self, parent: Node, child: Node) -> None:
        """
        Add edge to the graph by specifying a parent node & a child node

        Parameters
        ----------
        parent: Node
            parent node
        child: Node
            child node

        """
        self.edges.append(Edge(source=parent.name, target=child.name))
        self.edges_map[parent.name].append(child.name)
        self.backward_edges_map[child.name].append(parent.name)

    def _generate_node_name(self, node_type: NodeType) -> str:
        self.node_type_counter[node_type] += 1
        return f"{node_type}_{self.node_type_counter[node_type]}"

    def _add_node(
        self, node_type: NodeType, node_params: Dict[str, Any], node_output_type: NodeOutputType
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
        node_dict = {
            "name": self._generate_node_name(node_type),
            "type": node_type,
            "parameters": node_params,
            "output_type": node_output_type,
        }
        node = construct_node(**node_dict)
        self.nodes.append(node)
        self.nodes_map[node.name] = node
        return node

    def add_operation(
        self,
        node_type: NodeType,
        node_params: Dict[str, Any],
        node_output_type: NodeOutputType,
        input_nodes: List[Node],
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
        # create a temp_node to validate the node parameters & use only the required parameters to hash
        temp_node = construct_node(
            name=str(node_type),
            type=node_type,
            parameters=node_params,
            output_type=node_output_type,
        )
        node_ref = hash_node(
            temp_node.type,
            temp_node.parameters.dict(),
            temp_node.output_type,
            input_node_refs,
        )
        if node_ref not in self.ref_to_node_name:
            node = self._add_node(node_type, node_params, node_output_type)
            for input_node in input_nodes:
                self._add_edge(input_node, node)

            self.ref_to_node_name[node_ref] = node.name
            self.node_name_to_ref[node.name] = node_ref
        else:
            node = self.nodes_map[self.ref_to_node_name[node_ref]]
        return node


NodeNameMap = Dict[str, str]
GraphNodeNameMap = Tuple[QueryGraphModel, NodeNameMap]
