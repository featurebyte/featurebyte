"""
Implement graph data structure for execution graph
"""
from typing import List

import json
from collections import defaultdict
from dataclasses import dataclass

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.util import hash_node


class SingletonMeta(type):
    """
    Singleton Metaclass for Singleton construction
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

    def clear(cls):
        """
        Remove the singleton instance for the recreation of the new singleton
        """
        try:
            del SingletonMeta._instances[cls]
        except KeyError:
            pass


@dataclass()
class Node:
    """
    Graph Node
    """

    name: str
    type: str
    parameters: dict
    output_type: str


class Graph(metaclass=SingletonMeta):
    """
    Graph data structure
    """

    def __init__(self):
        self.edges = defaultdict(list)
        self.backward_edges = defaultdict(list)
        self.nodes = {}
        self._node_type_counter = defaultdict(int)

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
        self._node_type_counter[node_type] += 1
        return f"{node_type}_{self._node_type_counter[node_type]}"

    def add_node(
        self, node_type: NodeType, node_params: dict, node_output_type: NodeOutputType
    ) -> Node:
        """
        Add node to the graph by specifying node type, parameters & output type

        Parameters
        ----------
        node_type: NodeType
            node type
        node_params: dict
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
        self.nodes[node.name] = {
            "type": node_type.value,
            "parameters": node_params,
            "output_type": node_output_type.value,
        }
        return node

    def to_dict(self):
        """
        Convert the graph into dictionary format

        Returns
        -------
        output: dict

        """
        return {"nodes": self.nodes, "edges": dict(self.edges)}

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=4)


class QueryGraph(Graph):
    """
    Graph used to store the pandas like operations for the SQL query construction
    """

    def __init__(self):
        super().__init__()
        self.node_name_to_ref = {}
        self.ref_to_node_name = {}

    def add_operation(
        self,
        node_type: NodeType,
        node_params: dict,
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
        input_nodes: List[Node]
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
            node = Node(name=name, **self.nodes[name])
        return node
