"""
Implement graph data structure for execution graph
"""
from typing import Dict, List

import json
from collections import defaultdict
from dataclasses import dataclass

from featurebyte.execution_graph.util import hash_node


class SingletonMeta(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

    def clear(cls):
        try:
            del SingletonMeta._instances[cls]
        except KeyError:
            pass


@dataclass()
class Node:
    id: str
    type: str
    parameters: Dict
    output_type: str


class Graph(metaclass=SingletonMeta):
    def __init__(self):
        self.edges = defaultdict(list)
        self.nodes = {}
        self._node_type_counter = defaultdict(int)

    def add_edge(self, parent: Node, child: Node) -> None:
        self.edges[parent.id].append(child.id)

    def _generate_node_id(self, node_type: str) -> str:
        self._node_type_counter[node_type] += 1
        return f"{node_type}_{self._node_type_counter[node_type]}"

    def add_node(self, node_type: str, node_params: Dict, node_output_type: str) -> Node:
        node = Node(
            id=self._generate_node_id(node_type),
            type=node_type,
            parameters=node_params,
            output_type=node_output_type,
        )
        self.nodes[node.id] = {
            "type": node_type,
            "parameters": node_params,
            "output_type": node_output_type,
        }
        return node

    def to_dict(self):
        return {"nodes": self.nodes, "edges": dict(self.edges)}

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=4)


class ExecutionGraph(Graph):
    def __init__(self):
        super().__init__()
        self.node_to_ref_id = {}
        self.ref_to_node_id = {}

    def add_operation(
        self, node_type: str, node_params: Dict, node_output_type: str, input_nodes: List[Node]
    ) -> Node:
        input_node_refs = tuple(self.node_to_ref_id[node.id] for node in input_nodes)
        node_ref = hash_node(node_type, node_params, node_output_type, input_node_refs)
        if node_ref not in self.ref_to_node_id:
            node = self.add_node(node_type, node_params, node_output_type)
            for input_node in input_nodes:
                self.add_edge(input_node, node)

            self.ref_to_node_id[node_ref] = node.id
            self.node_to_ref_id[node.id] = node_ref
        else:
            node_id = self.ref_to_node_id[node_ref]
            node = Node(id=node_id, **self.nodes[node_id])
        return node
