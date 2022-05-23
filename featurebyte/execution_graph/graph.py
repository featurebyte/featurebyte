"""
Implement graph data structure for execution graph
"""
from typing import Dict

import json
from collections import defaultdict
from dataclasses import dataclass


class SingletonMeta(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


@dataclass()
class Node:
    id: str
    type: str
    parameters: Dict


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

    def add_node(self, node_type: str, node_params: Dict) -> Node:
        node = Node(
            id=self._generate_node_id(node_type),
            type=node_type,
            parameters=node_params,
        )
        self.nodes[node.id] = {"type": node_type, "parameters": node_params}
        return node

    def to_dict(self):
        return {"nodes": self.nodes, "edges": self.edges}

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=4)
