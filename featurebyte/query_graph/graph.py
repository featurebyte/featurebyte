"""
Implement graph data structure for query graph
"""
from typing import Any, Dict, List, Set, Tuple, TypedDict

import json
from collections import defaultdict

from pydantic import Field, validator

from featurebyte.common.singleton import SingletonMeta
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.algorithm import topological_sort
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node import Node, construct_node
from featurebyte.query_graph.node.sql import AssignNode
from featurebyte.query_graph.util import hash_node


class Graph(FeatureByteBaseModel):
    """
    Graph data structure
    """

    edges: Dict[str, List[str]] = Field(default=defaultdict(list))
    backward_edges: Dict[str, List[str]] = Field(default=defaultdict(list))
    nodes: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    node_type_counter: Dict[str, int] = Field(default=defaultdict(int))

    @validator("edges", "backward_edges")
    @classmethod
    def _convert_to_defaultdict_list(cls, value: Dict[str, List[str]]) -> Dict[str, List[str]]:
        if not isinstance(value, defaultdict):
            new_value: Dict[str, List[str]] = defaultdict(list)
            new_value.update(value)
            return new_value
        return value

    @validator("node_type_counter")
    @classmethod
    def _convert_to_defaultdict_int(cls, value: Dict[str, int]) -> Dict[str, int]:
        if not isinstance(value, defaultdict):
            new_value: Dict[str, int] = defaultdict(int)
            new_value.update(value)
            return new_value
        return value

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
        self.nodes[node.name] = node.dict()
        return node

    def __repr__(self) -> str:
        return json.dumps(self.dict(), indent=4)


class QueryGraph(Graph):
    """
    Query graph object
    """

    # pylint: disable=too-few-public-methods

    node_name_to_ref: Dict[str, str] = Field(default_factory=dict)
    ref_to_node_name: Dict[str, str] = Field(default_factory=dict)

    class Config:
        """
        Pydantic Config class
        """

        fields = {
            "node_name_to_ref": {"exclude": True},
            "ref_to_node_name": {"exclude": True},
            "node_type_counter": {"exclude": True},
        }

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
        return construct_node(**self.nodes[node_name])

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
        node_ref = hash_node(node_type, node_params, node_output_type, input_node_refs)
        if node_ref not in self.ref_to_node_name:
            node = self.add_node(node_type, node_params, node_output_type)
            for input_node in input_nodes:
                self.add_edge(input_node, node)

            self.ref_to_node_name[node_ref] = node.name
            self.node_name_to_ref[node.name] = node_ref
        else:
            name = self.ref_to_node_name[node_ref]
            node = construct_node(**self.nodes[name])
        return node

    def load(self, graph: "QueryGraph") -> Tuple["QueryGraph", Dict[str, str]]:
        """
        Load the query graph into the global query graph

        Parameters
        ----------
        graph: QueryGraph
            query graph object to be loaded

        Returns
        -------
        QueryGraph, dict[str, str]
            updated query graph with the node name mapping between input query graph & output query graph
        """
        node_name_map: Dict[str, str] = {}
        for node_name in topological_sort(graph):
            node = graph.get_node_by_name(node_name)
            input_nodes = [
                self.get_node_by_name(node_name_map[input_node_name])
                for input_node_name in graph.backward_edges[node_name]
            ]
            node_global = self.add_operation(
                node_type=NodeType(node.type),
                node_params=node.parameters.dict(),
                node_output_type=NodeOutputType(node.output_type),
                input_nodes=input_nodes,
            )
            node_name_map[node_name] = node_global.name
        return self, node_name_map


class GraphState(TypedDict):
    """
    Typed dictionary for graph state
    """

    edges: Dict[str, List[str]]
    backward_edges: Dict[str, List[str]]
    nodes: Dict[str, Dict[str, Any]]
    node_type_counter: Dict[str, int]
    node_name_to_ref: Dict[str, int]
    ref_to_node_name: Dict[int, str]


class GlobalQueryGraphState(metaclass=SingletonMeta):
    """
    Global singleton to store query graph related attributes
    """

    _state: GraphState = {
        "edges": defaultdict(list),
        "backward_edges": defaultdict(list),
        "nodes": {},
        "node_type_counter": defaultdict(int),
        "node_name_to_ref": {},
        "ref_to_node_name": {},
    }

    @classmethod
    def reset(cls) -> None:
        """
        Reset the global query graph state to clean state
        """
        cls._state["edges"] = defaultdict(list)
        cls._state["backward_edges"] = defaultdict(list)
        cls._state["nodes"] = {}
        cls._state["node_type_counter"] = defaultdict(int)
        cls._state["node_name_to_ref"] = {}
        cls._state["ref_to_node_name"] = {}

    @classmethod
    def get_edges(cls) -> Dict[str, List[str]]:
        """
        Get global query graph edges

        Returns
        -------
        dict[str, list[str]]
        """
        return cls._state["edges"]

    @classmethod
    def get_backward_edges(cls) -> Dict[str, List[str]]:
        """
        Get global query graph backward edges

        Returns
        -------
        dict[str, list[str]]
        """
        return cls._state["backward_edges"]

    @classmethod
    def get_nodes(cls) -> Dict[str, Dict[str, Any]]:
        """
        Get global query graph nodes

        Returns
        -------
        dict[str, Any]
        """
        return cls._state["nodes"]

    @classmethod
    def get_node_type_counter(cls) -> Dict[str, int]:
        """
        Get global query node type counter

        Returns
        -------
        dict[str, int]
        """
        return cls._state["node_type_counter"]

    @classmethod
    def get_node_name_to_ref(cls) -> Dict[str, int]:
        """
        Get global query node name to node hash dictionary

        Returns
        -------
        dict[str, int]
        """
        return cls._state["node_name_to_ref"]

    @classmethod
    def get_ref_to_node_name(cls) -> Dict[int, str]:
        """
        Get global query node hash to node name dictionary

        Returns
        -------
        dict[int, str]
        """
        return cls._state["ref_to_node_name"]


class GlobalQueryGraph(QueryGraph):
    """
    Global query graph used to store the core like operations for the SQL query construction
    """

    edges: Dict[str, List[str]] = Field(default_factory=GlobalQueryGraphState.get_edges)
    backward_edges: Dict[str, List[str]] = Field(
        default_factory=GlobalQueryGraphState.get_backward_edges
    )
    nodes: Dict[str, Dict[str, Any]] = Field(default_factory=GlobalQueryGraphState.get_nodes)
    node_type_counter: Dict[str, int] = Field(
        default_factory=GlobalQueryGraphState.get_node_type_counter
    )
    node_name_to_ref: Dict[str, str] = Field(
        default_factory=GlobalQueryGraphState.get_node_name_to_ref
    )
    ref_to_node_name: Dict[str, str] = Field(
        default_factory=GlobalQueryGraphState.get_ref_to_node_name
    )

    def copy(self, *args: Any, **kwargs: Any) -> "GlobalQueryGraph":
        # under no circumstances we should allow making copy of GlobalQueryGraph
        _ = args, kwargs
        return GlobalQueryGraph()

    def __copy__(self, *args: Any, **kwargs: Any) -> "GlobalQueryGraph":
        # under no circumstances we should allow making copy of GlobalQueryGraph
        _ = args, kwargs
        return GlobalQueryGraph()

    def __deepcopy__(self, *args: Any, **kwargs: Any) -> "GlobalQueryGraph":
        # under no circumstances we should allow making copy of GlobalQueryGraph
        _ = args, kwargs
        return GlobalQueryGraph()

    def _prune(
        self,
        target_node: Node,
        target_columns: Set[str],
        pruned_graph: QueryGraph,
        processed_node_names: Set[str],
        node_name_map: Dict[str, str],
        index_map: Dict[str, int],
    ) -> QueryGraph:
        # pylint: disable=too-many-locals
        # pruning: move backward from target node to the input node
        to_prune_target_node = False
        input_node_names = self.backward_edges.get(target_node.name, [])

        if isinstance(target_node, AssignNode):
            # check whether to keep the current assign node
            assign_column_name = target_node.parameters.name
            if assign_column_name in target_columns:
                # remove matched name from the target_columns
                target_columns -= {assign_column_name}
            else:
                # remove series path if exists
                to_prune_target_node = True
                input_node_names = input_node_names[:1]
        else:
            # Update target_columns to include list of required columns for the current node operations
            target_columns.update(target_node.get_required_input_columns())

        # If the current target node produces a new column, we should remove it from the target_columns
        # (as the condition has been matched). If it is not removed, the pruning algorithm may keep the unused
        # assign operation that generate the same column name.
        target_columns = target_columns.difference(target_node.get_new_output_columns())

        # reverse topological sort to make sure "target_columns" get filled properly. Example:
        # edges = {"assign_1": ["groupby_1", "project_1"], "project_1", ["groupby_1"], ...}
        # Here, "groupby_1" node have 2 input_node_names ("assign_1" and "project_1"), reverse topological
        # sort makes sure we travel "project_1" first (filled up target_columns) and then travel assign node.
        # If the assign node's new columns are not in "target_columns", we can safely remove the node.
        for input_node_name in sorted(input_node_names, key=lambda x: index_map[x], reverse=True):
            input_node = self.get_node_by_name(input_node_name)
            pruned_graph = self._prune(
                target_node=input_node,
                target_columns=target_columns,
                pruned_graph=pruned_graph,
                processed_node_names=processed_node_names,
                node_name_map=node_name_map,
                index_map=index_map,
            )

        if to_prune_target_node:
            # do not add the target_node to the pruned graph
            return pruned_graph

        # reconstruction: process the node from the input node towards the target node
        mapped_input_node_names = []
        for input_node_name in input_node_names:
            # if the input node get pruned, it will not exist in the processed_node_names.
            # in this case, keep finding the first parent node exists in the processed_node_names.
            # currently only ASSIGN node could get pruned, the first input node is the frame node.
            # it is used to replace the pruned assigned node
            while input_node_name not in processed_node_names:
                input_node_name = self.backward_edges[input_node_name][0]
            mapped_input_node_names.append(input_node_name)

        # add the node back to the pruned graph
        input_nodes = [
            pruned_graph.get_node_by_name(node_name_map[node_name])
            for node_name in mapped_input_node_names
        ]
        node_pruned = pruned_graph.add_operation(
            node_type=NodeType(target_node.type),
            node_params=target_node.parameters.dict(),
            node_output_type=NodeOutputType(target_node.output_type),
            input_nodes=input_nodes,
        )

        # update the container to store the mapped node name & processed nodes information
        node_name_map[target_node.name] = node_pruned.name
        processed_node_names.add(target_node.name)
        return pruned_graph

    def prune(
        self, target_node: Node, target_columns: Set[str]
    ) -> Tuple[QueryGraph, Dict[str, str]]:
        """
        Prune the query graph and return the pruned graph & mapped node.

        To prune the graph, this function first traverses from the target node to the input node.
        The unused branches of the graph will get pruned in this step. After that, a new graph is
        reconstructed by adding the required nodes back.

        Parameters
        ----------
        target_node: Node
            target end node
        target_columns: set[str]
            list of target columns

        Returns
        -------
        QueryGraph, node_name_map
        """
        node_name_map: Dict[str, str] = {}
        index_map = {value: idx for idx, value in enumerate(topological_sort(self))}
        pruned_graph = self._prune(
            target_node=target_node,
            target_columns=target_columns,
            pruned_graph=QueryGraph(),
            processed_node_names=set(),
            node_name_map=node_name_map,
            index_map=index_map,
        )
        return pruned_graph, node_name_map
