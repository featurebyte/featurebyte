"""
Implement graph data structure for query graph
"""
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, TypedDict

import json
from collections import defaultdict

from pydantic import Field, PrivateAttr

from featurebyte.common.singleton import SingletonMeta
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.algorithm import dfs_traversal, topological_sort
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node import Node, construct_node
from featurebyte.query_graph.node.generic import AssignNode
from featurebyte.query_graph.util import hash_node


class Edge(FeatureByteBaseModel):
    """
    Graph Edge
    """

    source: str
    target: str


class QueryGraph(FeatureByteBaseModel):
    """
    Graph data structure
    """

    edges: List[Edge] = Field(default_factory=list)
    nodes: List[Node] = Field(default_factory=list)

    # non-serialized variables
    node_type_counter: Dict[str, int] = Field(default=defaultdict(int), exclude=True)
    node_name_to_ref: Dict[str, str] = Field(default_factory=dict, exclude=True)
    ref_to_node_name: Dict[str, str] = Field(default_factory=dict, exclude=True)
    _nodes_map: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _edges_map: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _backward_edges_map: Optional[Dict[str, Any]] = PrivateAttr(default=None)

    @property
    def nodes_map(self) -> Dict[str, Node]:
        """
        Graph node name to node mapping

        Returns
        -------
        Dict[str, Node]
        """
        if self._nodes_map is None or len(self._nodes_map) != len(self.nodes):
            self._nodes_map = {}
            for node in self.nodes:
                self._nodes_map[node.name] = node
        return self._nodes_map

    @property
    def edges_map(self) -> Dict[str, List[str]]:
        """
        Graph parent node name to child node names mapping

        Returns
        -------
        Dict[str, List[str]]
        """
        if self._edges_map is None:
            self._edges_map = defaultdict(list)
            for edge in self.edges:
                self._edges_map[edge.source].append(edge.target)
        return self._edges_map

    @property
    def backward_edges_map(self) -> Dict[str, List[str]]:
        """
        Graph child node name to parent node names mapping

        Returns
        -------
        Dict[str, List[str]]
        """
        if self._backward_edges_map is None:
            self._backward_edges_map = defaultdict(list)
            for edge in self.edges:
                self._backward_edges_map[edge.target].append(edge.source)
        return self._backward_edges_map

    def __repr__(self) -> str:
        return json.dumps(self.dict(), indent=4)

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
        return node

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

        # remove cached maps
        self._nodes_map = None
        self._edges_map = None
        self._backward_edges_map = None
        return node

    def load(self, graph: "QueryGraph") -> Tuple["QueryGraph", Dict[str, str]]:
        """
        Load the query graph into the query graph

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
                for input_node_name in graph.backward_edges_map[node_name]
            ]
            node_global = self.add_operation(
                node_type=NodeType(node.type),
                node_params=node.parameters.dict(),
                node_output_type=NodeOutputType(node.output_type),
                input_nodes=input_nodes,
            )
            node_name_map[node_name] = node_global.name
        return self, node_name_map

    def iterate_grouby_nodes(self, target_node: Node) -> Iterator[Node]:
        """
        Iterate all groupby nodes in this query graph

        Parameters
        ----------
        target_node: Node
            Node from which to start the backward search

        Yields
        ------
        Node
            Query graph nodes of groupby type
        """
        for node in dfs_traversal(self, target_node):
            if node.type == NodeType.GROUPBY:
                yield node


class GraphState(TypedDict):
    """
    Typed dictionary for graph state
    """

    edges: List[Edge]
    nodes: List[Node]
    node_type_counter: Dict[str, int]
    node_name_to_ref: Dict[str, int]
    ref_to_node_name: Dict[int, str]


class GlobalQueryGraphState(metaclass=SingletonMeta):
    """
    Global singleton to store query graph related attributes
    """

    _state: GraphState = {
        "edges": [],
        "nodes": [],
        "node_type_counter": defaultdict(int),
        "node_name_to_ref": {},
        "ref_to_node_name": {},
    }

    @classmethod
    def reset(cls) -> None:
        """
        Reset the global query graph state to clean state
        """
        cls._state["edges"] = []
        cls._state["nodes"] = []
        cls._state["node_type_counter"] = defaultdict(int)
        cls._state["node_name_to_ref"] = {}
        cls._state["ref_to_node_name"] = {}

    @classmethod
    def get_edges(cls) -> List[Edge]:
        """
        Get global query graph edges

        Returns
        -------
        dict[str, list[str]]
        """
        return cls._state["edges"]

    @classmethod
    def get_nodes(cls) -> List[Node]:
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

    edges: List[Edge] = Field(default_factory=GlobalQueryGraphState.get_edges)
    nodes: List[Node] = Field(default_factory=GlobalQueryGraphState.get_nodes)
    node_type_counter: Dict[str, int] = Field(
        default_factory=GlobalQueryGraphState.get_node_type_counter, exclude=True
    )
    node_name_to_ref: Dict[str, str] = Field(
        default_factory=GlobalQueryGraphState.get_node_name_to_ref, exclude=True
    )
    ref_to_node_name: Dict[str, str] = Field(
        default_factory=GlobalQueryGraphState.get_ref_to_node_name, exclude=True
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
        input_node_names = self.backward_edges_map.get(target_node.name, [])

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
            input_node = self.nodes_map[input_node_name]
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
                input_node_name = self.backward_edges_map[input_node_name][0]
            mapped_input_node_names.append(input_node_name)

        # add the node back to the pruned graph
        input_nodes = [
            pruned_graph.nodes_map[node_name_map[node_name]]
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
