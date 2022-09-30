"""
Implement graph data structure for query graph
"""
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Set, Tuple, TypedDict

import json
from collections import defaultdict

from pydantic import Field, root_validator

from featurebyte.common.singleton import SingletonMeta
from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.algorithm import dfs_traversal, topological_sort
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node import Node, construct_node
from featurebyte.query_graph.node.generic import AssignNode, GroupbyNode, InputNode
from featurebyte.query_graph.util import (
    get_aggregation_identifier,
    get_tile_table_identifier,
    hash_node,
)


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

    # non-serialized attributes (will be derived during deserialization)
    # NEVER store a non-serialized attributes that CAN'T BE DERIVED from serialized attributes
    nodes_map: Dict[str, Node] = Field(default_factory=dict, exclude=True)
    edges_map: Dict[str, List[str]] = Field(default=defaultdict(list), exclude=True)
    backward_edges_map: Dict[str, List[str]] = Field(default=defaultdict(list), exclude=True)
    node_type_counter: Dict[str, int] = Field(default=defaultdict(int), exclude=True)
    node_name_to_ref: Dict[str, str] = Field(default_factory=dict, exclude=True)
    ref_to_node_name: Dict[str, str] = Field(default_factory=dict, exclude=True)

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
        if not nodes_map:
            values["nodes_map"] = cls._derive_nodes_map(values["nodes"], nodes_map)

        edges_map = values.get("edges_map")
        if not edges_map:
            values["edges_map"] = cls._derive_edges_map(values["edges"], edges_map)

        backward_edges_map = values.get("backward_edges_map")
        if not backward_edges_map:
            values["backward_edges_map"] = cls._derive_backward_edges_map(
                values["edges"], backward_edges_map
            )

        node_type_counter = values.get("node_type_counter")
        if not node_type_counter:
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
        sorted_node_names = topological_sort(list(graph.nodes_map), graph.edges_map)
        for node_name in sorted_node_names:
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
            Query graph nodes of groupby type
        """
        for node in dfs_traversal(self, target_node):
            if node.type == node_type:
                yield node

    def _prune(
        self,
        target_node: Node,
        target_columns: Set[str],
        pruned_graph: "QueryGraph",
        processed_node_names: Set[str],
        node_name_map: Dict[str, str],
        index_map: Dict[str, int],
    ) -> "QueryGraph":
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
    ) -> Tuple["QueryGraph", Dict[str, str]]:
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
        sorted_node_names = topological_sort(list(self.nodes_map), self.edges_map)
        index_map = {value: idx for idx, value in enumerate(sorted_node_names)}
        pruned_graph = self._prune(
            target_node=target_node,
            target_columns=target_columns,
            pruned_graph=QueryGraph(),
            processed_node_names=set(),
            node_name_map=node_name_map,
            index_map=index_map,
        )
        return pruned_graph, node_name_map

    def add_groupby_operation(self, node_params: Dict[str, Any], input_node: Node) -> GroupbyNode:
        """
        Insert groupby operation

        Parameters
        ----------
        node_params: Dict[str, Any]
            Node parameters
        input_node: Node
            Input node

        Returns
        -------
        GroupbyNode

        Raises
        ------
        ValueError
            When the query graph have unexpected structure (groupby node should have at least an InputNode)
        """
        temp_node = GroupbyNode(
            name="temp", parameters=node_params, output_type=NodeOutputType.FRAME
        )
        pruned_graph, node_name_map = self.prune(
            target_node=input_node,
            target_columns=set(temp_node.get_required_input_columns()),
        )
        pruned_input_node_name = None
        table_details = None
        for node in dfs_traversal(self, input_node):
            if pruned_input_node_name is None and node.name in node_name_map:
                # as the input node could be pruned in the pruned graph, traverse the graph to find a valid input
                pruned_input_node_name = node_name_map[node.name]
            if isinstance(node, InputNode) and node.parameters.type == TableDataType.EVENT_DATA:
                # get the table details from the input node
                if table_details is None:
                    table_details = node.parameters.table_details.dict()

        if pruned_input_node_name is None or table_details is None:
            raise ValueError("Failed to add groupby operation.")

        tile_id = get_tile_table_identifier(
            table_details_dict=table_details, parameters=temp_node.parameters.dict()
        )
        aggregation_id = get_aggregation_identifier(
            transformations_hash=pruned_graph.node_name_to_ref[pruned_input_node_name],
            parameters=temp_node.parameters.dict(),
        )
        node = self.add_operation(
            node_type=NodeType.GROUPBY,
            node_params={
                **temp_node.parameters.dict(),
                "tile_id": tile_id,
                "aggregation_id": aggregation_id,
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[input_node],
        )
        assert isinstance(node, GroupbyNode)
        return node

    def reconstruct(self, replace_nodes_map: Dict[str, Node]) -> "QueryGraph":
        """
        Reconstruct the query graph using the replacement node mapping

        Parameters
        ----------
        replace_nodes_map: Dict[str, Node]
            Node name (of the input query graph) to replacement node mapping

        Returns
        -------
        QueryGraph
        """
        output = QueryGraph()
        sorted_node_names = topological_sort(list(self.nodes_map), self.edges_map)
        for node_name in sorted_node_names:
            input_node_names = self.backward_edges_map[node_name]
            node = replace_nodes_map.get(node_name, self.nodes_map[node_name])
            input_nodes = [
                replace_nodes_map.get(input_node_name, self.nodes_map[input_node_name])
                for input_node_name in input_node_names
            ]
            output.add_operation(
                node_type=node.type,
                node_params=node.parameters.dict(),
                node_output_type=node.output_type,
                input_nodes=input_nodes,
            )
        return output


class GraphState(TypedDict):
    """
    Typed dictionary for graph state
    """

    edges: List[Edge]
    nodes: List[Node]
    nodes_map: Dict[str, Node]
    edges_map: Dict[str, List[str]]
    backward_edges_map: Dict[str, List[str]]
    node_type_counter: Dict[str, int]
    node_name_to_ref: Dict[str, int]
    ref_to_node_name: Dict[int, str]


# query state field values
StateField = Literal[
    "edges",
    "nodes",
    "nodes_map",
    "edges_map",
    "backward_edges_map",
    "node_type_counter",
    "node_name_to_ref",
    "ref_to_node_name",
]


class GlobalGraphState(metaclass=SingletonMeta):
    """
    Global singleton to store query graph related attributes
    """

    _state: GraphState = {
        "edges": [],
        "nodes": [],
        "nodes_map": {},
        "edges_map": defaultdict(list),
        "backward_edges_map": defaultdict(list),
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
        cls._state["nodes_map"] = {}
        cls._state["edges_map"] = defaultdict(list)
        cls._state["backward_edges_map"] = defaultdict(list)
        cls._state["node_type_counter"] = defaultdict(int)
        cls._state["node_name_to_ref"] = {}
        cls._state["ref_to_node_name"] = {}

    @classmethod
    def construct_getter_func(cls, field: StateField) -> Callable[[], Any]:
        """
        Construct getter function

        Parameters
        ----------
        field: StateField
            Global state field value

        Returns
        -------
        Getter function to retrieve global state value
        """

        def getter() -> Any:
            """
            Getter function

            Returns
            -------
            Field value stored at global state
            """
            return cls._state[field]

        return getter


class GlobalQueryGraph(QueryGraph):
    """
    Global query graph used to store the core like operations for the SQL query construction
    """

    # all the attributes (including non-serialized) should be stored at GlobalGraphState
    edges: List[Edge] = Field(default_factory=GlobalGraphState.construct_getter_func("edges"))
    nodes: List[Node] = Field(default_factory=GlobalGraphState.construct_getter_func("nodes"))

    # non-serialized attributes
    nodes_map: Dict[str, Node] = Field(
        default_factory=GlobalGraphState.construct_getter_func("nodes_map"), exclude=True
    )
    edges_map: Dict[str, List[str]] = Field(
        default_factory=GlobalGraphState.construct_getter_func("edges_map"), exclude=True
    )
    backward_edges_map: Dict[str, List[str]] = Field(
        default_factory=GlobalGraphState.construct_getter_func("backward_edges_map"), exclude=True
    )
    node_type_counter: Dict[str, int] = Field(
        default_factory=GlobalGraphState.construct_getter_func("node_type_counter"),
        exclude=True,
    )
    node_name_to_ref: Dict[str, str] = Field(
        default_factory=GlobalGraphState.construct_getter_func("node_name_to_ref"),
        exclude=True,
    )
    ref_to_node_name: Dict[str, str] = Field(
        default_factory=GlobalGraphState.construct_getter_func("ref_to_node_name"),
        exclude=True,
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
