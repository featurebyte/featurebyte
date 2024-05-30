"""
This model contains query graph internal model structures
"""

from typing import Any, DefaultDict, Dict, Iterator, List, Optional, Set, Tuple, cast

from collections import defaultdict

from pydantic import Field, PrivateAttr, root_validator, validator

from featurebyte.exception import GraphInconsistencyError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.algorithm import dfs_traversal, topological_sort
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.node import Node, construct_node
from featurebyte.query_graph.node.generic import AliasNode, ProjectNode
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import BaseGraphNode
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

    # private attributes used for caching and internal computation
    _total_node_num: Optional[int] = PrivateAttr(default=None)
    _sorted_node_names_by_ref: List[str] = PrivateAttr(default_factory=list)
    _sorted_edges_map_by_ref: Dict[str, List[str]] = PrivateAttr(default=defaultdict(list))
    _sorted_node_names: List[str] = PrivateAttr(default_factory=list)
    _node_topological_order_map: Dict[str, Any] = PrivateAttr(default_factory=dict)

    def __repr__(self) -> str:
        return self.json(by_alias=True, indent=4)

    def __str__(self) -> str:
        return repr(self)

    def _is_cache_invalid(self) -> bool:
        """
        Check if the cache is invalid

        Returns
        -------
        bool
        """
        # as the graph only supports insertion, the total node number will only increase
        # use the total node number to check if the cache is invalid
        return self._total_node_num != len(self.nodes)

    def _update_cache(self) -> None:
        """
        Update cache
        """
        # To make the order insensitive to the node names, we first sort the backward edges map by node hash.
        # Backward edges map is used due to the fact that input node order are important to the node operation.
        # If edges map is used, the input order will be lost. After that, we reconstruct the edges map from
        # the sorted backward edges map (required for topological sort).
        edges_map = defaultdict(list)
        sorted_backward_edges_keys = sorted(
            self.backward_edges_map, key=lambda x: self.node_name_to_ref[x]
        )
        for target_node_name in sorted_backward_edges_keys:
            for source_node_name in self.backward_edges_map[target_node_name]:
                edges_map[source_node_name].append(target_node_name)
        self._sorted_edges_map_by_ref = edges_map

        # Update sorted node names by reference
        self._sorted_node_names_by_ref = sorted(
            self.nodes_map, key=lambda x: self.node_name_to_ref[x]
        )

        # Update node topological order map
        self._sorted_node_names = topological_sort(
            self._sorted_node_names_by_ref, self._sorted_edges_map_by_ref
        )
        self._node_topological_order_map = {
            value: idx for idx, value in enumerate(self._sorted_node_names)
        }

        # Update total node number to validate the cache
        self._total_node_num = len(self.nodes)

    @property
    def sorted_node_names_by_ref(self) -> List[str]:
        """
        Sorted node names by reference

        Returns
        -------
        List[str]
        """
        if self._is_cache_invalid():
            self._update_cache()
        return self._sorted_node_names_by_ref

    @property
    def sorted_node_names(self) -> List[str]:
        """
        Topologically sorted node names

        Returns
        -------
        List[str]
        """
        if self._is_cache_invalid():
            self._update_cache()
        return self._sorted_node_names

    @property
    def sorted_edges_map_by_ref(self) -> Dict[str, List[str]]:
        """
        Sorted edges map by reference

        Returns
        -------
        Dict[str, List[str]]
        """
        if self._is_cache_invalid():
            self._update_cache()
        return self._sorted_edges_map_by_ref

    @property
    def node_topological_order_map(self) -> Dict[str, int]:
        """
        Node name to topological sort order index mapping. This mapping is used to sort the nodes in the graph.

        Returns
        -------
        Dict[str, int]
        """
        if self._is_cache_invalid():
            self._update_cache()
        return self._node_topological_order_map

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
    def _get_node_parameter_for_compute_node_hash(node: Node) -> Dict[str, Any]:
        """
        Get node parameters for computing node hash. If the node is a graph node, the output node hash of the
        nested graph is used to represent the graph parameters. Without doing this, the graph node's hash will
        be sensitive to the order of the nodes/edges in the nested graph.

        Parameters
        ----------
        node: Node
            Node to get parameters for computing node hash

        Returns
        -------
        Dict[str, Any]
        """
        node_parameters = node.parameters.dict()
        if node.type == NodeType.GRAPH:
            nested_graph = node.parameters.graph  # type: ignore
            node_parameters["graph"] = nested_graph.node_name_to_ref[node.parameters.output_node_name]  # type: ignore
            # remove node name from graph parameters to prevent the node name
            # from affecting the graph hash (node name could be different if the insert order is different)
            # even if the final graph is the same
            node_parameters.pop("output_node_name")
        if node.type == NodeType.INPUT:
            # exclude feature_store_details.details from input node hash if it exists
            node_parameters["feature_store_details"].pop("details", None)
        if node.type == NodeType.GROUPBY:
            node_parameters.pop("tile_id_version", None)
            fjs = node_parameters.pop("feature_job_setting")
            node_parameters["frequency"] = int(fjs["period"].rstrip("s"))
            node_parameters["time_modulo_frequency"] = int(fjs["offset"].rstrip("s"))
            node_parameters["blind_spot"] = int(fjs["blind_spot"].rstrip("s"))
            # keep node hash the same if not provided so that a new window aggregate feature without
            # offset has the same definition hash as an old feature before window offset was
            # introduced.
            if node_parameters.get("offset") is None:
                node_parameters.pop("offset", None)
        return node_parameters

    @classmethod
    def _derive_node_name_to_ref(
        cls,
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
                cls._get_node_parameter_for_compute_node_hash(node),
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

    def get_target_nodes_required_column_names(
        self,
        node_name: str,
        keep_target_node_names: Optional[Set[str]],
        available_column_names: List[str],
    ) -> List[str]:
        """
        Get the target required column names of the given node.
        Current node output must have these columns, otherwise it will trigger error in processing the graph.

        Parameters
        ----------
        node_name: str
            Node name
        keep_target_node_names: Optional[Set[str]]
            If provided, only use the target nodes with names in the set
        available_column_names: List[str]
            List of available input columns. This is used to cater the case when the node output should keep all
            the input columns (like filter node).

        Returns
        -------
        List[str]
        """
        assert node_name in self.edges_map, "Node name not found in edges_map"
        target_node_names = self.edges_map[node_name]
        if keep_target_node_names:
            target_node_names = [
                node_name for node_name in target_node_names if node_name in keep_target_node_names
            ]
        target_nodes = [self.get_node_by_name(node_name) for node_name in target_node_names]
        if target_nodes:
            # get the input column order from current node to the target nodes
            target_node_input_order_pairs = []
            for target_node in target_nodes:
                target_node_input_node_names = self.get_input_node_names(node=target_node)
                node_name_input_order = target_node_input_node_names.index(node_name)
                target_node_input_order_pairs.append((target_node, node_name_input_order))

            # construct required column names
            required_columns = set().union(
                *(
                    node.get_required_input_columns(
                        input_index=input_order, available_column_names=available_column_names
                    )
                    for node, input_order in target_node_input_order_pairs
                )
            )
            return list(required_columns)
        return []

    def get_node_output_column_name(self, node_name: str) -> Optional[str]:
        """
        Get the output column name of the given node. The node should correspond to a single column
        (i.e. project or alias node), otherwise this returns None.

        Parameters
        ----------
        node_name: str
            Node name

        Returns
        -------
        Optional[str]
        """
        node = self.get_node_by_name(node_name)
        output_column_name = None
        if isinstance(node, AliasNode):
            output_column_name = cast(str, node.parameters.name)
        elif isinstance(node, ProjectNode):
            output_column_name = cast(str, node.parameters.columns[0])
        return output_column_name

    def has_node_type(self, target_node: Node, node_type: NodeType) -> bool:
        """
        Check if the query sub-graph has a specific node type

        Parameters
        ----------
        target_node: Node
            Target node used to start the search
        node_type: NodeType
            Node type to check

        Returns
        -------
        bool
            True if the query sub-graph has a request column node, False otherwise
        """
        for _ in self.iterate_nodes(target_node=target_node, node_type=node_type):
            return True
        return False

    def iterate_nodes(
        self,
        target_node: Node,
        node_type: Optional[NodeType],
        skip_node_type: Optional[NodeType] = None,
        skip_node_names: Optional[Set[str]] = None,
    ) -> Iterator[Node]:
        """
        Iterate all specified nodes in this query graph

        Parameters
        ----------
        target_node: Node
            Node from which to start the backward search
        node_type: Optional[NodeType]
            Specific node type to iterate, if None, iterate all node types
        skip_node_type : Optional[NodeType]
            If specified, skip nodes of this type during traversal
        skip_node_names: Optional[Set[str]]
            If specified, skip nodes of these names during traversal

        Yields
        ------
        Node
            Query graph nodes of the specified node type
        """
        for node in dfs_traversal(
            self, target_node, skip_node_type=skip_node_type, skip_node_names=skip_node_names
        ):
            if node_type is None:
                yield node
            else:
                if node.type == node_type:
                    yield node

    def iterate_sorted_graph_nodes(
        self, graph_node_types: Set[GraphNodeType]
    ) -> Iterator[BaseGraphNode]:
        """
        Iterate all specified nodes in this query graph in a topologically sorted order

        Parameters
        ----------
        graph_node_types: Set[GraphNodeType]
            Specific node types to iterate

        Yields
        ------
        BaseGraphNode
            Graph nodes of the specified graph node types
        """
        for node in self.iterate_sorted_nodes():
            if node.type == NodeType.GRAPH:
                assert isinstance(node, BaseGraphNode)
                if node.parameters.type in graph_node_types:
                    yield node
                else:
                    for graph_node in node.parameters.graph.iterate_sorted_graph_nodes(
                        graph_node_types=graph_node_types
                    ):
                        yield graph_node

    def iterate_sorted_nodes(self) -> Iterator[Node]:
        """
        Iterate all nodes in topological sorted order

        Yields
        ------
        Node
            Topologically sorted query graph nodes
        """
        for node_name in self.sorted_node_names:
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

    def _add_node(self, node: Node) -> Node:
        """
        Add node to the graph by specifying node type, parameters & output type

        Parameters
        ----------
        node: Node
            Node to add

        Returns
        -------
        node: Node
        """
        node = node.copy()
        node.name = self._generate_node_name(node.type)
        self.nodes.append(node)
        self.nodes_map[node.name] = node
        return node

    def add_operation_node(self, node: Node, input_nodes: List[Node]) -> Node:
        """
        Add operation node to the query graph.

        Parameters
        ----------
        node: Node
            operation node to add
        input_nodes: list[Node]
            list of input nodes

        Returns
        -------
        Node
            operation node of the given input
        """
        input_node_refs = [self.node_name_to_ref[node.name] for node in input_nodes]
        node_ref = hash_node(
            node.type,
            self._get_node_parameter_for_compute_node_hash(node),
            node.output_type,
            input_node_refs,
        )
        if node_ref not in self.ref_to_node_name:
            node = self._add_node(node)
            for input_node in input_nodes:
                self._add_edge(input_node, node)

            self.ref_to_node_name[node_ref] = node.name
            self.node_name_to_ref[node.name] = node_ref
        else:
            node = self.nodes_map[self.ref_to_node_name[node_ref]]
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
        # create a temp_node to validate the node parameters & use only the required parameters to hash
        temp_node = construct_node(
            name=str(node_type),
            type=node_type,
            parameters=node_params,
            output_type=node_output_type,
        )
        return self.add_operation_node(node=temp_node, input_nodes=input_nodes)


NodeNameMap = Dict[str, str]
GraphNodeNameMap = Tuple[QueryGraphModel, NodeNameMap]
