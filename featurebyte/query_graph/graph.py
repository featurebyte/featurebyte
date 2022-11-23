"""
Implement graph data structure for query graph
"""
from typing import Any, Callable, Dict, List, Literal, Set, Tuple, TypedDict, cast

from collections import defaultdict

from pydantic import Field

from featurebyte.common.singleton import SingletonMeta
from featurebyte.enum import TableDataType
from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model import Edge, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import AssignNode, GroupbyNode, InputNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.nested import BaseGraphNode, GraphNodeParameters, ProxyInputNode
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier


class QueryGraph(QueryGraphModel):
    """
    Graph data structure
    """

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
        for node in graph.iterate_sorted_nodes():
            input_nodes = [
                self.get_node_by_name(node_name_map[input_node_name])
                for input_node_name in graph.backward_edges_map[node.name]
            ]
            node_global = self.add_operation(
                node_type=NodeType(node.type),
                node_params=node.parameters.dict(),
                node_output_type=NodeOutputType(node.output_type),
                input_nodes=input_nodes,
            )
            node_name_map[node.name] = node_global.name
        return self, node_name_map

    def _prune(
        self,
        target_node: Node,
        target_columns: Set[str],
        pruned_graph: "QueryGraph",
        processed_node_names: Set[str],
        node_name_map: Dict[str, str],
        topological_order_map: Dict[str, int],
    ) -> "QueryGraph":
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

        # If the current target node produces a new column, we should remove it from the target_columns
        # (as the condition has been matched). If it is not removed, the pruning algorithm may keep the unused
        # assign operation that generate the same column name.
        target_columns = target_columns.difference(target_node.get_new_output_columns())

        # Update target_columns to include list of required columns for the current node operations
        target_columns.update(target_node.get_required_input_columns())

        # reverse topological sort to make sure "target_columns" get filled properly. Example:
        # edges = {"assign_1": ["groupby_1", "project_1"], "project_1", ["groupby_1"], ...}
        # Here, "groupby_1" node have 2 input_node_names ("assign_1" and "project_1"), reverse topological
        # sort makes sure we travel "project_1" first (filled up target_columns) and then travel assign node.
        # If the assign node's new columns are not in "target_columns", we can safely remove the node.
        for input_node_name in sorted(
            input_node_names, key=lambda x: topological_order_map[x], reverse=True
        ):
            pruned_graph = self._prune(
                target_node=self.nodes_map[input_node_name],
                target_columns=target_columns,
                pruned_graph=pruned_graph,
                processed_node_names=processed_node_names,
                node_name_map=node_name_map,
                topological_order_map=topological_order_map,
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
        pruned_graph = self._prune(
            target_node=target_node,
            target_columns=target_columns,
            pruned_graph=QueryGraph(),
            processed_node_names=set(),
            node_name_map=node_name_map,
            topological_order_map=self.node_topological_order_map,
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
        # create a temporary groupby node & prune the graph to generate tile_id & aggregation_id
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
                table_details = node.parameters.table_details.dict()
                break

        if pruned_input_node_name is None or table_details is None:
            raise ValueError("Failed to add groupby operation.")

        # tile_id & aggregation_id should be based on pruned graph to improve tile reuse
        tile_id = get_tile_table_identifier(
            table_details_dict=table_details, parameters=temp_node.parameters.dict()
        )
        aggregation_id = get_aggregation_identifier(
            transformations_hash=pruned_graph.node_name_to_ref[pruned_input_node_name],
            parameters=temp_node.parameters.dict(),
        )

        # insert the groupby node using the generated tile_id & aggregation_id
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
        return cast(GroupbyNode, node)

    def reconstruct(
        self, replace_nodes_map: Dict[str, Node], regenerate_groupby_hash: bool = False
    ) -> "QueryGraph":
        """
        Reconstruct the query graph using the replacement node mapping

        Parameters
        ----------
        replace_nodes_map: Dict[str, Node]
            Node name (of the input query graph) to replacement node mapping
        regenerate_groupby_hash: bool
            Whether to regenerate tile ID & aggregation ID in groupby node

        Returns
        -------
        QueryGraph
        """
        output = QueryGraph()
        for _node in self.iterate_sorted_nodes():
            input_node_names = self.backward_edges_map[_node.name]
            node = replace_nodes_map.get(_node.name, self.nodes_map[_node.name])
            input_nodes = [
                replace_nodes_map.get(input_node_name, self.nodes_map[input_node_name])
                for input_node_name in input_node_names
            ]
            if node.type == NodeType.GROUPBY and regenerate_groupby_hash:
                output.add_groupby_operation(
                    node_params=node.parameters.dict(),
                    input_node=input_nodes[0],
                )
            else:
                output.add_operation(
                    node_type=node.type,
                    node_params=node.parameters.dict(),
                    node_output_type=node.output_type,
                    input_nodes=input_nodes,
                )
        return output

    def _extract_operation_structure(
        self,
        node: Node,
        visited_node_types: Set[NodeType],
        topological_order_map: Dict[str, int],
    ) -> OperationStructure:
        input_node_names = self.backward_edges_map.get(node.name, [])
        input_node_map = {}
        for input_node_name in sorted(
            input_node_names, key=lambda x: topological_order_map[x], reverse=True
        ):
            input_node = self.nodes_map[input_node_name]
            input_node_map[input_node_name] = self._extract_operation_structure(
                node=input_node,
                visited_node_types=visited_node_types.union([node.type]),
                topological_order_map=topological_order_map,
            )

        return node.derive_node_operation_info(
            inputs=[input_node_map[node_name] for node_name in input_node_names],
            visited_node_types=visited_node_types,
        )

    def extract_operation_structure(self, node: Node) -> OperationStructure:
        """
        Extract operation structure from the graph given target node

        Parameters
        ----------
        node: Node
            Target node used to construct the operation structure

        Returns
        -------
        OperationStructure
        """
        return self._extract_operation_structure(
            node=node,
            visited_node_types=set(),
            topological_order_map=self.node_topological_order_map,
        )

    def add_graph_node(self, graph_node: "GraphNode", input_nodes: List[Node]) -> Node:
        """
        Add graph node to the graph

        Parameters
        ----------
        graph_node: GraphNode
            Graph node
        input_nodes: List[Node]
            Input nodes to the graph node

        Returns
        -------
        Node
        """
        return self.add_operation(
            node_type=graph_node.type,
            node_params=graph_node.parameters.dict(),
            node_output_type=graph_node.output_node.output_type,
            input_nodes=input_nodes,
        )

    def flatten(self) -> "QueryGraph":
        """
        Construct a query graph which flattened all the graph nodes of this query graph

        Returns
        -------
        QueryGraph
        """
        flattened_graph = QueryGraph()
        # node_name_map: key(this-graph-node-name) => value(flattened-graph-node-name)
        node_name_map: Dict[str, str] = {}
        for node in self.iterate_sorted_nodes():
            if isinstance(node, BaseGraphNode):
                nested_graph = node.parameters.graph.flatten()
                # nested_node_name_map: key(nested-node-name) => value(flattened-graph-node-name)
                nested_node_name_map: Dict[str, str] = {}
                for nested_node in nested_graph.iterate_sorted_nodes():
                    input_nodes = []
                    for input_node_name in nested_graph.backward_edges_map[nested_node.name]:
                        input_nodes.append(
                            flattened_graph.get_node_by_name(nested_node_name_map[input_node_name])
                        )

                    if isinstance(nested_node, ProxyInputNode):
                        nested_node_name_map[nested_node.name] = node_name_map[
                            nested_node.parameters.node_name
                        ]
                    else:
                        inserted_node = flattened_graph.add_operation(
                            node_type=nested_node.type,
                            node_params=nested_node.parameters.dict(),
                            node_output_type=nested_node.output_type,
                            input_nodes=input_nodes,
                        )
                        nested_node_name_map[nested_node.name] = inserted_node.name

                    if nested_node.name == node.parameters.output_node_name:
                        node_name_map[node.name] = nested_node_name_map[nested_node.name]
            else:
                inserted_node = flattened_graph.add_operation(
                    node_type=node.type,
                    node_params=node.parameters.dict(),
                    node_output_type=node.output_type,
                    input_nodes=[
                        flattened_graph.get_node_by_name(node_name_map[input_node_name])
                        for input_node_name in self.backward_edges_map[node.name]
                    ],
                )
                node_name_map[node.name] = inserted_node.name
        return flattened_graph


# update forward references after QueryGraph is defined
GraphNodeParameters.update_forward_refs(QueryGraph=QueryGraph)


class GraphNode(BaseGraphNode):
    """
    Extend graph node by providing additional graph-node-construction-related methods
    """

    @classmethod
    def create(
        cls,
        node_type: NodeType,
        node_params: Dict[str, Any],
        node_output_type: NodeOutputType,
        input_nodes: List[Node],
    ) -> Tuple["GraphNode", List[Node]]:
        """
        Construct a graph node

        Parameters
        ----------
        node_type: NodeType
            Type of node (to be inserted in the graph inside the graph node)
        node_params: Dict[str, Any]
            Parameters of the node (to be inserted in the graph inside the graph node)
        node_output_type: NodeOutputType
            Output type of the node (to be inserted in the graph inside the graph node)
        input_nodes: List[Nodes]
            Input nodes of the node (to be inserted in the graph inside the graph node)

        Returns
        -------
        Tuple[GraphNode, List[Node]]
        """
        graph = QueryGraph()
        proxy_input_nodes = []
        for node in input_nodes:
            proxy_input_node = graph.add_operation(
                node_type=NodeType.PROXY_INPUT,
                node_params={"node_name": node.name},
                node_output_type=node.output_type,
                input_nodes=[],
            )
            proxy_input_nodes.append(proxy_input_node)

        nested_node = graph.add_operation(
            node_type=node_type,
            node_params=node_params,
            node_output_type=node_output_type,
            input_nodes=proxy_input_nodes,
        )
        graph_node = GraphNode(
            name="graph",
            output_type=nested_node.output_type,
            parameters=GraphNodeParameters(graph=graph, output_node_name=nested_node.name),
        )
        return graph_node, proxy_input_nodes

    @property
    def output_node(self) -> Node:
        """
        Output node of the graph (in the graph node)

        Returns
        -------
        Node
        """
        return cast(Node, self.parameters.graph.nodes_map[self.parameters.output_node_name])

    def add_operation(
        self,
        node_type: NodeType,
        node_params: Dict[str, Any],
        node_output_type: NodeOutputType,
        input_nodes: List[Node],
    ) -> Node:
        """
        Add operation to the query graph inside the graph node

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
            operation node of the given input (from the graph inside the graph node)
        """
        nested_node = self.parameters.graph.add_operation(
            node_type=node_type,
            node_params=node_params,
            node_output_type=node_output_type,
            input_nodes=input_nodes,
        )
        self.parameters.output_node_name = nested_node.name
        return cast(Node, nested_node)


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
