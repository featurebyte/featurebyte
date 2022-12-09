"""
Implement graph data structure for query graph
"""
from typing import Any, Callable, Dict, List, Literal, Tuple, TypedDict, cast

from collections import defaultdict

from pydantic import Field

from featurebyte.common.singleton import SingletonMeta
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model import Edge, GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.nested import BaseGraphNode, GraphNodeParameters
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.pruning import GraphPruningExtractor
from featurebyte.query_graph.transform.reconstruction import GraphReconstructionTransformer


class QueryGraph(QueryGraphModel):
    """
    Graph data structure
    """

    def load(self, graph: QueryGraphModel) -> Tuple["QueryGraph", Dict[str, str]]:
        """
        Load the query graph into the query graph

        Parameters
        ----------
        graph: QueryGraphModel
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
        op_struct_info = OperationStructureExtractor(graph=self).extract(node=node)
        return op_struct_info.operation_structure_map[node.name]

    def prune(self, target_node: Node, aggressive: bool = False) -> GraphNodeNameMap:
        """
        Prune the query graph and return the pruned graph & mapped node.

        To prune the graph, this function first traverses from the target node to the input node.
        The unused branches of the graph will get pruned in this step. After that, a new graph is
        reconstructed by adding the required nodes back.

        Parameters
        ----------
        target_node: Node
            Target end node
        aggressive: bool
            Flag to enable aggressive mode. When the `aggressive` is True, those prunable nodes
            may get removed if they do not contribute to the target node output. In addition,
            all the node parameters will get pruned based on the output of the node. When the
            `aggressive` is False, a graph traversal from the target node to the input node
            is performed, all the traversed nodes will be kept without any modification on the
            node parameters.

        Returns
        -------
        GraphNodeNameMap
            Tuple of pruned graph & its node name mapping
        """
        return GraphPruningExtractor(graph=self).extract(node=target_node, aggressive=aggressive)

    def reconstruct(
        self, node_name_to_replacement_node: Dict[str, Node], regenerate_groupby_hash: bool
    ) -> GraphNodeNameMap:
        """
        Reconstruct the query graph using the replacement node mapping

        Parameters
        ----------
        node_name_to_replacement_node: Dict[str, Node]
            Node name (of the input query graph) to replacement node mapping
        regenerate_groupby_hash: bool
            Whether to regenerate tile ID & aggregation ID in groupby node

        Returns
        -------
        GraphNodeNameMap
        """
        return GraphReconstructionTransformer(graph=self).transform(
            node_name_to_replacement_node=node_name_to_replacement_node,
            regenerate_groupby_hash=regenerate_groupby_hash,
        )

    def flatten(self) -> QueryGraphModel:
        """
        Construct a query graph which flattened all the graph nodes of this query graph

        Returns
        -------
        QueryGraphModel
        """
        return GraphFlatteningTransformer(graph=self).transform()

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

    def prepare_to_store(self, target_node: Node) -> Tuple[QueryGraphModel, str]:
        """
        Prepare the graph before getting stored at persistent

        Parameters
        ----------
        target_node: Node
            Target node

        Returns
        -------
        Tuple[QueryGraphModel, str]
            Aggressively pruned graph (with regenerated hash) & node name
        """
        pruned_graph, pruned_node_name_map = self.prune(target_node=target_node, aggressive=True)
        reconstructed_graph, recon_node_name_map = QueryGraph(**pruned_graph.dict()).reconstruct(
            node_name_to_replacement_node={},
            regenerate_groupby_hash=True,
        )
        return reconstructed_graph, recon_node_name_map[pruned_node_name_map[target_node.name]]


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
