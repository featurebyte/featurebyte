"""
Implement graph data structure for query graph
"""
from typing import (
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    TypedDict,
    cast,
)

from collections import defaultdict

from bson import ObjectId
from pydantic import Field

from featurebyte.common.singleton import SingletonMeta
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.graph import Edge, GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import NodeT
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    PostAggregationColumn,
    SourceDataColumn,
)
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.pruning import prune_query_graph
from featurebyte.query_graph.transform.reconstruction import GraphReconstructionTransformer


class QueryGraph(QueryGraphModel):
    """
    Graph data structure
    """

    def get_main_input_nodes(self, node_name: str) -> List[InputNode]:
        """
        Get the main input node of the query graph

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
        List[InputNode]
            Main InputNode objects
        """
        target_node = self.get_node_by_name(node_name)
        operation_structure_info = OperationStructureExtractor(graph=self).extract(
            node=target_node,
            keep_all_source_columns=True,
        )
        target_op_struct = operation_structure_info.operation_structure_map[node_name]
        main_input_node_names = set()
        if target_op_struct.output_category == NodeOutputCategory.VIEW:
            for column in target_op_struct.columns:
                if isinstance(column, SourceDataColumn):
                    main_input_node_names.add(self.get_input_node(node_name=column.node_name).name)
                else:
                    assert isinstance(column, DerivedDataColumn)
                    for source_col in column.columns:
                        main_input_node_names.add(
                            self.get_input_node(node_name=source_col.node_name).name
                        )
        else:
            for aggregation in target_op_struct.aggregations:
                if isinstance(aggregation, AggregationColumn):
                    main_input_node_names.add(
                        self.get_input_node(node_name=aggregation.node_name).name
                    )
                else:
                    assert isinstance(aggregation, PostAggregationColumn)
                    for source_agg in aggregation.columns:
                        main_input_node_names.add(
                            self.get_input_node(node_name=source_agg.node_name).name
                        )
        return [self.get_node_by_name(node_name) for node_name in sorted(main_input_node_names)]

    def iterate_group_by_node_and_table_id_pairs(
        self, target_node: Node
    ) -> Iterator[Tuple[GroupByNode, Optional[ObjectId]]]:
        """
        Iterate all GroupBy nodes and their corresponding Table ID

        Parameters
        ----------
        target_node: Node
            Node from which to start the backward search

        Yields
        ------
        Tuple[GroupByNode, Optional[ObjectId]]
            GroupBy node and its corresponding EventTable input node
        """
        operation_structure_info = OperationStructureExtractor(graph=self).extract(
            node=target_node,
            keep_all_source_columns=True,
        )
        for group_by_node in self.iterate_nodes(
            target_node=target_node, node_type=NodeType.GROUPBY
        ):
            assert isinstance(group_by_node, GroupByNode)
            group_by_op_struct = operation_structure_info.operation_structure_map[
                group_by_node.name
            ]
            timestamp_col = next(
                (
                    col
                    for col in group_by_op_struct.columns
                    if col.name == group_by_node.parameters.timestamp
                ),
                None,
            )
            assert timestamp_col is not None, "Timestamp column not found"
            if isinstance(timestamp_col, SourceDataColumn):
                table_id = timestamp_col.tabular_data_id
            else:
                # for the item view, the timestamp column is from the event table, not the item table
                # therefore the column is a DerivedDataColumn
                assert isinstance(timestamp_col, DerivedDataColumn)
                table_id = None
                for column in timestamp_col.columns:
                    table_id = column.tabular_data_id

            assert table_id is not None, "Table ID not found"
            yield group_by_node, table_id

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

    def prune(self, target_node: Node, aggressive: bool) -> GraphNodeNameMap:
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
        pruned_graph, node_name_map, _ = prune_query_graph(
            graph=self, node=target_node, aggressive=aggressive
        )
        return pruned_graph, node_name_map

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

    def flatten(self) -> GraphNodeNameMap:
        """
        Construct a query graph which flattened all the graph nodes of this query graph

        Returns
        -------
        QueryGraphModel
        """
        return GraphFlatteningTransformer(graph=self).transform()

    def add_node(self, node: NodeT, input_nodes: List[Node]) -> NodeT:
        """
        Add graph node to the graph

        Parameters
        ----------
        node: NodeT
            Node to be inserted into the graph
        input_nodes: List[Node]
            Input nodes to the graph node

        Returns
        -------
        NodeT
        """
        if isinstance(node, GraphNode):
            node_output_type = node.output_node.output_type
        else:
            node_output_type = node.output_type

        return cast(
            NodeT,
            self.add_operation(
                node_type=node.type,
                node_params=node.parameters.dict(),
                node_output_type=node_output_type,
                input_nodes=input_nodes,
            ),
        )


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
    edges_map: DefaultDict[str, List[str]] = Field(
        default_factory=GlobalGraphState.construct_getter_func("edges_map"), exclude=True
    )
    backward_edges_map: DefaultDict[str, List[str]] = Field(
        default_factory=GlobalGraphState.construct_getter_func("backward_edges_map"), exclude=True
    )
    node_type_counter: DefaultDict[str, int] = Field(
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
