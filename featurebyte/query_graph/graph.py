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
    Set,
    Tuple,
    TypedDict,
    cast,
)

from collections import OrderedDict, defaultdict

from bson import ObjectId
from pydantic import Field

from featurebyte.common.singleton import SingletonMeta
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.graph import Edge, GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import NodeT
from featurebyte.query_graph.node.function import GenericFunctionNode
from featurebyte.query_graph.node.generic import ForwardAggregateNode, GroupByNode, LookupNode
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.metadata.operation import (
    DerivedDataColumn,
    OperationStructure,
    SourceDataColumn,
)
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.query_graph.transform.entity_extractor import EntityExtractor
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.pruning import prune_query_graph
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer
from featurebyte.query_graph.transform.reconstruction import GraphReconstructionTransformer


class QueryGraph(QueryGraphModel):
    """
    Graph data structure
    """

    def get_primary_input_nodes(self, node_name: str) -> List[InputNode]:
        """
        Get the primary input node of the query graph

        Parameters
        ----------
        node_name: str
            Name of node to get input node for

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
        node_name_to_input_node = OrderedDict()
        for column in target_op_struct.iterate_source_columns_or_aggregations():
            if self.get_node_by_name(column.node_name).type == NodeType.REQUEST_COLUMN:
                continue
            # get_input_node performs a depth-first search to find the input node
            # during the search, it will traverse the left input node first before the right input node.
            # This is important because left input node is the main table for all existing join operations.
            input_node = self.get_input_node(node_name=column.node_name)
            if input_node.name not in node_name_to_input_node:
                node_name_to_input_node[input_node.name] = input_node
        return list(node_name_to_input_node.values())

    def get_table_ids(self, node_name: str) -> List[ObjectId]:
        """
        Get table IDs of the query graph given the target node name

        Parameters
        ----------
        node_name: str
            Name of the node to get table IDs for

        Returns
        -------
        List[ObjectId]
            List of table IDs in the query graph
        """
        output = []
        target_node = self.get_node_by_name(node_name)
        for node in self.iterate_nodes(target_node=target_node, node_type=NodeType.INPUT):
            assert isinstance(node, InputNode)
            if node.parameters.id:
                output.append(node.parameters.id)
        return sorted(set(output))

    def get_primary_table_ids(self, node_name: str) -> List[ObjectId]:
        """
        Get primary table IDs of the query graph given the target node name

        Parameters
        ----------
        node_name: str
            Name of the node to get primary table IDs for

        Returns
        -------
        List[ObjectId]
            List of primary table IDs in the query graph
        """
        primary_input_nodes = self.get_primary_input_nodes(node_name=node_name)
        return sorted(set(node.parameters.id for node in primary_input_nodes if node.parameters.id))

    def get_entity_ids(self, node_name: str) -> List[ObjectId]:
        """
        Get entity IDs of the query graph given the target node name

        Parameters
        ----------
        node_name: str
            Name of the node to get entity IDs for

        Returns
        -------
        List[ObjectId]
            List of entity IDs in the query graph
        """
        entity_state = EntityExtractor(graph=self).extract(
            node=self.get_node_by_name(node_name=node_name)
        )
        return sorted(entity_state.entity_ids)

    def get_user_defined_function_ids(self, node_name: str) -> List[ObjectId]:
        """
        Get user defined function IDs of the query graph given the target node name

        Parameters
        ----------
        node_name: str
            Name of the node to get user defined function IDs for

        Returns
        -------
        List[ObjectId]
            List of user defined function IDs in the query graph
        """
        output = []
        target_node = self.get_node_by_name(node_name)
        for node in self.iterate_nodes(
            target_node=target_node, node_type=NodeType.GENERIC_FUNCTION
        ):
            assert isinstance(node, GenericFunctionNode)
            output.append(node.parameters.function_id)
        return sorted(set(output))

    def get_forward_aggregate_window(self, node_name: str) -> Optional[str]:
        """
        Get the window of the forward aggregate node

        Parameters
        ----------
        node_name: str
            Name of the node to get the window for

        Returns
        -------
        Optional[str]
            window of the forward aggregate node
        """
        target_node = self.get_node_by_name(node_name)
        for node in self.iterate_nodes(
            target_node=target_node, node_type=NodeType.FORWARD_AGGREGATE
        ):
            assert isinstance(node, ForwardAggregateNode)
            return node.parameters.window
        return None

    def get_entity_columns(self, node_name: str) -> List[str]:
        """
        Get entity columns of the query graph given the target node name

        Parameters
        ----------
        node_name: str
            Name of the node to get entity columns for

        Returns
        -------
        List[str]
            List of entity columns in the query graph
        """
        output = []
        target_node = self.get_node_by_name(node_name)
        for node in self.iterate_nodes(target_node=target_node, node_type=None):
            if isinstance(node.parameters, BaseGroupbyParameters):
                if node.parameters.entity_ids:
                    output.extend(node.parameters.keys)
            elif isinstance(node, LookupNode):
                output.append(node.parameters.entity_column)
        return sorted(set(output))

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
                table_id = timestamp_col.table_id
            else:
                # for the item view, the timestamp column is from the event table, not the item table
                # therefore the column is a DerivedDataColumn
                assert isinstance(timestamp_col, DerivedDataColumn)
                # use the last column's table ID as timestamp column's table ID
                table_id = timestamp_col.columns[-1].table_id

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
            node_global = self.add_operation_node(node=node, input_nodes=input_nodes)
            node_name_map[node.name] = node_global.name
        return self, node_name_map

    def extract_operation_structure(
        self,
        node: Node,
        keep_all_source_columns: bool = True,
        **kwargs: Any,
    ) -> OperationStructure:
        """
        Extract operation structure from the graph given target node

        Parameters
        ----------
        node: Node
            Target node used to construct the operation structure
        keep_all_source_columns: bool
            Whether to keep all source columns in the operation structure
        kwargs: Any
            Additional arguments to be passed to the OperationStructureExtractor.extract() method

        Returns
        -------
        OperationStructure
        """
        op_struct_info = OperationStructureExtractor(graph=self).extract(
            node=node, keep_all_source_columns=keep_all_source_columns, **kwargs
        )
        return op_struct_info.operation_structure_map[node.name]

    def extract_table_id_to_table_column_names(self, node: Node) -> Dict[ObjectId, Set[str]]:
        """
        Extract table ID to table column names based on the graph & given target node.
        This mapping is used to prune the view graph node parameters or extract the table columns used by the
        query graph object.

        Parameters
        ----------
        node: Node
            Target node used to the mapping

        Returns
        -------
        dict[ObjectId, set[str]]
            Table ID to table column names
        """
        operation_structure = self.extract_operation_structure(node, keep_all_source_columns=True)
        # prepare table ID to source column names mapping, use this mapping to prune the view graph node parameters
        table_id_to_source_column_names: Dict[ObjectId, Set[str]] = defaultdict(set)
        for src_col in operation_structure.iterate_source_columns():
            assert src_col.table_id is not None, "Source table ID is missing."
            table_id_to_source_column_names[src_col.table_id].add(src_col.name)
        return table_id_to_source_column_names

    def prune(self, target_node: Node) -> GraphNodeNameMap:
        """
        Prune the query graph and return the pruned graph & mapped node.

        To prune the graph, this function first traverses from the target node to the input node.
        The unused branches of the graph will get pruned in this step. After that, a new graph is
        reconstructed by adding the required nodes back.

        Parameters
        ----------
        target_node: Node
            Target end node

        Returns
        -------
        GraphNodeNameMap
            Tuple of pruned graph & its node name mapping
        """
        pruned_graph, node_name_map, _ = prune_query_graph(graph=self, node=target_node)
        return pruned_graph, node_name_map

    def quick_prune(self, target_node_names: List[str]) -> GraphNodeNameMap:
        """
        Quick prune the query graph and return the pruned graph & mapped node.

        The main difference between `quick_prune` and `prune` is that `quick_prune` does not change existing
        node parameters. The generated graph's node parameters are the same as the input graph. It is less
        expensive than `prune` as it does not perform any operation structure extraction.

        Parameters
        ----------
        target_node_names: List[str]
            Target end node names

        Returns
        -------
        GraphNodeNameMap
        """
        return QuickGraphStructurePruningTransformer(graph=self).transform(
            target_node_names=target_node_names
        )

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
