"""
Implement graph data structure for query graph
"""

from collections import OrderedDict, defaultdict
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypedDict,
    Union,
    cast,
)

from bson import ObjectId
from pydantic import Field
from typing_extensions import Literal

from featurebyte.common.singleton import SingletonMeta
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.feature_job_setting import TableIdFeatureJobSetting
from featurebyte.query_graph.model.graph import Edge, GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import NodeT
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    TableIdCleaningOperation,
)
from featurebyte.query_graph.node.function import GenericFunctionNode
from featurebyte.query_graph.node.generic import (
    GroupByNode,
    LookupNode,
    LookupTargetNode,
    NonTileWindowAggregateNode,
    TimeSeriesWindowAggregateNode,
)
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.metadata.operation import (
    DerivedDataColumn,
    OperationStructure,
    OperationStructureInfo,
    SourceDataColumn,
)
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.query_graph.node.nested import BaseGraphNode, BaseViewGraphNodeParameters
from featurebyte.query_graph.transform.decompose_point import (
    DecomposePointExtractor,
    DecomposePointState,
)
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.transform.pruning import prune_query_graph
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer
from featurebyte.query_graph.transform.reconstruction import GraphReconstructionTransformer


class QueryGraph(QueryGraphModel):
    """
    Graph data structure
    """

    @classmethod
    def get_primary_input_nodes_from_graph_model(
        cls, graph: QueryGraphModel, node_name: str
    ) -> List[InputNode]:
        """
        Get primary input nodes from the query graph given the target node name

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph model to get primary input nodes from
        node_name: str
            Name of the node to get primary input nodes for

        Returns
        -------
        List[InputNode]
        """
        target_node = graph.get_node_by_name(node_name)
        operation_structure_info = OperationStructureExtractor(graph=graph).extract(
            node=target_node,
            keep_all_source_columns=True,
        )
        target_op_struct = operation_structure_info.operation_structure_map[node_name]
        node_name_to_input_node = OrderedDict()
        for column in target_op_struct.iterate_source_columns_or_aggregations():
            if graph.get_node_by_name(column.node_name).type == NodeType.REQUEST_COLUMN:
                continue
            # get_input_node performs a depth-first search to find the input node
            # during the search, it will traverse the left input node first before the right input node.
            # This is important because left input node is the main table for all existing join operations.
            input_node = graph.get_input_node(node_name=column.node_name)
            if input_node.name not in node_name_to_input_node:
                node_name_to_input_node[input_node.name] = input_node
        return list(node_name_to_input_node.values())

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
        return self.get_primary_input_nodes_from_graph_model(graph=self, node_name=node_name)

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

    def get_decompose_state(
        self,
        node_name: str,
        relationships_info: Optional[List[EntityRelationshipInfo]] = None,
        extract_primary_entity_ids_only: bool = False,
    ) -> DecomposePointState:
        """
        Get decompose state of the query graph given the target node name

        Parameters
        ----------
        node_name: str
            Name of the node to get decompose state for
        relationships_info: Optional[List[EntityRelationshipInfo]]
            Entity relationship info
        extract_primary_entity_ids_only: bool
            Whether to extract primary entity IDs only to speed up the extraction process

        Returns
        -------
        DecomposePointState
            Decompose state of the query graph
        """
        decompose_state = DecomposePointExtractor(graph=self).extract(
            node=self.get_node_by_name(node_name=node_name),
            relationships_info=relationships_info,
            extract_primary_entity_ids_only=extract_primary_entity_ids_only,
        )
        return decompose_state

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
        # not passing entity relationship to the extractor, primary entity will be the same as entity
        decompose_state = self.get_decompose_state(
            node_name=node_name, relationships_info=None, extract_primary_entity_ids_only=True
        )
        return sorted(decompose_state.primary_entity_ids)

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
            elif isinstance(node, (LookupNode, LookupTargetNode)):
                output.append(node.parameters.entity_column)
        return sorted(set(output))

    def iterate_group_by_node_and_table_id_pairs(
        self, target_node: Node
    ) -> Iterator[
        Tuple[
            Union[GroupByNode, NonTileWindowAggregateNode, TimeSeriesWindowAggregateNode],
            Optional[ObjectId],
        ]
    ]:
        """
        Iterate all GroupBy nodes and their corresponding Table ID

        Parameters
        ----------
        target_node: Node
            Node from which to start the backward search

        Yields
        ------
        Tuple[Union[GroupByNode, NonTileWindowAggregateNode, TimeSeriesWindowAggregateNode], Optional[ObjectId]]
            GroupBy node and its corresponding EventTable input node
        """
        operation_structure_info = OperationStructureExtractor(graph=self).extract(
            node=target_node,
            keep_all_source_columns=True,
        )

        def _iter_window_aggregate_nodes() -> Generator[Node, None, None]:
            for group_by_node in self.iterate_nodes(
                target_node=target_node, node_type=NodeType.GROUPBY
            ):
                yield group_by_node
            for non_tile_window_aggregate_node in self.iterate_nodes(
                target_node=target_node, node_type=NodeType.NON_TILE_WINDOW_AGGREGATE
            ):
                yield non_tile_window_aggregate_node
            for ts_window_aggregate_node in self.iterate_nodes(
                target_node=target_node, node_type=NodeType.TIME_SERIES_WINDOW_AGGREGATE
            ):
                yield ts_window_aggregate_node

        for node in _iter_window_aggregate_nodes():
            assert isinstance(
                node, (GroupByNode, NonTileWindowAggregateNode, TimeSeriesWindowAggregateNode)
            )
            agg_node_op_struct = operation_structure_info.operation_structure_map[node.name]
            timestamp_col = next(
                (
                    col
                    for col in agg_node_op_struct.columns
                    if col.name == node.parameters.timestamp
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
            yield node, table_id

    def extract_table_id_feature_job_settings(
        self, target_node: Node, keep_first_only: bool = False
    ) -> List[TableIdFeatureJobSetting]:
        """
        Extract table ID feature job settings of the query graph given the target node name

        Parameters
        ----------
        target_node: Node
            Node from which to start the backward search
        keep_first_only: bool
            Whether to keep the first table ID feature job setting only. When validating whether the
            feature graph have the consistent feature job settings (per table), we need to disable this
            to get all the table ID feature job settings in the query graph.

        Returns
        -------
        List[TableIdFeatureJobSetting]
            List of table ID feature job settings in the query graph
        """
        table_feature_job_settings = []
        node_table_id_iterator = self.iterate_group_by_node_and_table_id_pairs(
            target_node=target_node
        )
        table_ids = set()
        for group_by_node, table_id in node_table_id_iterator:
            if keep_first_only and table_id in table_ids:
                # skip duplicate table
                continue

            table_ids.add(table_id)
            table_feature_job_settings.append(
                TableIdFeatureJobSetting(
                    table_id=table_id,
                    feature_job_setting=group_by_node.parameters.feature_job_setting,
                )
            )
        return table_feature_job_settings

    def extract_table_id_cleaning_operations(
        self,
        target_node: Node,
        keep_all_columns: bool = True,
        table_id_to_col_names: Optional[Dict[ObjectId, Set[str]]] = None,
    ) -> List[TableIdCleaningOperation]:
        """
        Extract table ID cleaning operations from the query graph given the target node name

        Parameters
        ----------
        target_node: Node
            Node from which to start the backward search
        keep_all_columns: bool
            Whether to keep all columns in the table
        table_id_to_col_names: Optional[Dict[ObjectId, Set[str]]]
            Table ID to column names mapping. If not provided, it will be computed from the graph

        Returns
        -------
        List[TableIdCleaningOperation]
            List of table cleaning operations
        """
        table_column_operations = []
        found_table_ids = set()
        if table_id_to_col_names is None:
            table_id_to_col_names = self.extract_table_id_to_table_column_names(node=target_node)
        for node in self.iterate_nodes(target_node=target_node, node_type=NodeType.GRAPH):
            assert isinstance(node, BaseGraphNode)
            if node.parameters.type in GraphNodeType.view_graph_node_types():
                node_params = node.parameters
                assert isinstance(node_params, BaseViewGraphNodeParameters)
                if not keep_all_columns and not node_params.metadata.column_cleaning_operations:
                    continue

                col_to_clean_ops = {
                    col_clean_op.column_name: col_clean_op.cleaning_operations
                    for col_clean_op in node_params.metadata.column_cleaning_operations
                }
                table_column_operations.append(
                    TableIdCleaningOperation(
                        table_id=node_params.metadata.table_id,
                        column_cleaning_operations=[
                            ColumnCleaningOperation(
                                column_name=col_name,
                                cleaning_operations=col_to_clean_ops.get(col_name, []),
                            )
                            for col_name in sorted(
                                table_id_to_col_names[node_params.metadata.table_id]
                            )
                            if keep_all_columns or col_to_clean_ops.get(col_name)
                        ],
                    )
                )
                found_table_ids.add(node_params.metadata.table_id)

        if keep_all_columns:
            # prepare column name to column cleaning operations mapping
            for table_id, column_names in table_id_to_col_names.items():
                if table_id not in found_table_ids:
                    column_clean_ops = [
                        ColumnCleaningOperation(column_name=column_name, cleaning_operations=[])
                        for column_name in sorted(column_names)
                    ]
                    table_column_operations.append(
                        TableIdCleaningOperation(
                            table_id=table_id, column_cleaning_operations=column_clean_ops
                        )
                    )

        return table_column_operations

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
                for input_node_name in graph.backward_edges_map.get(node.name, [])
            ]
            node_global = self.add_operation_node(node=node, input_nodes=input_nodes)
            node_name_map[node.name] = node_global.name
        return self, node_name_map

    def extract_operation_structure_info(
        self,
        node: Node,
        keep_all_source_columns: bool = True,
        **kwargs: Any,
    ) -> OperationStructureInfo:
        """
        Extract operation structure info from the graph given target node

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
        OperationStructureInfo
        """
        op_struct_info = OperationStructureExtractor(graph=self).extract(
            node=node, keep_all_source_columns=keep_all_source_columns, **kwargs
        )
        return op_struct_info

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
        op_struct_info = self.extract_operation_structure_info(
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
                node_params=node.parameters.model_dump(),
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


def _create_global_graph_state() -> GraphState:
    return {
        "edges": [],
        "nodes": [],
        "nodes_map": {},
        "edges_map": {},
        "backward_edges_map": {},
        "node_type_counter": {},
        "node_name_to_ref": {},
        "ref_to_node_name": {},
    }


class GlobalGraphState(metaclass=SingletonMeta):
    """
    Global singleton to store query graph related attributes
    """

    _state: GraphState = _create_global_graph_state()

    @classmethod
    def reset(cls) -> None:
        """
        Reset the global query graph state to clean state
        """
        del cls._state
        cls._state = _create_global_graph_state()

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

    def clear(self) -> None:
        """
        Clear the global query graph
        """
        del self.nodes[:]
        del self.edges[:]
        self.nodes = []
        self.edges = []
        GlobalGraphState.reset()
        self._update_cache()
