"""
This module contains
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityAncestorDescendantMapper,
    EntityRelationshipInfo,
)
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    GroupByNode,
    LookupNode,
    LookupTargetNode,
    NonTileWindowAggregateNode,
    TimeSeriesWindowAggregateNode,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.mixin import AggregationOpStructMixin, BaseGroupbyParameters
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.query_graph.ttl_handling_util import is_ttl_handling_required


@dataclass
class AggregationInfo:
    """
    AggregationInfo class stores information about the aggregation-type node. Aggregation-type node
    includes
    - GroupByNode (time-to-live)
    - ItemGroupbyNode (non-time-to-live)
    - AggregateAsAtNode (non-time-to-live)
    - LookupNode (SCD lookup, Event lookup & Dimension lookup) (non-time-to-live)
    - TimeSeriesWindowAggregateNode (time series aggregation)
    - RequestColumnNode
    """

    agg_node_types: List[NodeType]
    primary_entity_ids: List[PydanticObjectId]
    primary_entity_dtypes: List[DBVarType]
    feature_job_settings: List[FeatureJobSettingUnion]
    has_request_column: bool
    has_ingest_graph_node: bool
    has_ttl_agg_type: bool
    extract_primary_entity_ids_only: bool

    def __init__(self, extract_primary_entity_ids_only: bool) -> None:
        self.agg_node_types = []
        self.primary_entity_ids = []
        self.primary_entity_dtypes = []
        self.feature_job_settings = []
        self.has_request_column = False
        self.has_ingest_graph_node = False
        self.has_ttl_agg_type = False
        self.extract_primary_entity_ids_only = extract_primary_entity_ids_only

    @property
    def primary_entity_id_to_dtype_map(self) -> Dict[PydanticObjectId, DBVarType]:
        """
        Get primary entity id to dtype mapping

        Returns
        -------
        Dict[PydanticObjectId, DBVarType]
        """
        return dict(zip(self.primary_entity_ids, self.primary_entity_dtypes))

    def __add__(self, other: "AggregationInfo") -> "AggregationInfo":
        """
        Add two AggregationInfo objects

        Parameters
        ----------
        other: AggregationInfo
            Other AggregationInfo object

        Returns
        -------
        AggregationInfo
            Added AggregationInfo object
        """
        primary_entity_id_to_dtype_map = self.primary_entity_id_to_dtype_map.copy()
        primary_entity_id_to_dtype_map.update(other.primary_entity_id_to_dtype_map)

        output = AggregationInfo(self.extract_primary_entity_ids_only)
        output.primary_entity_ids = sorted(set(self.primary_entity_ids + other.primary_entity_ids))
        if not self.extract_primary_entity_ids_only:
            output.agg_node_types = sorted(set(self.agg_node_types + other.agg_node_types))
            output.primary_entity_dtypes = [
                primary_entity_id_to_dtype_map[entity_id] for entity_id in output.primary_entity_ids
            ]
            output.feature_job_settings = list(
                set(self.feature_job_settings + other.feature_job_settings)
            )
            output.has_request_column = self.has_request_column or other.has_request_column
            output.has_ingest_graph_node = self.has_ingest_graph_node or other.has_ingest_graph_node
            output.has_ttl_agg_type = self.has_ttl_agg_type or other.has_ttl_agg_type
        return output


class FeatureJobSettingExtractor:
    """
    FeatureJobSettingExtractor class is used to extract feature job setting for the given node
    """

    def __init__(self, graph: QueryGraphModel) -> None:
        self.graph = graph

    @property
    def default_feature_job_setting(self) -> FeatureJobSetting:
        """
        Get default feature job setting

        Returns
        -------
        FeatureJobSetting
        """
        return FeatureJobSetting(period="1d", offset="0s", blind_spot="0s")

    def _extract_lookup_node_feature_job_setting(
        self, node: LookupNode
    ) -> Optional[FeatureJobSetting]:
        input_node = self.graph.get_input_node(node_name=node.name)
        if input_node.parameters.type == TableDataType.DIMENSION_TABLE:
            return None
        return self.default_feature_job_setting

    def extract_from_agg_node(self, node: Node) -> Optional[FeatureJobSettingUnion]:
        """
        Extract feature job setting from the given node, return None if the node is not an aggregation node

        Parameters
        ----------
        node: Node
            Node to be processed

        Returns
        -------
        Optional[FeatureJobSettingUnion]
        """
        if isinstance(node, TimeSeriesWindowAggregateNode):
            return node.parameters.feature_job_setting

        if not isinstance(node, AggregationOpStructMixin):
            return None

        if isinstance(node, (GroupByNode, NonTileWindowAggregateNode)):
            return node.parameters.feature_job_setting

        if isinstance(node, LookupNode):
            return self._extract_lookup_node_feature_job_setting(node=node)

        return self.default_feature_job_setting

    def extract_from_target_node(self, node: Node) -> Optional[FeatureJobSettingUnion]:
        """
        Extract feature job setting from the given target node. DFS is used to find the first
        aggregation node in the lineage of the target node.

        Parameters
        ----------
        node: Node
            Node to be processed

        Returns
        -------
        Optional[FeatureJobSettingUnion]
        """
        for _node in self.graph.iterate_nodes(target_node=node, node_type=None):
            if isinstance(_node, (AggregationOpStructMixin, TimeSeriesWindowAggregateNode)):
                return self.extract_from_agg_node(node=_node)
        return None


@dataclass
class DecomposePointState:
    """DecomposePointState class stores global state of the decompose point extractor"""

    # entity id to ancestor/descendant mapping
    entity_ancestor_descendant_mapper: EntityAncestorDescendantMapper
    # (original graph) node name to aggregation node info mapping (from the original graph)
    node_name_to_aggregation_info: Dict[str, AggregationInfo]
    # (original graph) aggregation node names used to determine whether to start decomposing the graph
    aggregation_node_names: Set[str]
    # (original graph) node names that should be used as decompose point
    decompose_node_names: Set[str]
    # (original graph) node names that should be used as offline store ingest query graph output node
    ingest_graph_output_node_names: Set[str]
    # operation structure info
    operation_structure_map: Dict[str, OperationStructure]
    # extract primary entity ids only
    extract_primary_entity_ids_only: bool

    @property
    def should_decompose(self) -> bool:
        """
        Check whether the graph should be decomposed

        Returns
        -------
        bool
        """
        return bool(self.decompose_node_names)

    @property
    def primary_entity_ids(self) -> List[PydanticObjectId]:
        """
        Get entity ids

        Returns
        -------
        List[ObjectId]
        """
        entity_ids = set()
        for agg_info in self.node_name_to_aggregation_info.values():
            entity_ids.update(agg_info.primary_entity_ids)
        return sorted(entity_ids)

    @property
    def primary_entity_ids_to_dtypes_map(self) -> Dict[PydanticObjectId, DBVarType]:
        """
        Get entity ids to dtypes mapping

        Returns
        -------
        Dict[ObjectId, DBVarType]
        """
        entity_ids_to_dtypes_map = {}
        for agg_info in self.node_name_to_aggregation_info.values():
            entity_ids_to_dtypes_map.update(agg_info.primary_entity_id_to_dtype_map)
        return entity_ids_to_dtypes_map

    @classmethod
    def create(
        cls,
        relationships_info: List[EntityRelationshipInfo],
        aggregation_node_names: Set[str],
        operation_structure_map: Dict[str, OperationStructure],
        extract_primary_entity_ids_only: bool,
    ) -> "DecomposePointState":
        """
        Create DecomposePointState object

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            List of entity relationship info
        aggregation_node_names: Set[str]
            Set of aggregation node names
        operation_structure_map: Dict[str, OperationStructure]
            Operation structure map
        extract_primary_entity_ids_only: bool
            Extract primary entity ids only without extracting other information to speed up the process

        Returns
        -------
        DecomposePointState
        """
        entity_ancestor_descendant_mapper = EntityAncestorDescendantMapper.create(
            relationships_info=relationships_info
        )
        return cls(
            entity_ancestor_descendant_mapper=entity_ancestor_descendant_mapper,
            node_name_to_aggregation_info={},
            aggregation_node_names=aggregation_node_names,
            decompose_node_names=set(),
            ingest_graph_output_node_names=set(),
            operation_structure_map=operation_structure_map,
            extract_primary_entity_ids_only=extract_primary_entity_ids_only,
        )

    def _update_more_aggregation_info(
        self,
        query_graph: QueryGraphModel,
        node: Node,
        aggregation_info: AggregationInfo,
    ) -> AggregationInfo:
        op_struct = self.operation_structure_map[node.name]

        if node.name in self.aggregation_node_names:
            aggregation_info.agg_node_types = [node.type]
            aggregation_info.has_ttl_agg_type = is_ttl_handling_required(node=node)

        if isinstance(node.parameters, BaseGroupbyParameters):
            groupby_keys = node.parameters.keys
            # check the dtype of the source columns that match the groupby keys to get dtype
            # of the primary entity ids
            colname_to_dtype_map = {}
            for source_column in op_struct.columns:
                if source_column.name in groupby_keys:
                    colname_to_dtype_map[source_column.name] = source_column.dtype
            aggregation_info.primary_entity_dtypes = [
                colname_to_dtype_map[key] for key in groupby_keys
            ]
            assert len(aggregation_info.primary_entity_dtypes) == len(
                aggregation_info.primary_entity_ids
            ), "Primary entity dtype not matches"

        elif isinstance(node, (LookupNode, LookupTargetNode)):
            # primary entity ids introduced by lookup node family
            for source_column in op_struct.columns:
                if source_column.name == node.parameters.entity_column:
                    aggregation_info.primary_entity_dtypes = [source_column.dtype]
                    break
            assert (
                len(aggregation_info.primary_entity_dtypes) == 1
            ), "Primary entity dtype not found"

        if isinstance(node, RequestColumnNode):
            # request columns introduced by request column node
            aggregation_info.has_request_column = True

        feature_job_setting = FeatureJobSettingExtractor(graph=query_graph).extract_from_agg_node(
            node=node
        )
        if feature_job_setting:
            aggregation_info.feature_job_settings = [feature_job_setting]
        return aggregation_info

    def update_aggregation_info(
        self,
        query_graph: QueryGraphModel,
        node: Node,
        input_node_names: List[str],
        extract_primary_entity_ids_only: bool,
    ) -> None:
        """
        Update aggregation info of the given node

        Parameters
        ----------
        query_graph: QueryGraphModel
            Query graph
        node: Node
            Node to be processed
        input_node_names: List[str]
            List of input node names
        extract_primary_entity_ids_only: bool
            Extract primary entity ids only without extracting other information to speed up the process
        """
        aggregation_info = AggregationInfo(extract_primary_entity_ids_only)
        for input_node_name in input_node_names:
            input_aggregation_info = self.node_name_to_aggregation_info.get(input_node_name)
            if input_aggregation_info:
                # if current node is groupby node, the input_aggregation_info should be empty
                # as they are skipped in the pre-compute step
                aggregation_info += input_aggregation_info

        if isinstance(node.parameters, BaseGroupbyParameters):
            # primary entity ids introduced by groupby node family
            aggregation_info.primary_entity_ids = node.parameters.entity_ids or []
        elif isinstance(node, (LookupNode, LookupTargetNode)):
            # primary entity ids introduced by lookup node family
            aggregation_info.primary_entity_ids = [node.parameters.entity_id]

        if not extract_primary_entity_ids_only:
            aggregation_info = self._update_more_aggregation_info(
                query_graph=query_graph, node=node, aggregation_info=aggregation_info
            )

        # reduce the primary entity ids based on entity relationship
        aggregation_info.primary_entity_ids = (
            self.entity_ancestor_descendant_mapper.reduce_entity_ids(
                entity_ids=aggregation_info.primary_entity_ids
            )
        )

        # update the mapping
        self.node_name_to_aggregation_info[node.name] = aggregation_info

    def check_input_aggregations(
        self, agg_info: AggregationInfo, input_node_names: List[str]
    ) -> bool:
        """
        Check whether to split the input nodes into multiple offline store ingest query graphs

        Parameters
        ----------
        agg_info: AggregationInfo
            Aggregation info of the current node
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        bool
        """
        # check whether the input nodes have mixed request column flags or mixed ingest graph node flags
        input_aggregations_info = [
            self.node_name_to_aggregation_info[input_node_name]
            for input_node_name in input_node_names
            # this condition is used to handle inputs of the groupby node (which are skipped)
            if input_node_name in self.node_name_to_aggregation_info
        ]

        # Check for conditions where splitting should occur
        split_conditions = []
        for input_agg_info in input_aggregations_info:
            split_conditions.append(
                input_agg_info.has_request_column or input_agg_info.has_ingest_graph_node
            )

        # Check if any of the inputs have either request column or ingest graph node, split it
        if any(split_conditions):
            # If all inputs have either request columns or ingest graph nodes, do not split
            if all(split_conditions):
                return False
            return True

        # check whether the input nodes can be merged into one offline store ingest query graph
        all_inputs_have_empty_agg_node_types = True
        for input_agg_info in input_aggregations_info:
            if (
                input_agg_info.primary_entity_ids == agg_info.primary_entity_ids
                and input_agg_info.feature_job_settings == agg_info.feature_job_settings
                and input_agg_info.has_ttl_agg_type == agg_info.has_ttl_agg_type
            ):
                # if any of the input is the same as the output, that means
                # - no new entity ids are added
                # - no new feature job settings are added
                # - no new time-to-live aggregation type is added
                # to the universe.
                return False

            if input_agg_info.agg_node_types:
                all_inputs_have_empty_agg_node_types = False

        if all_inputs_have_empty_agg_node_types:
            # if all the input nodes do not have any aggregation operation introduced, that means
            # the current node is the first aggregation operation introduced in the graph (between
            # the input nodes and the current node).
            return False

        # if none of the above conditions are met, that means we should split the query graph
        return True

    def should_decompose_query_graph(self, node_name: str, input_node_names: List[str]) -> bool:
        """
        Check whether to decompose the query graph into graph with nested offline store ingest query nodes

        Parameters
        ----------
        node_name: str
            Node name
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        bool
        """
        agg_info = self.node_name_to_aggregation_info[node_name]
        if not agg_info.agg_node_types:
            # do not decompose if aggregation operation has not been introduced
            return False

        return self.check_input_aggregations(agg_info=agg_info, input_node_names=input_node_names)

    def update_ingest_graph_node_output_names(self, input_node_names: List[str]) -> None:
        """
        Check the list of input node names to determine whether the input should be an offline store ingest
        query graph. If so, add the input node name to the list of ingest graph output node names.

        Parameters
        ----------
        input_node_names: List[str]
            List of input node names
        """
        input_aggregations_info = [
            self.node_name_to_aggregation_info[input_node_name]
            for input_node_name in input_node_names
        ]
        any_input_has_request_column = False
        any_input_has_ingest_graph_node = False
        for input_agg_info in input_aggregations_info:
            any_input_has_request_column |= input_agg_info.has_request_column
            any_input_has_ingest_graph_node |= input_agg_info.has_ingest_graph_node

        for input_node_name, input_agg_info in zip(input_node_names, input_aggregations_info):
            if input_agg_info.has_request_column:
                # if the input node has request column, it must not be an offline store ingest query graph
                continue

            if input_agg_info.has_ingest_graph_node:
                # if the input node has ingest graph node, it must not be an offline store ingest query graph
                continue

            self.ingest_graph_output_node_names.add(input_node_name)
            self.node_name_to_aggregation_info[input_node_name].has_ingest_graph_node = True

        if not self.ingest_graph_output_node_names.intersection(input_node_names):
            assert False, "No offline store ingest query graph output node found"


class DecomposePointExtractor(
    BaseGraphExtractor[DecomposePointState, FeatureByteBaseModel, DecomposePointState]
):
    """
    DecomposePointExtractor class is used to extract information about the decompose point
    used in offline ingest graph decomposition.
    """

    def _pre_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: DecomposePointState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        skip_input_nodes = False
        if isinstance(node.parameters, BaseGroupbyParameters):
            # if groupby node has entity_ids, skip further exploration on input nodes
            skip_input_nodes = True
        return [] if skip_input_nodes else input_node_names, False

    def _in_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: DecomposePointState,
        node: Node,
        input_node: Node,
    ) -> FeatureByteBaseModel:
        return branch_state

    def _post_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: DecomposePointState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> DecomposePointState:
        input_node_names = self.graph.get_input_node_names(node)
        global_state.update_aggregation_info(
            query_graph=self.graph,
            node=node,
            input_node_names=input_node_names,
            extract_primary_entity_ids_only=global_state.extract_primary_entity_ids_only,
        )
        if not global_state.extract_primary_entity_ids_only:
            # skip decompose point extraction if the graph is only used to extract primary entity ids
            # to speed up the process
            to_decompose = global_state.should_decompose_query_graph(
                node_name=node.name, input_node_names=input_node_names
            )
            if to_decompose:
                global_state.decompose_node_names.add(node.name)
                global_state.update_ingest_graph_node_output_names(
                    input_node_names=input_node_names
                )
                global_state.node_name_to_aggregation_info[node.name].has_ingest_graph_node = True
        return global_state

    def extract(
        self,
        node: Node,
        relationships_info: Optional[List[EntityRelationshipInfo]] = None,
        extract_primary_entity_ids_only: bool = False,
        **kwargs: Any,
    ) -> DecomposePointState:
        aggregation_node_names = set()
        operation_structure_map = {}
        if not extract_primary_entity_ids_only:
            # identify aggregation node names in the graph, aggregation node names are used to determine
            # whether to start decomposing the graph
            op_struct_state = OperationStructureExtractor(graph=self.graph).extract(node=node)
            op_struct = op_struct_state.operation_structure_map[node.name]
            aggregation_node_names = {agg.node_name for agg in op_struct.iterate_aggregations()}
            operation_structure_map = op_struct_state.operation_structure_map

        global_state = DecomposePointState.create(
            relationships_info=relationships_info or [],
            aggregation_node_names=aggregation_node_names,
            operation_structure_map=operation_structure_map,
            extract_primary_entity_ids_only=extract_primary_entity_ids_only,
        )
        self._extract(
            node=node,
            branch_state=FeatureByteBaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state
