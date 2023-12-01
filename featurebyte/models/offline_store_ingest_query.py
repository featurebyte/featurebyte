"""
OfflineStoreIngestQuery object stores the offline store ingest query for a feature.
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from pydantic import validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.nested import AggregationNodeInfo
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer


class OfflineStoreIngestQueryGraph(FeatureByteBaseModel):
    """
    OfflineStoreIngestQuery object stores the offline store ingest query for a feature or target.
    """

    # offline store ingest query graph & output node name (from the graph)
    graph: QueryGraphModel
    node_name: str
    # primary entity ids of the offline store ingest query graph
    primary_entity_ids: List[PydanticObjectId]
    # reference node name that is used in decomposed query graph
    # if None, the query graph is not decomposed
    ref_node_name: Optional[str]
    # output column name of the offline store ingest query graph
    output_column_name: str
    output_dtype: DBVarType
    # feature job setting of the offline store ingest query graph
    feature_job_setting: Optional[FeatureJobSetting]
    # whether the offline store ingest query graph has time-to-live (TTL) component
    has_ttl: bool
    # aggregation nodes info of the offline store ingest query graph
    aggregation_nodes_info: List[AggregationNodeInfo]

    # pydantic validators
    _sort_ids_validator = validator("primary_entity_ids", allow_reuse=True)(
        construct_sort_validator()
    )

    @property
    def feast_feature_view_grouping_key(
        self,
    ) -> Tuple[Tuple[PydanticObjectId, ...], Optional[FeatureJobSetting], bool]:
        """
        Get feast feature view grouping key which is used to group offline store feature into the same feature view

        Returns
        -------
        Tuple[Tuple[PydanticObjectId, ...], Optional[FeatureJobSetting], bool]
            Feast feature view grouping key
        """
        return (
            tuple(self.primary_entity_ids),
            self.feature_job_setting,
            self.has_ttl,
        )

    def ingest_graph_and_node(self) -> Tuple[QueryGraphModel, Node]:
        """
        Construct graph and node for generating offline store ingest SQL query

        Returns
        -------
        Tuple[QueryGraphModel, Node]
            Ingest graph and node
        """
        output_node = self.graph.get_node_by_name(self.node_name)
        if self.ref_node_name is None:
            # if the query graph is not decomposed, return the original graph & node
            return self.graph, output_node

        # if the query graph is decomposed, update the graph output column name to match output_column_name
        if output_node.type != NodeType.ALIAS:
            graph = QueryGraphModel(**self.graph.dict(by_alias=True))
        else:
            output_parent_node_name = self.graph.backward_edges_map[self.node_name][0]
            transformer = QuickGraphStructurePruningTransformer(graph=self.graph)
            graph, node_name_map = transformer.transform(
                target_node_names=[output_parent_node_name]
            )
            output_node = graph.get_node_by_name(node_name_map[output_parent_node_name])

        # add alias node to rename the output column name
        output_node = graph.add_operation(
            node_type=NodeType.ALIAS,
            node_params={"name": self.output_column_name},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[output_node],
        )
        return graph, output_node
