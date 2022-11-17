"""
This module contains Tile related models
"""
from typing import List, cast

from featurebyte.enum import TableDataType
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.generic import InputNode
from featurebyte.query_graph.sql.online_serving import (
    get_entities_ids_and_serving_names,
    get_online_store_feature_compute_sql,
    get_online_store_table_name_from_graph,
)


class OnlineFeatureSpec(FeatureByteBaseModel):
    """
    Model for Online Feature Store

    feature: ExtendedFeatureModel
        feature model
    feature_sql: str
        feature sql
    feature_store_table_name: int
        online feature store table name
    tile_ids: List[str]
        derived tile_ids from tile_specs
    entity_column_names: List[str]
        derived entity column names from tile_specs
    """

    feature: ExtendedFeatureModel

    @property
    def tile_ids(self) -> List[str]:
        """
        derived tile_ids property from tile_specs

        Returns
        -------
            derived tile_ids
        """
        tile_ids_set = set()
        for tile_spec in self.feature.tile_specs:
            tile_ids_set.add(tile_spec.tile_id)
        return list(tile_ids_set)

    @property
    def serving_names(self) -> List[str]:
        """
        Derived serving names from the query graph. This will be the join keys in the store table

        Returns
        -------
        List[str]
        """
        _, serving_names = get_entities_ids_and_serving_names(self.feature.graph, self.feature.node)
        return sorted(serving_names)

    @property
    def event_data_ids(self) -> List[str]:
        """
        derived event_data_ids from graph

        Returns
        -------
            derived event_data_ids
        """
        output = []
        for input_node in self.feature.graph.iterate_nodes(
            target_node=self.feature.node, node_type=NodeType.INPUT
        ):
            input_node2 = cast(InputNode, input_node)
            if input_node2.parameters.type == TableDataType.EVENT_DATA:
                e_id = input_node2.parameters.id
                if e_id:
                    output.append(str(e_id))

        return output

    @property
    def feature_sql(self) -> str:
        """
        Feature pre-computation SQL for online store

        Returns
        -------
        str
        """
        return get_online_store_feature_compute_sql(
            graph=self.feature.graph,
            node=self.feature.node,
            source_type=self.feature.feature_store.type,
        )

    @property
    def feature_store_table_name(self) -> str:
        """
        Name of the online store table for the feature

        Returns
        -------
        str
        """
        return get_online_store_table_name_from_graph(self.feature.graph, self.feature.node)
