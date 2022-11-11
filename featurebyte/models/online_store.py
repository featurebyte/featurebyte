"""
This module contains Tile related models
"""
from typing import List, cast

from featurebyte.enum import TableDataType
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.generic import InputNode


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
    feature_sql: str
    feature_store_table_name: str

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
    def entity_column_names(self) -> List[str]:
        """
        derived entity_column_names property from tile_specs

        Returns
        -------
            derived entity_column_names
        """
        entity_column_names_set = set()
        for tile_spec in self.feature.tile_specs:
            entity_column_names_set.update(tile_spec.entity_column_names)
        return sorted(list(entity_column_names_set))

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
                output.append(input_node2.parameters.id)

        return output
