"""
Base Feature Or Target table model
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node


class BaseFeatureOrTargetTableModel(MaterializedTableModel):
    """
    Base Feature Or Target table model for shared properties
    """

    observation_table_id: Optional[PydanticObjectId]


class FeatureCluster(FeatureByteBaseModel):
    """
    Schema for a group of features from the same feature store
    """

    feature_store_id: PydanticObjectId
    graph: QueryGraph
    node_names: List[StrictStr]

    @property
    def nodes(self) -> List[Node]:
        """
        Get feature nodes

        Returns
        -------
        List[Node]
        """
        return [self.graph.get_node_by_name(name) for name in self.node_names]

    def get_nodes_for_feature_names(self, feature_names: List[str]) -> List[Node]:
        """
        Get feature nodes for a list of feature names

        Parameters
        ----------
        feature_names: List[str]
            List of feature names

        Returns
        -------
        List[Node]
        """
        selected_nodes = []
        for node in self.nodes:
            feature_name = self.graph.get_node_output_column_name(node.name)
            if feature_name in feature_names:
                selected_nodes.append(node)
        return selected_nodes
