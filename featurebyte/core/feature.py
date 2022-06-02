"""
Feature and FeatureList classes
"""
from __future__ import annotations

from featurebyte.core.frame import Frame
from featurebyte.core.series import Series
from featurebyte.query_graph.graph import Node, QueryGraph


class FeatureMixin:
    """
    FeatureMixin contains common properties & operations shared between FeatureList & Feature
    """

    @property
    def inception_node(self) -> Node:
        """
        Input node where the event source is introduced to the query graph

        Returns
        -------
        Node
        """
        graph = QueryGraph()
        return graph.get_node_by_name(self.row_index_lineage[0])

    def publish(self) -> None:
        """
        Publish feature or feature list
        """
        raise NotImplementedError


class Feature(FeatureMixin, Series):
    """
    Feature class
    """

    def publish(self) -> None:
        pass


class FeatureList(FeatureMixin, Frame):
    """
    FeatureList class
    """

    series_class = Feature

    def publish(self) -> None:
        pass
