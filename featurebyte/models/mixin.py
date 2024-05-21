"""
This module contains mixin classes for models.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import Field, PrivateAttr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.graph import QueryGraph


class QueryGraphMixin(FeatureByteBaseModel):
    """
    QueryGraphMixin is a mixin class for models that contains the query graph. This class
    contains the logic to deserialize the query graph on demand and cache the result.
    """

    # special handling for those attributes that are expensive to deserialize
    # internal_* is used to store the raw data from persistence, _* is used as a cache
    internal_graph: Any = Field(allow_mutation=False, alias="graph")
    _graph: Optional[QueryGraph] = PrivateAttr(default=None)

    @property
    def graph(self) -> QueryGraph:
        """
        Get the graph. If the graph is not loaded, load it first.

        Returns
        -------
        QueryGraph
            QueryGraph object
        """
        # TODO: make this a cached_property for pydantic v2
        if self._graph is None:
            if isinstance(self.internal_graph, QueryGraph):
                self._graph = self.internal_graph
            else:
                if isinstance(self.internal_graph, dict):
                    graph_dict = self.internal_graph
                else:
                    # for example, QueryGraphModel
                    graph_dict = self.internal_graph.dict(by_alias=True)
                self._graph = QueryGraph(**graph_dict)
        return self._graph
