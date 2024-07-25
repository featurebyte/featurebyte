"""
This module contains mixin classes for models.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from pydantic import Field, PrivateAttr, field_serializer

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.graph import QueryGraph


class QueryGraphMixin(FeatureByteBaseModel):
    """
    QueryGraphMixin is a mixin class for models that contains the query graph. This class
    contains the logic to deserialize the query graph on demand and cache the result.
    """

    # special handling for those attributes that are expensive to deserialize
    # internal_* is used to store the raw data from persistence, _* is used as a cache
    internal_graph: Any = Field(default=None, frozen=True, alias="graph")
    _graph: Optional[QueryGraph] = PrivateAttr(default=None)

    @field_serializer("internal_graph", when_used="json")
    def _serialize_graph(self, graph: Any) -> Any:
        _ = graph
        return json.loads(self.graph.model_dump_json(by_alias=True))

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
                    graph_dict = self.internal_graph.model_dump(by_alias=True)
                self._graph = QueryGraph(**graph_dict)
        return self._graph
