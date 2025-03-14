"""
This module contains mixin classes for models.
"""

from __future__ import annotations

import json
from functools import cached_property
from typing import Any

from pydantic import Field, field_serializer

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

    @field_serializer("internal_graph", when_used="json")
    def _serialize_graph(self, graph: Any) -> Any:
        _ = graph
        return json.loads(self.graph.model_dump_json(by_alias=True))

    @cached_property
    def graph(self) -> QueryGraph:
        """
        Get the graph. If the graph is not loaded, load it first.

        Returns
        -------
        QueryGraph
            QueryGraph object
        """
        if isinstance(self.internal_graph, QueryGraph):
            return self.internal_graph
        else:
            if isinstance(self.internal_graph, dict):
                graph_dict = self.internal_graph
            else:
                # for example, QueryGraphModel
                graph_dict = self.internal_graph.model_dump(by_alias=True)
            return QueryGraph(**graph_dict)
