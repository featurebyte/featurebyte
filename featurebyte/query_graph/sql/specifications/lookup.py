"""
Lookup spec
"""
from __future__ import annotations

from typing import Optional

from dataclasses import dataclass

from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import LookupNode
from featurebyte.query_graph.sql.specifications.base_lookup import BaseLookupSpec
from featurebyte.query_graph.sql.specs import AggregationSource, AggregationType


@dataclass
class LookupSpec(BaseLookupSpec):
    """
    LookupSpec contains all information required to generate sql for a lookup feature
    """

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.LOOKUP

    @classmethod
    def construct_specs(
        cls,
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
        graph: Optional[QueryGraphModel],
    ) -> list[LookupSpec]:
        assert isinstance(node, LookupNode)
        params = node.parameters
        specs = []
        for input_column_name, feature_name in zip(params.input_column_names, params.feature_names):
            spec = LookupSpec(
                input_column_name=input_column_name,
                feature_name=feature_name,
                entity_column=params.entity_column,
                entity_ids=[params.entity_id],
                serving_names=[params.serving_name],
                serving_names_mapping=serving_names_mapping,
                aggregation_source=aggregation_source,
                scd_parameters=params.scd_parameters,
                event_parameters=params.event_parameters,
            )
            specs.append(spec)
        return specs
