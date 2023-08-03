"""
Lookup spec
"""
from typing import Any, Optional

from dataclasses import dataclass

from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    EventLookupParameters,
    LookupNode,
    LookupTargetNode,
    SCDLookupParameters,
)
from featurebyte.query_graph.sql.specs import (
    AggregationSource,
    AggregationType,
    NonTileBasedAggregationSpec,
)


@dataclass
class LookupSpec(NonTileBasedAggregationSpec):
    """
    LookupSpec contains all information required to generate sql for a lookup feature
    """

    input_column_name: str
    feature_name: str
    entity_column: str
    serving_names: list[str]
    scd_parameters: Optional[SCDLookupParameters]
    event_parameters: Optional[EventLookupParameters]
    is_parent_lookup: bool = False

    @property
    def agg_result_name(self) -> str:
        if self.is_parent_lookup:
            return self.feature_name
        return self.construct_agg_result_name(self.input_column_name)

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.LOOKUP

    def get_source_hash_parameters(self) -> dict[str, Any]:
        params: dict[str, Any] = {
            "source_expr": self.source_expr.sql(),
            "entity_column": self.entity_column,
        }
        if self.scd_parameters is not None:
            params["scd_parameters"] = self.scd_parameters.dict()
        if self.event_parameters is not None:
            params["event_parameters"] = self.event_parameters.dict()
        return params

    @classmethod
    def should_filter_scd_by_current_flag(cls, graph: QueryGraphModel, node: Node) -> bool:
        assert isinstance(node, (LookupNode, LookupTargetNode))
        scd_parameters = node.parameters.scd_parameters
        if scd_parameters is not None:
            return cls.get_scd_filter_flag_from_scd_parameters(scd_parameters)
        return False

    @staticmethod
    def get_scd_filter_flag_from_scd_parameters(scd_parameters: SCDLookupParameters) -> bool:
        """
        Returns whether the current flag should be used to filter SCD join

        Parameters
        ----------
        scd_parameters: SCDLookupParameters
            SCD related parameters in the lookup node

        Returns
        -------
        bool
        """
        # Online serving might not have to use SCD join if current flag is applicable
        current_flag_usable_for_online_serving = (
            scd_parameters.current_flag_column is not None and scd_parameters.offset is None
        )
        return current_flag_usable_for_online_serving

    @classmethod
    def construct_specs(
        cls,
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
    ) -> list[LookupSpec]:
        assert isinstance(node, (LookupNode, LookupTargetNode))
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
