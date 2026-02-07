"""
Lookup target spec
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from featurebyte.models.column_statistics import ColumnStatisticsInfo
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import LookupTargetNode
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.specifications.base_lookup import BaseLookupSpec
from featurebyte.query_graph.sql.specs import AggregationSource, AggregationType


@dataclass
class LookupTargetSpec(BaseLookupSpec):
    """
    LookupTargetSpec contains all information required to generate sql for a lookup target feature
    """

    offset: Optional[str] = None

    @property
    def is_target(self) -> bool:
        return True

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.LOOKUP_TARGET

    @classmethod
    def construct_specs(
        cls,
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
        graph: QueryGraphModel,
        agg_result_name_include_serving_names: bool,
        column_statistics_info: Optional[ColumnStatisticsInfo],
        on_demand_tile_tables_mapping: Optional[dict[str, str]],
        is_deployment_sql: bool,
        adapter: BaseAdapter,
    ) -> list[LookupTargetSpec]:
        assert isinstance(node, LookupTargetNode)
        params = node.parameters
        specs = []
        for input_column_name, feature_name in zip(params.input_column_names, params.feature_names):
            spec = LookupTargetSpec(
                node_name=node.name,
                input_column_name=input_column_name,
                feature_name=feature_name,
                entity_column=params.entity_column,
                entity_ids=[params.entity_id],
                serving_names=[params.serving_name],
                serving_names_mapping=serving_names_mapping,
                aggregation_source=aggregation_source,
                scd_parameters=params.scd_parameters,
                event_parameters=params.event_parameters,
                snapshots_parameters=params.snapshots_parameters,
                use_forecast_point=params.use_forecast_point,
                forecast_point_schema=params.forecast_point_schema,
                offset=params.offset,
                agg_result_name_include_serving_names=agg_result_name_include_serving_names,
                is_deployment_sql=is_deployment_sql,
            )
            specs.append(spec)
        return specs
