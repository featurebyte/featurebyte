"""
ForecastAggregateAsAtSpec
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, cast

from bson import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.models.column_statistics import ColumnStatisticsInfo
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    ForecastAggregateAsAtNode,
    ForecastAggregateAsAtParameters,
)
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.specifications.base_aggregate_asat import BaseAggregateAsAtSpec
from featurebyte.query_graph.sql.specs import AggregationSource, AggregationType


@dataclass
class ForecastAggregateAsAtSpec(BaseAggregateAsAtSpec):
    """
    Forecast as-at aggregation specification.

    This spec is used for aggregation that uses FORECAST_POINT instead of POINT_IN_TIME
    as the reference time for as-at joins.
    """

    parameters: ForecastAggregateAsAtParameters
    parent_dtype: Optional[DBVarType]
    use_forecast_point: bool = True
    forecast_point_schema: Optional[ForecastPointSchema] = None

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.FORECAST_AS_AT

    @classmethod
    def construct_specs(  # type: ignore[override]
        cls,
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
        graph: Optional[QueryGraphModel],
        agg_result_name_include_serving_names: bool,
        column_statistics_info: Optional[ColumnStatisticsInfo],
        on_demand_tile_tables_mapping: Optional[dict[str, str]],
        is_deployment_sql: bool,
        adapter: BaseAdapter,
    ) -> list[ForecastAggregateAsAtSpec]:
        assert isinstance(node, ForecastAggregateAsAtNode)
        return [
            cls(
                node_name=node.name,
                feature_name=node.parameters.name,
                parameters=node.parameters,
                parent_dtype=cls.get_parent_dtype_from_graph(graph, node.parameters.parent, node),
                aggregation_source=aggregation_source,
                entity_ids=cast(List[ObjectId], node.parameters.entity_ids),
                serving_names=node.parameters.serving_names,
                serving_names_mapping=serving_names_mapping,
                agg_result_name_include_serving_names=agg_result_name_include_serving_names,
                is_deployment_sql=is_deployment_sql,
                use_forecast_point=node.parameters.use_forecast_point,
                forecast_point_schema=node.parameters.forecast_point_schema,
            )
        ]
