"""
NonTileWindowAggregateSpec
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, cast

from bson import ObjectId

from featurebyte.common.model_util import parse_duration_string
from featurebyte.enum import DBVarType
from featurebyte.models.column_statistics import ColumnStatisticsInfo
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import (
    NonTileWindowAggregateNode,
    NonTileWindowAggregateParameters,
)
from featurebyte.query_graph.sql.specs import (
    AggregationSource,
    AggregationType,
    NonTileBasedAggregationSpec,
)


@dataclass
class NonTileWindowAggregateSpec(NonTileBasedAggregationSpec):
    """
    Non-tile window aggregation specification
    """

    parameters: NonTileWindowAggregateParameters
    parent_dtype: Optional[DBVarType]
    window: int
    offset: Optional[int]

    @property
    def aggregation_type(self) -> AggregationType:
        return AggregationType.NON_TILE_WINDOW

    @property
    def agg_result_name(self) -> str:
        """Column name of the aggregated result

        Returns
        -------
        str
            Column name of the aggregated result
        """
        args = self._get_additional_agg_result_name_params()
        return self.get_agg_result_name_from_groupby_parameters(self.parameters, *args)

    def _get_additional_agg_result_name_params(self) -> list[Any]:
        args = [f"W{self.window}"]
        if self.parameters is not None and self.parameters.offset is not None:
            args.append(self.parameters.offset)
        args.extend(self.parameters.feature_job_setting.to_seconds().values())
        return args

    def get_source_hash_parameters(self) -> dict[str, Any]:
        # Input to be aggregated
        params: dict[str, Any] = {
            "source_expr": self.source_expr.sql(),
            "window": self.window,
            "offset": self.offset,
        }

        # Parameters that affect whether aggregation can be done together (e.g. same groupby keys)
        if self.parameters.value_by is None:
            parameters_dict = self.parameters.model_dump(
                exclude={"parent", "agg_func", "names", "windows", "offset"}
            )
        else:
            parameters_dict = self.parameters.model_dump(exclude={"names", "windows", "offset"})
        if parameters_dict.get("timestamp_metadata", None) is None:
            parameters_dict.pop("timestamp_metadata", None)
        if parameters_dict.get("entity_ids") is not None:
            parameters_dict["entity_ids"] = [
                str(entity_id) for entity_id in parameters_dict["entity_ids"]
            ]
        params["parameters"] = parameters_dict

        return params

    @classmethod
    def construct_specs(
        cls,
        node: Node,
        aggregation_source: AggregationSource,
        serving_names_mapping: Optional[dict[str, str]],
        graph: Optional[QueryGraphModel],
        agg_result_name_include_serving_names: bool,
        column_statistics_info: Optional[ColumnStatisticsInfo],
    ) -> list[NonTileWindowAggregateSpec]:
        assert isinstance(node, NonTileWindowAggregateNode)
        specs = []
        for feature_name, window in zip(node.parameters.names, node.parameters.windows):
            assert window is not None
            specs.append(
                cls(
                    node_name=node.name,
                    feature_name=feature_name,
                    parameters=node.parameters,
                    parent_dtype=cls.get_parent_dtype_from_graph(
                        graph, node.parameters.parent, node
                    ),
                    aggregation_source=aggregation_source,
                    entity_ids=cast(List[ObjectId], node.parameters.entity_ids),
                    serving_names=node.parameters.serving_names,
                    serving_names_mapping=serving_names_mapping,
                    agg_result_name_include_serving_names=agg_result_name_include_serving_names,
                    window=parse_duration_string(window),
                    offset=(
                        parse_duration_string(node.parameters.offset)
                        if node.parameters.offset
                        else None
                    ),
                )
            )
        return specs
