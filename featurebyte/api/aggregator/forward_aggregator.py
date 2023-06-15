"""
Forward aggregator module
"""
from typing import Any, List, Optional, Type, cast

from featurebyte import AggFunc, ChangeView, EventView, ItemView
from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.target import Target
from featurebyte.api.view import View
from featurebyte.common.model_util import parse_duration_string
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.transform.reconstruction import add_pruning_sensitive_operation


class ForwardAggregator(BaseAggregator):
    """
    ForwardAggregator implements the forward_aggregate method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [EventView, ItemView, ChangeView]

    @property
    def aggregation_method_name(self) -> str:
        return "forward_aggregate"

    def forward_aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        horizon: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> Target:
        # Validation
        self._validate_parameters(
            value_column=value_column, method=method, horizon=horizon, target_name=target_name
        )

        # Create new node
        node_params = self._prepare_node_parameters(
            value_column=value_column, method=method, horizon=horizon, target_name=target_name
        )
        # Don't need this as there's no tile IDs in the parameters
        # Can just call graph.add_operation directly
        groupby_node = add_pruning_sensitive_operation(
            graph=self.view.graph,
            node_cls=GroupByNode,  # create a new node - ForwardAggregateNode
            node_params=node_params,
            input_node=self.view.node,
        )
        # Potentially include projection, so that the SQL generation logic can be similar with the other aggregations.

        # Build and return Target
        return Target(
            name=target_name,
            entity_ids=self.entity_ids,
            horizon=horizon,
            blind_spot="",
        )

    # Notes
    # - materialization: Target.materialize(observation_table|dataframe) -> this will trigger the SQL generation
    # - can target interact w/ other targets/features?
    #   will influence how the SQL generation is done; can reuse some of the feature part
    # expected SQL after generation can be found in expected_preview_sql.sql

    def _prepare_node_parameters(
        self,
        value_column: Optional[str],
        method: Optional[str],
        horizon: Optional[str],
        target_name: Optional[str],
    ) -> dict[str, Any]:
        return {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "horizon": horizon,
            "name": target_name,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
        }

    def _validate_parameters(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        horizon: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> None:
        self._validate_method_and_value_column(method=method, value_column=value_column)

        if not target_name:
            raise ValueError("Target name must be specified")

        if horizon:
            parse_duration_string(horizon)
