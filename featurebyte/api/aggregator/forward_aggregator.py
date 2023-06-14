"""
Forward aggregator module
"""
from typing import Any, List, Optional, Type

from featurebyte import ChangeView, EventView, ItemView
from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.view import View
from featurebyte.common.model_util import parse_duration_string


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

    def aggregate_over(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        horizon: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> None:
        self._validate_parameters(
            value_column=value_column, method=method, horizon=horizon, target_name=target_name
        )

        node_params = self._prepare_node_parameters(
            value_column=value_column, method=method, horizon=horizon, target_name=target_name
        )
        groupby_node = add_pruning_sensitive_operation(
            graph=self.view.graph,
            node_cls=GroupByNode,
            node_params=node_params,
            input_node=self.view.node,
        )
        assert isinstance(feature_names, list)
        assert method is not None
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))

        items = []
        for feature_name in feature_names:
            feature = self._project_feature_from_groupby_node(
                agg_method=agg_method,
                feature_name=feature_name,
                groupby_node=groupby_node,
                method=method,
                value_column=value_column,
                fill_value=fill_value,
                skip_fill_na=skip_fill_na,
            )
            items.append(feature)
        feature_group = FeatureGroup(items)
        return feature_group

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
