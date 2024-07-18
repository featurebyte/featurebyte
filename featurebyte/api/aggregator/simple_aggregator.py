"""
This module contains simple aggregator related class
"""

from __future__ import annotations

from typing import List, Optional, Type, Union

from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.aggregator.util import conditional_set_skip_fill_na
from featurebyte.api.feature import Feature
from featurebyte.api.item_view import ItemView
from featurebyte.api.view import View
from featurebyte.enum import AggFunc
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.transform.reconstruction import (
    ItemGroupbyNode,
    add_pruning_sensitive_operation,
)
from featurebyte.typing import OptionalScalar


class SimpleAggregator(BaseAggregator):
    """
    SimpleAggregator implements the aggregate method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [ItemView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate"

    @property
    def not_supported_aggregation_methods(self) -> Optional[List[AggFunc]]:
        return [AggFunc.LATEST]

    def aggregate(
        self,
        value_column: Optional[str],
        method: Union[AggFunc, str],
        feature_name: str,
        fill_value: OptionalScalar = None,
        skip_fill_na: Optional[bool] = None,
    ) -> Feature:
        """
        Aggregate given value_column for each group specified in keys, without time windows

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: Union[AggFunc, str]
            Aggregation method
        feature_name: str
            Output feature name
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty
        skip_fill_na: Optional[bool]
            Whether to skip filling NaN values

        Returns
        -------
        Feature
        """
        skip_fill_na = conditional_set_skip_fill_na(skip_fill_na, fill_value)
        self._validate_method_and_value_column(method=method, value_column=value_column)
        self._validate_fill_value_and_skip_fill_na(fill_value=fill_value, skip_fill_na=skip_fill_na)
        self.view.validate_simple_aggregate_parameters(
            keys=self.keys,
            value_column=value_column,
        )

        node_params = {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": self.category,
            "name": feature_name,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
        }
        groupby_node = add_pruning_sensitive_operation(
            graph=self.view.graph,
            node_cls=ItemGroupbyNode,
            node_params=node_params,
            input_node=self.view.node,
        )

        assert method is not None
        assert feature_name is not None
        agg_method = construct_agg_func(agg_func=AggFunc(method))
        feature = self._project_feature_from_aggregation_node(
            agg_method=agg_method,
            feature_name=feature_name,
            aggregation_node=groupby_node,
            method=method,
            value_column=value_column,
            fill_value=fill_value,
            skip_fill_na=skip_fill_na,
        )
        return feature
