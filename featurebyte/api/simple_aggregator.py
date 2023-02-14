"""
This module contains simple aggregator related class
"""
from __future__ import annotations

from typing import List, Optional, Type, cast

from featurebyte.api.base_aggregator import BaseAggregator
from featurebyte.api.feature import Feature
from featurebyte.api.item_view import ItemView
from featurebyte.api.view import View
from featurebyte.common.typing import OptionalScalar
from featurebyte.enum import AggFunc
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.transform.reconstruction import (
    ItemGroupbyNode,
    add_pruning_sensitive_operation,
)


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

    def aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        feature_name: Optional[str] = None,
        fill_value: OptionalScalar = None,
    ) -> Feature:
        """
        Aggregate given value_column for each group specified in keys, without time windows

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        feature_name: str
            Output feature name
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty

        Returns
        -------
        Feature
        """
        self._validate_method_and_value_column(method=method, value_column=value_column)
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
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))
        feature = self._project_feature_from_groupby_node(
            agg_method=agg_method,
            feature_name=feature_name,
            groupby_node=groupby_node,
            method=method,
            value_column=value_column,
            fill_value=fill_value,
        )
        return feature
