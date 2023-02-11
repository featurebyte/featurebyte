"""
This module contains as at aggregator related class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, Union, cast

from abc import ABC, abstractmethod

from typeguard import typechecked

from featurebyte.api.change_view import ChangeView
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup
from featurebyte.api.item_view import ItemView
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.api.view import View
from featurebyte.api.base_aggregator import BaseAggregator
from featurebyte.api.window_validator import validate_window
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import validate_offset_string
from featurebyte.common.typing import OptionalScalar, get_or_default
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.exception import AggregationNotSupportedForViewError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.agg_func import AggFuncType, construct_agg_func
from featurebyte.query_graph.transform.reconstruction import (
    GroupbyNode,
    ItemGroupbyNode,
    add_pruning_sensitive_operation,
)


class AsAtAggregator(BaseAggregator):
    """
    AsAtAggregator implements the aggregate_asat method for GroupBy
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [SlowlyChangingView]

    @property
    def aggregation_method_name(self) -> str:
        return "aggregate_asat"

    @typechecked
    def aggregate_asat(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        feature_name: Optional[str] = None,
        offset: Optional[str] = None,
        backward: bool = True,
        fill_value: OptionalScalar = None,
    ) -> Feature:
        """
        Aggregate a column in SlowlyChangingView as at a point in time

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        feature_name: str
            Output feature name
        offset: Optional[str]
            Optional offset to apply to the point in time column in the feature request. The
            aggregation result will be as at the point in time adjusted by this offset. Format of
            offset is "{size}{unit}", where size is a positive integer and unit is one of the
            following:

            "ns": nanosecond
            "us": microsecond
            "ms": millisecond
            "s": second
            "m": minute
            "h": hour
            "d": day
            "w": week

        backward: bool
            Whether the offset should be applied backward or forward
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty

        Returns
        -------
        Feature
        """
        self._validate_parameters(
            method=method,
            value_column=value_column,
            feature_name=feature_name,
            offset=offset,
        )

        view = cast(SlowlyChangingView, self.view)
        node_params = {
            "keys": self.keys,
            "parent": value_column,
            "agg_func": method,
            "value_by": self.category,
            "name": feature_name,
            "serving_names": self.serving_names,
            "entity_ids": self.entity_ids,
            "offset": offset,
            "backward": backward,
            **view.get_common_scd_parameters().dict(),
        }
        groupby_node = self.view.graph.add_operation(
            node_type=NodeType.AGGREGATE_AS_AT,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.view.node],
        )

        assert method is not None
        assert feature_name is not None
        agg_method = construct_agg_func(agg_func=cast(AggFunc, method))

        return self._project_feature_from_groupby_node(
            agg_method=agg_method,
            feature_name=feature_name,
            groupby_node=groupby_node,
            method=method,
            value_column=value_column,
            fill_value=fill_value,
        )

    def _validate_parameters(
        self,
        method: Optional[str],
        feature_name: Optional[str],
        value_column: Optional[str],
        offset: Optional[str],
    ) -> None:

        self._validate_method_and_value_column(method=method, value_column=value_column)

        if method == AggFunc.LATEST:
            raise ValueError("latest aggregation method is not supported for aggregated_asat")

        if feature_name is None:
            raise ValueError("feature_name is required")

        if self.category is not None:
            raise ValueError("category is not supported for aggregate_asat")

        view = cast(SlowlyChangingView, self.view)
        for key in self.keys:
            if key == view.natural_key_column:
                raise ValueError(
                    "Natural key column cannot be used as a groupby key in aggregate_asat"
                )

        if offset is not None:
            validate_offset_string(offset)
            raise NotImplementedError("offset support is not yet implemented")
