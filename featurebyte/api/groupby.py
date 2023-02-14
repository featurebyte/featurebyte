"""
This module contains groupby related class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, Union, cast

from abc import ABC, abstractmethod

from typeguard import typechecked

from featurebyte.api.as_at_aggregator import AsAtAggregator
from featurebyte.api.base_aggregator import BaseAggregator
from featurebyte.api.change_view import ChangeView
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup
from featurebyte.api.item_view import ItemView
from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.api.simple_aggregator import SimpleAggregator
from featurebyte.api.view import View
from featurebyte.api.window_aggregator import WindowAggregator
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


class GroupBy:
    """
    GroupBy class that is applicable to EventView, ItemView, and ChangeView
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["GroupBy"])

    @typechecked
    def __init__(
        self,
        obj: Union[EventView, ItemView, ChangeView, SlowlyChangingView],
        keys: Union[str, List[str]],
        category: Optional[str] = None,
    ):
        keys_value = []
        if isinstance(keys, str):
            keys_value.append(keys)
        elif isinstance(keys, list):
            keys_value = keys

        # construct column name entity mapping
        columns_info = obj.columns_info
        column_entity_map = {col.name: col.entity_id for col in columns_info if col.entity_id}

        # construct serving_names
        serving_names = []
        entity_ids = []
        for key in keys_value:
            if key not in obj.columns:
                raise KeyError(f'Column "{key}" not found!')
            if key not in column_entity_map:
                raise ValueError(f'Column "{key}" is not an entity!')

            entity = Entity.get_by_id(column_entity_map[key])
            serving_names.append(entity.serving_name)
            entity_ids.append(entity.id)

        if category is not None and category not in obj.columns:
            raise KeyError(f'Column "{category}" not found!')

        self.view_obj = obj
        self.keys = keys_value
        self.category = category
        self.serving_names = serving_names
        self.entity_ids = entity_ids

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.view_obj}, keys={self.keys})"

    def __str__(self) -> str:
        return repr(self)

    @typechecked
    def aggregate_over(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        windows: Optional[List[Optional[str]]] = None,
        feature_names: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
        feature_job_setting: Optional[Dict[str, str]] = None,
        fill_value: OptionalScalar = None,
    ) -> FeatureGroup:
        """
        Aggregate given value_column for each group specified in keys over a list of time windows

        This aggregation is available to EventView, ItemView, and ChangeView.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: str
            Aggregation method
        windows: List[str]
            List of aggregation window sizes. Use `None` to indicated unbounded window size (only
            applicable to "latest" method). Format of a window size is "{size}{unit}",
            where size is a positive integer and unit is one of the following:

            "ns": nanosecond
            "us": microsecond
            "ms": millisecond
            "s": second
            "m": minute
            "h": hour
            "d": day
            "w": week

            **Note**: Window sizes must be multiples of feature job frequency

        feature_names: List[str]
            Output feature names
        timestamp_column: Optional[str]
            Timestamp column used to specify the window (if not specified, event data timestamp is used)
        feature_job_setting: Optional[Dict[str, str]]
            Dictionary contains `blind_spot`, `frequency` and `time_modulo_frequency` keys which are
            feature job setting parameters
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty

        Returns
        -------
        FeatureGroup

        Examples
        --------
        >>> import featurebyte as fb
        >>> features = cc_transactions.groupby("AccountID").aggregate_over(  # doctest: +SKIP
        ...    "Amount",
        ...    method=fb.AggFunc.SUM,
        ...    windows=["7d", "30d"],
        ...    feature_names=["Total spend 7d", "Total spend 30d"],
        ... )
        >>> features.feature_names  # doctest: +SKIP
        ['Total spend 7d', 'Total spend 30d']

        See Also
        --------
        - [FeatureGroup](/reference/featurebyte.api.feature_list.FeatureGroup/): FeatureGroup object
        - [Feature](/reference/featurebyte.api.feature.Feature/): Feature object
        """
        return WindowAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate_over(
            value_column=value_column,
            method=method,
            windows=windows,
            feature_names=feature_names,
            timestamp_column=timestamp_column,
            feature_job_setting=feature_job_setting,
            fill_value=fill_value,
        )

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

        Examples
        --------
        >>> import featurebyte as fb
        >>> feature = credit_card_accounts.groupby("CustomerID").aggregate_asat(  # doctest: +SKIP
        ...    method=fb.AggFunc.COUNT,
        ...    feature_name="Number of Credit Cards",
        ... )
        >>> feature  # doctest: +SKIP
        Feature[FLOAT](name=Number of Credit Cards, node_name=alias_1)
        """
        return AsAtAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate_asat(
            value_column=value_column,
            method=method,
            feature_name=feature_name,
            offset=offset,
            backward=backward,
            fill_value=fill_value,
        )

    @typechecked
    def aggregate(
        self,
        value_column: Optional[str] = None,
        method: Optional[str] = None,
        feature_name: Optional[str] = None,
        fill_value: OptionalScalar = None,
    ) -> Feature:
        """
        Aggregate given value_column for each group specified in keys, without time windows

        This aggregation is available to ItemView.

        Parameters
        ----------
        value_column: Optional[str]
            Column to be aggregated
        method: Optional[str]
            Aggregation method
        feature_name: Optional[str]
            Output feature name
        fill_value: OptionalScalar
            Value to fill if the value in the column is empty

        Returns
        -------
        Feature
        """
        return SimpleAggregator(
            self.view_obj, self.category, self.entity_ids, self.keys, self.serving_names
        ).aggregate(
            value_column=value_column,
            method=method,
            feature_name=feature_name,
            fill_value=fill_value,
        )
