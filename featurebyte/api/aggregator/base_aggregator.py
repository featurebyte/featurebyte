"""
This module contains base aggregator related class
"""

from __future__ import annotations

from typing import List, Optional, Type, Union

from abc import ABC, abstractmethod

from featurebyte.api.aggregator.vector_validator import validate_vector_aggregate_parameters
from featurebyte.api.feature import Feature
from featurebyte.api.target import Target
from featurebyte.api.view import View
from featurebyte.enum import AggFunc, DBVarType
from featurebyte.exception import AggregationNotSupportedForViewError
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.agg_func import AggFuncType
from featurebyte.typing import OptionalScalar, get_or_default


class BaseAggregator(ABC):
    """
    BaseAggregator is the base class for aggregators in groupby
    """

    def __init__(
        self,
        view: View,
        category: Optional[str],
        entity_ids: list[PydanticObjectId],
        keys: list[str],
        serving_names: list[str],
    ):
        self.view = view
        self.category = category
        self.entity_ids = entity_ids
        self.keys = keys
        self.serving_names = serving_names
        if not isinstance(self.view, tuple(self.supported_views)):
            supported_views_formatted = ", ".join(
                [view_cls.__name__ for view_cls in self.supported_views]
            )
            raise AggregationNotSupportedForViewError(
                f"{self.aggregation_method_name}() is only available for {supported_views_formatted}"
            )

    @property
    @abstractmethod
    def supported_views(self) -> List[Type[View]]:
        """
        Views that support this type of aggregation

        Returns
        -------
        List[Type[View]]
        """

    @property
    @abstractmethod
    def aggregation_method_name(self) -> str:
        """
        Aggregation method name for readable error message

        Returns
        -------
        str
        """

    def _validate_method_and_value_column(
        self, method: Optional[str], value_column: Optional[str]
    ) -> None:
        if method is None:
            raise ValueError("method is required")

        if method not in AggFunc.all():
            raise ValueError(f"Aggregation method not supported: {method}")

        if method == AggFunc.COUNT:
            if value_column is not None:
                raise ValueError(
                    "Specifying value column is not allowed for COUNT aggregation;"
                    " try setting None as the value_column"
                )
        else:
            if value_column is None:
                raise ValueError("value_column is required")
            if value_column not in self.view.columns:
                raise KeyError(f'Column "{value_column}" not found in {self.view}!')

        validate_vector_aggregate_parameters(self.view.columns_info, value_column, method)

    @staticmethod
    def _validate_fill_value_and_skip_fill_na(
        fill_value: OptionalScalar, skip_fill_na: bool
    ) -> None:
        if fill_value is not None and skip_fill_na:
            raise ValueError(
                "Specifying both fill_value and skip_fill_na is not allowed;"
                " try setting fill_value to None or skip_fill_na to False"
            )

    def get_output_var_type(
        self, agg_method: AggFuncType, method: str, value_column: str
    ) -> DBVarType:
        """
        Get output variable type for aggregation method.

        Parameters
        ----------
        agg_method: AggFuncType
            Aggregation method
        method: str
            Aggregation method name
        value_column: str
            Value column name

        Returns
        -------
        DBVarType

        Raises
        ------
        ValueError
            If aggregation method does not support input variable type
        """
        # value_column is None for count-like aggregation method
        input_var_type = self.view.column_var_type_map.get(value_column, DBVarType.FLOAT)
        if not agg_method.is_var_type_supported(input_var_type):
            raise ValueError(
                f'Aggregation method "{method}" does not support "{input_var_type}" input variable'
            )
        return agg_method.derive_output_var_type(
            input_var_type=input_var_type, category=self.category
        )

    def _project_feature_from_groupby_node(
        self,
        agg_method: AggFuncType,
        feature_name: str,
        groupby_node: Node,
        method: str,
        value_column: Optional[str],
        fill_value: OptionalScalar,
        skip_fill_na: bool,
    ) -> Feature:
        # value_column is None for count-like aggregation method
        var_type = self.get_output_var_type(agg_method, method, value_column)  # type: ignore[arg-type]

        feature = self.view.project_feature_from_node(
            node=groupby_node,
            feature_name=feature_name,
            feature_dtype=var_type,
        )
        if not skip_fill_na:
            self._fill_feature_or_target(feature, method, feature_name, fill_value)
        return feature

    def _fill_feature_or_target(
        self,
        feature_or_target: Union[Feature, Target],
        method: str,
        feature_or_target_name: str,
        fill_value: OptionalScalar,
    ) -> Union[Feature, Target]:
        """
        Fill feature or target values as needed.

        Parameters
        ----------
        feature_or_target: Union[Feature, Target]
            feature or target
        method: str
            aggregation method
        feature_or_target_name: str
            feature or target name
        fill_value: OptionalScalar
            value to fill

        Returns
        -------
        Feature

        Raises
        ------
        ValueError
            If both fill_value and category parameters are specified
        """
        if fill_value is not None and self.category is not None:
            raise ValueError("fill_value is not supported for aggregation per category")

        if method in {AggFunc.COUNT, AggFunc.NA_COUNT} and self.category is None:
            # Count features should be 0 instead of NaN when there are no records
            value_to_fill = get_or_default(fill_value, 0)
            feature_or_target.fillna(value_to_fill)
            feature_or_target.name = feature_or_target_name
        elif fill_value is not None:
            feature_or_target.fillna(fill_value)
            feature_or_target.name = feature_or_target_name

        return feature_or_target
