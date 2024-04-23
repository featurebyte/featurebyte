"""
This module contains as at aggregator base class
"""

from __future__ import annotations

from typing import List, Optional, Type, cast

from abc import abstractmethod

from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.scd_view import SCDView
from featurebyte.api.view import View
from featurebyte.common.model_util import validate_offset_string
from featurebyte.enum import AggFunc
from featurebyte.typing import OptionalScalar


class BaseAsAtAggregator(BaseAggregator):
    """
    Base class for as at aggregators
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [SCDView]

    @property
    @abstractmethod
    def output_name_parameter(self) -> str:
        """
        Parameter name of the output (e.g. feature_name or target_name)

        Returns
        -------
        str
        """

    def _validate_parameters(
        self,
        method: Optional[str],
        output_name: Optional[str],
        value_column: Optional[str],
        offset: Optional[str],
        fill_value: OptionalScalar,
        skip_fill_na: bool,
    ) -> None:
        self._validate_method_and_value_column(method=method, value_column=value_column)
        self._validate_fill_value_and_skip_fill_na(fill_value=fill_value, skip_fill_na=skip_fill_na)

        if method == AggFunc.LATEST:
            raise ValueError("latest aggregation method is not supported for aggregated_asat")

        if output_name is None:
            raise ValueError(f"{self.output_name_parameter} is required")

        view = cast(SCDView, self.view)
        for key in self.keys:
            if key == view.natural_key_column:
                raise ValueError(
                    "Natural key column cannot be used as a groupby key in aggregate_asat"
                )

        if offset is not None:
            validate_offset_string(offset)
