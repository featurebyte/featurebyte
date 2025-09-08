"""
This module contains as at aggregator base class
"""

from __future__ import annotations

from abc import abstractmethod
from typing import List, Optional, Type

from featurebyte.api.aggregator.base_aggregator import BaseAggregator
from featurebyte.api.scd_view import SCDView
from featurebyte.api.snapshots_view import SnapshotsView
from featurebyte.api.view import View
from featurebyte.enum import AggFunc
from featurebyte.typing import OptionalScalar


class BaseAsAtAggregator(BaseAggregator):
    """
    Base class for as at aggregators
    """

    @property
    def supported_views(self) -> List[Type[View]]:
        return [SCDView, SnapshotsView]

    @property
    @abstractmethod
    def output_name_parameter(self) -> str:
        """
        Parameter name of the output (e.g. feature_name or target_name)

        Returns
        -------
        str
        """

    @property
    def not_supported_aggregation_methods(self) -> Optional[List[AggFunc]]:
        return [AggFunc.LATEST]

    def _validate_parameters(
        self,
        method: str,
        value_column: Optional[str],
        offset: Optional[str | int],
        fill_value: OptionalScalar,
        skip_fill_na: bool,
    ) -> None:
        self._validate_method_and_value_column(method=method, value_column=value_column)
        self._validate_fill_value_and_skip_fill_na(fill_value=fill_value, skip_fill_na=skip_fill_na)

        if isinstance(self.view, SCDView):
            forbidden_key = self.view.natural_key_column
            forbidden_key_name = "Natural key column"
        else:
            assert isinstance(self.view, SnapshotsView)
            forbidden_key = self.view.snapshot_id_column
            forbidden_key_name = "Snapshot ID column"

        for key in self.keys:
            if key == forbidden_key:
                raise ValueError(
                    f"{forbidden_key_name} cannot be used as a groupby key in aggregate_asat"
                )

        if offset is not None:
            self.view.validate_offset(offset)
