"""
This module contains datetime accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

if TYPE_CHECKING:
    from featurebyte.core.series import Series


class DtAccessorMixin:
    """
    DtAccessorMixin class
    """

    # pylint: disable=too-few-public-methods

    @property
    def dt(self: Series) -> DatetimeAccessor:  # type: ignore # pylint: disable=invalid-name
        """
        dt accessor object

        Returns
        -------
        DatetimeAccessor
        """
        return DatetimeAccessor(self)


class DatetimeAccessor:
    """
    DatetimeAccessor class used to manipulate datetime-like type Series object
    """

    # pylint: disable=too-few-public-methods

    _property_node_params_map = {
        "year": "year",
        "quarter": "quarter",
        "month": "month",
        "week": "week",
        "day": "day",
        "day_of_week": "dayofweek",
        "hour": "hour",
        "minute": "minute",
        "second": "second",
    }

    def __init__(self, obj: Series):
        if obj.var_type != DBVarType.TIMESTAMP:
            raise AttributeError("Can only use .dt accessor with TIMESTAMP values!")
        self._obj = obj

    def _extract_datetime_properties(self, node_params: Dict[str, Any]) -> Series:
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.DT_EXTRACT,
            output_var_type=DBVarType.INT,
            node_params=node_params,
            **self._obj.unary_op_series_params(),
        )

    def __getattr__(self, item: str) -> Any:
        try:
            return object.__getattribute__(self, item)
        except AttributeError as exc:
            if item in self._property_node_params_map:
                return series_unary_operation(
                    input_series=self._obj,
                    node_type=NodeType.DT_EXTRACT,
                    output_var_type=DBVarType.INT,
                    node_params={"property": self._property_node_params_map[item]},
                    **self._obj.unary_op_series_params(),
                )
            raise exc
