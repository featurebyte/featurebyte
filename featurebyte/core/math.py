"""
Mixin for mathematical functions
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from functools import wraps

from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

if TYPE_CHECKING:
    from featurebyte.core.series import Series

    SeriesT = TypeVar("SeriesT", bound=Series)


def numeric_only(func):  # type: ignore
    """
    Decorator for methods that can only applied to numeric Series

    Returns
    -------
    callable
    """

    @wraps(func)
    def wrapped(self: SeriesT, *args: Any, **kwargs: Any) -> Any:
        op_name = func.__name__
        if not self.is_numeric:
            raise TypeError(f"{op_name} is only available to numeric series; got {self.dtype}")
        return func(self, *args, **kwargs)

    return wrapped


class MathMixin:
    """
    Mixin for mathematical functions
    """

    @numeric_only
    def abs(self: SeriesT) -> SeriesT:  # type: ignore
        """
        Returns a new Series that computes the absolute value of the current Series
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.ABS,
            output_var_type=self.dtype,
            node_params={},
            **self.unary_op_series_params(),
        )

    @numeric_only
    def sqrt(self: SeriesT) -> SeriesT:  # type: ignore
        """
        Returns a new Series that computes the square root of the current Series

        Returns
        -------
        Series

        Raises
        ------
        TypeError
            if the current Series dtype is not compatible with sqrt
        """
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.SQRT,
            output_var_type=DBVarType.FLOAT,
            node_params={},
            **self.unary_op_series_params(),
        )
