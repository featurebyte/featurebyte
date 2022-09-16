"""
Mixin for mathematical functions
"""
from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

if TYPE_CHECKING:
    from featurebyte.core.series import Series

    SeriesT = TypeVar("SeriesT", bound=Series)


class MathMixin:
    """
    Mixin for mathematical functions
    """

    # pylint: disable=too-few-public-methods

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
        if not self.is_numeric:
            raise TypeError(f"sqrt is only available to numeric series; got {self.dtype}")
        return series_unary_operation(
            input_series=self,
            node_type=NodeType.SQRT,
            output_var_type=DBVarType.FLOAT,
            node_params={},
            **self.unary_op_series_params(),
        )
