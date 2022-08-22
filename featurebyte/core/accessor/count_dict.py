"""
This module contains count_dict accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

if TYPE_CHECKING:
    from featurebyte.core.series import Series


class CdAccessorMixin:
    """
    CdAccessorMixin class
    """

    # pylint: disable=too-few-public-methods

    @property
    def cd(self: Series) -> CountDictAccessor:  # type: ignore # pylint: disable=invalid-name
        return CountDictAccessor(self)


class CountDictAccessor:
    """
    CountDictAccessor used to manipulate dict-like type Series object
    """

    def __init__(self, obj: Series):
        if obj.var_type != DBVarType.OBJECT:
            raise AttributeError("Can only use .cd accessor with count per category features")
        self._obj = obj

    def _make_operation(self, cd_transform, output_var_type) -> Series:
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.COUNT_DICT_TRANSFORM,
            output_var_type=output_var_type,
            node_params={"cd_transform": cd_transform},
            **self._obj.unary_op_series_params(),
        )

    def entropy(self) -> Series:
        """
        Compute the entropy of the count dictionary
        """
        return self._make_operation("entropy", DBVarType.FLOAT)

    def most_frequent(self) -> Series:
        """
        Compute the most frequent key in the dictionary
        """
        return self._make_operation("most_frequent", DBVarType.VARCHAR)

    def nunique(self) -> Series:
        """
        Compute number of distinct keys in the dictionary
        """
        return self._make_operation("num_unique", DBVarType.FLOAT)
