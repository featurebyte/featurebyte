"""
This module contains count_dict accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from featurebyte.core.util import series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

if TYPE_CHECKING:
    from featurebyte.api.feature import Feature


class CdAccessorMixin:
    """
    CdAccessorMixin class
    """

    # pylint: disable=too-few-public-methods

    @property
    def cd(self: Feature) -> CountDictAccessor:  # type: ignore # pylint: disable=invalid-name
        """
        Accessor object that provides transformations on count dictionary features

        Returns
        -------
        CountDictAccessor
        """
        return CountDictAccessor(self)


class CountDictAccessor:
    """
    CountDictAccessor used to manipulate dict-like type Series object
    """

    def __init__(self, obj: Feature):
        if obj.var_type != DBVarType.OBJECT:
            raise AttributeError("Can only use .cd accessor with count per category features")
        self._obj = obj

    def _make_operation(self, transform_type: str, output_var_type: DBVarType) -> Feature:
        return series_unary_operation(
            input_series=self._obj,
            node_type=NodeType.COUNT_DICT_TRANSFORM,
            output_var_type=output_var_type,
            node_params={"transform_type": transform_type},
            **self._obj.unary_op_series_params(),
        )

    def entropy(self) -> Feature:
        """
        Compute the entropy of the count dictionary

        Returns
        -------
        Feature
        """
        return self._make_operation("entropy", DBVarType.FLOAT)

    def most_frequent(self) -> Feature:
        """
        Compute the most frequent key in the dictionary

        Returns
        -------
        Feature
        """
        return self._make_operation("most_frequent", DBVarType.VARCHAR)

    def nunique(self) -> Feature:
        """
        Compute number of distinct keys in the dictionary

        Returns
        -------
        Feature
        """
        return self._make_operation("num_unique", DBVarType.FLOAT)
