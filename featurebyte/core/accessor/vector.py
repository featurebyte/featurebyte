"""
Vector accessor module
"""
from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.util import series_binary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType

if TYPE_CHECKING:
    from featurebyte.core.series import FrozenSeries
else:
    FrozenSeries = TypeVar("FrozenSeries")


class VectorAccessorMixin:
    """
    VectorAccessorMixin class
    """

    @property
    def vec(self: FrozenSeries) -> VectorAccessor:  # type: ignore
        """
        Vector accessor object

        Returns
        -------
        VectorAccessor
        """
        return VectorAccessor(self)


class VectorAccessor:
    """
    VectorAccessor class used to manipulate vector type FrozenSeries object
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.VectorAccessor",
    )

    def __init__(self, obj: FrozenSeries):
        if obj.dtype != DBVarType.ARRAY:
            raise AttributeError("Can only use .vec accessor with ARRAY values!")
        self._obj = obj

    def cosine_similarity(self, other: FrozenSeries) -> FrozenSeries:
        """
        Calculate cosine similarity between two vector series.

        Parameters
        ----------
        other: FrozenSeries
            The other vector series

        Returns
        -------
        FrozenSeries
        """
        return series_binary_operation(
            input_series=self._obj,
            other=other,
            node_type=NodeType.VECTOR_COSINE_SIMILARITY,
            output_var_type=DBVarType.FLOAT,
            additional_node_params={},  # TODO: add
        )
