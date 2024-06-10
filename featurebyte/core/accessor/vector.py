"""
Vector accessor module
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, TypeVar

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

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.VectorAccessor")

    def __init__(self, obj: FrozenSeries):
        if obj.dtype not in DBVarType.array_types():
            raise AttributeError("Can only use .vec accessor with ARRAY or EMBEDDING values!")
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

        Raises
        ------
        AttributeError
            If the other series is not a vector series
        """
        if other.dtype not in DBVarType.array_types():
            raise AttributeError("Other series should be of ARRAY or EMBEDDING dtype.")
        return series_binary_operation(
            input_series=self._obj,
            other=other,
            node_type=NodeType.VECTOR_COSINE_SIMILARITY,
            output_var_type=DBVarType.FLOAT,
            additional_node_params={},
        )
