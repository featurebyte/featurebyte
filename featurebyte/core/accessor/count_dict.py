"""
This module contains count_dict accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, Union

from typeguard import typechecked

from featurebyte.api.feature_validation_util import assert_is_lookup_feature
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import Scalar
from featurebyte.core.util import SeriesBinaryOperator, series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.count_dict import GetValueFromDictionaryNode

if TYPE_CHECKING:
    from featurebyte.api.feature import Feature
else:
    Feature = TypeVar("Feature")


class CdAccessorMixin:
    """
    CdAccessorMixin class
    """

    @property
    def cd(self: Feature) -> CountDictAccessor:  # type: ignore # pylint: disable=invalid-name
        """
        Accessor object that provides transformations on count dictionary features

        Returns
        -------
        CountDictAccessor
        """
        return CountDictAccessor(self)


class CountDictSeriesOperator(SeriesBinaryOperator):
    """
    CountDict series operator that has specialized handling for input validation.
    """

    def validate_inputs(self) -> None:
        """
        Validate the input series, and other parameter.

        Raises
        ------
        TypeError
            If the the current or the other Feature is not of dictionary type
        """
        if not isinstance(self.other, type(self.input_series)):
            raise TypeError(f"cosine_similarity is only available for Feature; got {self.other}")
        if self.other.dtype != DBVarType.OBJECT:
            raise TypeError(
                f"cosine_similarity is only available for Feature of dictionary type; got "
                f"{self.other.dtype}"
            )


class CountDictAccessor:
    """
    CountDictAccessor used to manipulate dict-like type Feature object
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Series"],
        proxy_class="featurebyte.Series",
        accessor_name="cd",
    )

    def __init__(self, obj: Feature):
        if obj.dtype != DBVarType.OBJECT:
            raise AttributeError("Can only use .cd accessor with count per category features")
        self._feature_obj = obj

    def _make_operation(
        self,
        transform_type: str,
        output_var_type: DBVarType,
        additional_params: dict[str, Any] | None = None,
    ) -> Feature:
        node_params = {"transform_type": transform_type}
        if additional_params is not None:
            node_params.update(additional_params)
        return series_unary_operation(
            input_series=self._feature_obj,
            node_type=NodeType.COUNT_DICT_TRANSFORM,
            output_var_type=output_var_type,
            node_params=node_params,
            **self._feature_obj.unary_op_series_params(),
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

    @typechecked()
    def unique_count(self, include_missing: bool = True) -> Feature:
        """
        Compute number of distinct keys in the dictionary

        Parameters
        ----------
        include_missing : bool
            Whether to include missing value when counting the number of distinct keys

        Returns
        -------
        Feature
        """
        return self._make_operation(
            "unique_count",
            DBVarType.FLOAT,
            additional_params={"include_missing": include_missing},
        )

    def cosine_similarity(self, other: Feature) -> Feature:
        """
        Compute the cosine similarity with another dictionary Feature

        Parameters
        ----------
        other : Feature
            Another dictionary feature

        Returns
        -------
        Feature
        """
        series_operator = CountDictSeriesOperator(self._feature_obj, other)
        return series_operator.operate(
            node_type=NodeType.COSINE_SIMILARITY,
            output_var_type=DBVarType.FLOAT,
            **self._feature_obj.binary_op_series_params(),
        )

    def get_value(self, key: Union[Scalar, Feature]) -> Feature:
        """
        Get the value in a dictionary feature, based on the key provided.

        This key could be either
        - the value in the lookup feature, or
        - a scalar value passed in.

        Parameters
        ----------
        key: Union[Scalar, Feature]
            key to lookup the value for

        Returns
        -------
        Feature
            new feature

        Examples
        --------
        Getting value from a dictionary feature using a scalar value

        >>> dictionary_feature.cd.get_value("key")  # doctest: +SKIP

        Getting value from a dictionary feature using a lookup feature

        >>> dictionary_feature.cd.get_value(lookup_feature)  # doctest: +SKIP
        """
        feature_clazz = type(self._feature_obj)
        if isinstance(key, feature_clazz):
            assert_is_lookup_feature(key.node_types_lineage)

        additional_node_params = {}
        # We only need to assign value if we have been passed in a single scalar value.
        if not isinstance(key, feature_clazz):
            additional_node_params["value"] = key

        # construct operation structure of the get value node output
        op_struct = self._feature_obj.graph.extract_operation_structure(node=self._feature_obj.node)
        get_value_node = GetValueFromDictionaryNode(name="temp", parameters=additional_node_params)
        response = self._feature_obj._binary_op(  # pylint: disable=protected-access
            other=key,
            node_type=NodeType.GET_VALUE,
            output_var_type=get_value_node.derive_var_type([op_struct]),
            right_op=False,
            additional_node_params=additional_node_params,
        )
        assert isinstance(response, feature_clazz)
        return response
