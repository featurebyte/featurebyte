"""
This module contains count_dict accessor class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, TypeVar, Union

from typeguard import typechecked

from featurebyte.api.feature_validation_util import assert_is_lookup_feature
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import Scalar
from featurebyte.core.series import DefaultSeriesBinaryOperator
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
    # pylint: disable=line-too-long
    """
    CountDictAccessor used to manipulate dict-like type Feature object
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        proxy_class="featurebyte.Series",
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
        Compute the entropy of the dictionary over the keys. The values are normalized to sum to 1
        and used as the probability of each key in the entropy calculation.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------

        Create a new feature by calculating the entropy of the dictionary feature:

        >>> counts = fb.Feature.get("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.entropy()
        >>> new_feature.name = "CustomerProductGroupCountsEntropy_7d"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(pd.DataFrame([{"POINT_IN_TIME": "2022-04-15 10:00:00", "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0"}]))


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        '{"Chips et Tortillas":1,"Colas, Thés glacés et Sodas":3,"Crèmes et Chantilly":1,"Pains":1,"Œufs":1}'


        New feature:

        >>> df["CustomerProductGroupCountsEntropy_7d"].iloc[0]
        1.475076311054695
        """
        return self._make_operation("entropy", DBVarType.FLOAT)

    def most_frequent(self) -> Feature:
        """
        Retrieve the most frequent key in the dictionary feature.

        When there are ties, the lexicographically smallest key is returned.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------

        Create a new feature by retrieving the most frequent key of the dictionary feature:

        >>> counts = fb.Feature.get("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.most_frequent()
        >>> new_feature.name = "CustomerProductGroupCountsMostFrequent_7d"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(pd.DataFrame([{"POINT_IN_TIME": "2022-04-15 10:00:00", "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0"}]))


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        '{"Chips et Tortillas":1,"Colas, Thés glacés et Sodas":3,"Crèmes et Chantilly":1,"Pains":1,"Œufs":1}'


        New feature:

        >>> df["CustomerProductGroupCountsMostFrequent_7d"].iloc[0]
        'Colas, Thés glacés et Sodas'
        """
        return self._make_operation("most_frequent", DBVarType.VARCHAR)

    @typechecked()
    def unique_count(self, include_missing: bool = True) -> Feature:
        """
        Compute number of distinct keys in the dictionary feature.

        Parameters
        ----------
        include_missing : bool
            Whether to include missing value when counting the number of distinct keys.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------

        Create a new feature by counting the number of keys in the dictionary feature:

        >>> counts = fb.Feature.get("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.unique_count()
        >>> new_feature.name = "CustomerProductGroupCountsUniqueCount_7d"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(pd.DataFrame([{"POINT_IN_TIME": "2022-04-15 10:00:00", "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0"}]))


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        '{"Chips et Tortillas":1,"Colas, Thés glacés et Sodas":3,"Crèmes et Chantilly":1,"Pains":1,"Œufs":1}'


        New feature:

        >>> df["CustomerProductGroupCountsUniqueCount_7d"].iloc[0]
        5
        """
        return self._make_operation(
            "unique_count",
            DBVarType.FLOAT,
            additional_params={"include_missing": include_missing},
        )

    def cosine_similarity(self, other: Feature) -> Feature:
        """
        Compute the cosine similarity with another dictionary Feature.

        Parameters
        ----------
        other : Feature
            Another dictionary feature.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------
        Create a similarity feature between two dictionary features:

        >>> feature_1 = fb.Feature.get("CustomerProductGroupCounts_7d")
        >>> feature_2 = fb.Feature.get("CustomerProductGroupCounts_90d")
        >>> similarity = feature_1.cd.cosine_similarity(feature_2)
        >>> similarity.name = "CustomerProductGroupCounts_7d_90d_similarity"


        Preview the features:

        >>> features = fb.FeatureGroup([feature_1, feature_2, similarity])
        >>> df = features.preview(pd.DataFrame([{"POINT_IN_TIME": "2022-04-15 10:00:00", "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0"}]))


        Dictionary feature 1:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
         '{"Chips et Tortillas":1,"Colas, Thés glacés et Sodas":3,"Crèmes et Chantilly":1,"Pains":1,"Œufs":1}'


        Dictionary feature 2:

        >>> df["CustomerProductGroupCounts_90d"].iloc[0]
        '{"Biscuits apéritifs":1,"Biscuits":1,"Bonbons":1,"Chips et Tortillas":2,"Colas, Thés glacés et Sodas":12,"Confitures":1,"Crèmes et Chantilly":2,"Céréales":1,"Emballages et sacs":1,"Fromages":3,"Glaces et Sorbets":1,"Glaçons":1,"Laits":4,"Noix":1,"Pains":4,"Petit-déjeuner":2,"Viande Surgelée":1,"Œufs":1}'


        Similarity feature:

        >>> df["CustomerProductGroupCounts_7d_90d_similarity"].iloc[0]
        0.8653846153846161
        """
        series_operator = CountDictSeriesOperator(self._feature_obj, other)
        return series_operator.operate(
            node_type=NodeType.COSINE_SIMILARITY,
            output_var_type=DBVarType.FLOAT,
        )

    def get_value(self, key: Union[Scalar, Feature]) -> Feature:
        """
        Retrieve the value in a dictionary feature based on the key provided.

        This key could be either

        - a lookup feature, or

        - a scalar value.

        Parameters
        ----------
        key: Union[Scalar, Feature]
            key to lookup the value for

        Returns
        -------
        Feature
            A new Feature object

        Examples
        --------
        Create a new feature by getting the value for a particular key:

        >>> counts = fb.Feature.get("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.get_value("Chips et Tortillas")
        >>> new_feature.name = "Chips et Tortillas Value"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(pd.DataFrame([{"POINT_IN_TIME": "2022-04-15 10:00:00", "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0"}]))


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        '{"Chips et Tortillas":1,"Colas, Thés glacés et Sodas":3,"Crèmes et Chantilly":1,"Pains":1,"Œufs":1}'


        New feature:

        >>> df["Chips et Tortillas Value"].iloc[0]
        1
        """
        additional_node_params = {}
        if isinstance(key, type(self._feature_obj)):
            assert_is_lookup_feature(key.node_types_lineage)
        else:
            # We only need to assign value if we have been passed in a single scalar value.
            additional_node_params["value"] = key
        # construct operation structure of the get value node output
        op_struct = self._feature_obj.graph.extract_operation_structure(node=self._feature_obj.node)
        get_value_node = GetValueFromDictionaryNode(name="temp", parameters=additional_node_params)

        series_operator = DefaultSeriesBinaryOperator(self._feature_obj, key)
        return series_operator.operate(
            node_type=NodeType.GET_VALUE,
            output_var_type=get_value_node.derive_var_type([op_struct]),
            additional_node_params=additional_node_params,
        )

    def get_rank(self, key: Union[Scalar, Feature], descending: bool = False) -> Feature:
        """
        Compute the rank of a particular key in the dictionary feature. If multiple keys have the
        same value, these keys will have the same rank which is equal to the smallest rank among
        these keys.

        - a lookup feature, or

        - a scalar value.

        Parameters
        ----------
        key: Union[Scalar, Feature]
            Key to lookup the value for.
        descending: bool
            Defaults to ranking in ascending order. Set to true to rank in descending order.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------
        Create a new feature by computing the rank for a particular key:

        >>> counts = fb.Feature.get("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.get_rank("Chips et Tortillas")
        >>> new_feature.name = "Chips et Tortillas Rank"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(pd.DataFrame([{"POINT_IN_TIME": "2022-04-15 10:00:00", "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0"}]))


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        '{"Chips et Tortillas":1,"Colas, Thés glacés et Sodas":3,"Crèmes et Chantilly":1,"Pains":1,"Œufs":1}'


        New feature:

        >>> df["Chips et Tortillas Rank"].iloc[0]
        1.0
        """
        additional_node_params: Dict[str, Any] = {
            "descending": descending,
        }
        if isinstance(key, type(self._feature_obj)):
            assert_is_lookup_feature(key.node_types_lineage)
        else:
            # We only need to assign value if we have been passed in a single scalar value.
            additional_node_params["value"] = key

        series_operator = DefaultSeriesBinaryOperator(self._feature_obj, key)
        return series_operator.operate(
            node_type=NodeType.GET_RANK,
            output_var_type=DBVarType.FLOAT,
            additional_node_params=additional_node_params,
        )

    def get_relative_frequency(self, key: Union[Scalar, Feature]) -> Feature:
        """
        Compute the relative frequency of a particular key in the dictionary feature.

        This key could be either

        - a lookup feature, or

        - a scalar value.

        Parameters
        ----------
        key: Union[Scalar, Feature]
            Key to lookup the value for.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------

        Create a new feature by computing the relative frequency for a particular key:

        >>> counts = fb.Feature.get("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.get_relative_frequency("Chips et Tortillas")
        >>> new_feature.name = "Chips et Tortillas Relative Frequency"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(pd.DataFrame([{"POINT_IN_TIME": "2022-04-15 10:00:00", "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0"}]))


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        '{"Chips et Tortillas":1,"Colas, Thés glacés et Sodas":3,"Crèmes et Chantilly":1,"Pains":1,"Œufs":1}'


        New feature:

        >>> df["Chips et Tortillas Relative Frequency"].iloc[0]
        0.14285714285714302
        """
        additional_node_params = {}
        if isinstance(key, type(self._feature_obj)):
            assert_is_lookup_feature(key.node_types_lineage)
        else:
            # We only need to assign value if we have been passed in a single scalar value.
            additional_node_params["value"] = key

        series_operator = DefaultSeriesBinaryOperator(self._feature_obj, key)
        return series_operator.operate(
            node_type=NodeType.GET_RELATIVE_FREQUENCY,
            output_var_type=DBVarType.FLOAT,
            additional_node_params=additional_node_params,
        )
