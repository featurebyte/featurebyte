"""
This module contains count_dict accessor class
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, TypeVar, Union

from typeguard import typechecked

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.series import DefaultSeriesBinaryOperator
from featurebyte.core.util import SeriesBinaryOperator, series_unary_operation
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.count_dict import GetValueFromDictionaryNode
from featurebyte.typing import Scalar, is_scalar

if TYPE_CHECKING:
    from featurebyte.api.feature import Feature
    from featurebyte.api.request_column import RequestColumn
else:
    Feature = TypeVar("Feature")
    RequestColumn = TypeVar("RequestColumn")


KeyType = Union[Union[Scalar, Feature, RequestColumn]]


class CdAccessorMixin:
    """
    CdAccessorMixin class
    """

    @property
    def cd(self: Feature) -> CountDictAccessor:  # type: ignore
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

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Series")

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
        )

    def entropy(self) -> Feature:
        """
        Computes the entropy of the Cross Aggregate feature over the feature keys. The values are normalized to sum to
        1 and used as the probability of each key in the entropy calculation.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------

        Create a new feature by calculating the entropy of the dictionary feature:

        >>> counts = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.entropy()
        >>> new_feature.name = "CustomerProductGroupCountsEntropy_7d"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        {'Chips et Tortillas': 1, 'Colas, Thés glacés et Sodas': 3, 'Crèmes et Chantilly': 1, 'Pains': 1, 'Œufs': 1}


        New feature:

        >>> df["CustomerProductGroupCountsEntropy_7d"].iloc[0]
        1.475076311054695
        """
        return self._make_operation("entropy", DBVarType.FLOAT)

    def most_frequent(self) -> Feature:
        """
        Retrieves the most frequent key in the Cross Aggregate feature. When there are ties, the
        lexicographically smallest key is returned.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------

        Create a new feature by retrieving the most frequent key of the dictionary feature:

        >>> counts = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.most_frequent()
        >>> new_feature.name = "CustomerProductGroupCountsMostFrequent_7d"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        {'Chips et Tortillas': 1, 'Colas, Thés glacés et Sodas': 3, 'Crèmes et Chantilly': 1, 'Pains': 1, 'Œufs': 1}


        New feature:

        >>> df["CustomerProductGroupCountsMostFrequent_7d"].iloc[0]
        'Colas, Thés glacés et Sodas'
        """
        return self._make_operation("most_frequent", DBVarType.VARCHAR)

    def key_with_highest_value(self) -> Feature:
        """
        Retrieves the key with the highest value in the Cross Aggregate feature. When there are
        ties, the lexicographically smallest key is returned.

        This is an alias for `most_frequent()`.

        Returns
        -------
        Feature
            A new feature object.

        Examples
        --------

        Create a new feature by retrieving the key with the highest value of the dictionary feature:

        >>> counts = catalog.get_feature("CustomerProductGroupTotalCost_7d")
        >>> new_feature = counts.cd.key_with_highest_value()
        >>> new_feature.name = "CustomerProductGroupWithHighestTotalCost_7d"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature:

        >>> df["CustomerProductGroupTotalCost_7d"].iloc[0]
        {'Chips et Tortillas': 2.0, 'Colas, Thés glacés et Sodas': 10.0, 'Crèmes et Chantilly': 0.75, 'Pains': 1.09, 'Œufs': 1.19}

        New feature:

        >>> df["CustomerProductGroupWithHighestTotalCost_7d"].iloc[0]
        'Colas, Thés glacés et Sodas'
        """
        return self._make_operation("key_with_highest_value", DBVarType.VARCHAR)

    def key_with_lowest_value(self) -> Feature:
        """
        Retrieves the key with the lowest value in the Cross Aggregate feature. When there are
        ties, the lexicographically smallest key is returned.

        Returns
        -------
        Feature
            A new feature object.

        Examples
        --------

        Create a new feature by retrieving the key with the lowest value of the dictionary feature:

        >>> counts = catalog.get_feature("CustomerProductGroupTotalCost_7d")
        >>> new_feature = counts.cd.key_with_lowest_value()
        >>> new_feature.name = "CustomerProductGroupWithLowestTotalCost_7d"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature:

        >>> df["CustomerProductGroupTotalCost_7d"].iloc[0]
        {'Chips et Tortillas': 2.0, 'Colas, Thés glacés et Sodas': 10.0, 'Crèmes et Chantilly': 0.75, 'Pains': 1.09, 'Œufs': 1.19}

        New feature:

        >>> df["CustomerProductGroupWithLowestTotalCost_7d"].iloc[0]
        'Crèmes et Chantilly'
        """
        return self._make_operation("key_with_lowest_value", DBVarType.VARCHAR)

    @typechecked()
    def unique_count(self, include_missing: bool = True) -> Feature:
        """
        Computes number of distinct keys in a Cross Aggregate feature.

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

        >>> counts = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.unique_count()
        >>> new_feature.name = "CustomerProductGroupCountsUniqueCount_7d"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        {'Chips et Tortillas': 1, 'Colas, Thés glacés et Sodas': 3, 'Crèmes et Chantilly': 1, 'Pains': 1, 'Œufs': 1}


        New feature:

        >>> df["CustomerProductGroupCountsUniqueCount_7d"].iloc[0]
        5.0
        """
        return self._make_operation(
            "unique_count",
            DBVarType.FLOAT,
            additional_params={"include_missing": include_missing},
        )

    def cosine_similarity(self, other: Feature) -> Feature:
        """
        Computes the cosine similarity with another Cross Aggregate feature.

        Parameters
        ----------
        other : Feature
            Another dictionary feature.

        Returns
        -------
        Feature
            Another Cross Aggregate feature.

        Examples
        --------
        Create a similarity feature between two dictionary features:

        >>> feature_1 = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> feature_2 = catalog.get_feature("CustomerProductGroupCounts_90d")
        >>> similarity = feature_1.cd.cosine_similarity(feature_2)
        >>> similarity.name = "CustomerProductGroupCounts_7d_90d_similarity"


        Preview the features:

        >>> features = fb.FeatureGroup([feature_1, feature_2, similarity])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature 1:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
         {'Chips et Tortillas': 1, 'Colas, Thés glacés et Sodas': 3, 'Crèmes et Chantilly': 1, 'Pains': 1, 'Œufs': 1}


        Dictionary feature 2:

        >>> df["CustomerProductGroupCounts_90d"].iloc[0]
        {'Biscuits apéritifs': 1, 'Biscuits': 1, 'Bonbons': 1, 'Chips et Tortillas': 2, 'Colas, Thés glacés et Sodas': 12, 'Confitures': 1, 'Crèmes et Chantilly': 2, 'Céréales': 1, 'Emballages et sacs': 1, 'Fromages': 3, 'Glaces et Sorbets': 1, 'Glaçons': 1, 'Laits': 4, 'Noix': 1, 'Pains': 4, 'Petit-déjeuner': 2, 'Viande Surgelée': 1, 'Œufs': 1}


        Similarity feature:

        >>> df["CustomerProductGroupCounts_7d_90d_similarity"].iloc[0]
        0.8653846153846161
        """
        series_operator = CountDictSeriesOperator(self._feature_obj, other)
        return series_operator.operate(
            node_type=NodeType.COSINE_SIMILARITY,
            output_var_type=DBVarType.FLOAT,
        )

    def get_value(self, key: KeyType) -> Feature:
        """
        Retrieves the value of a specific key in the Cross Aggregate feature. The key may either be a
        lookup feature or a scalar value.

        Parameters
        ----------
        key: KeyType
            key to lookup the value for

        Returns
        -------
        Feature
            A new Feature object

        Examples
        --------
        Create a new feature by getting the value for a particular key:

        >>> counts = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.get_value("Chips et Tortillas")
        >>> new_feature.name = "Chips et Tortillas Value"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        {'Chips et Tortillas': 1, 'Colas, Thés glacés et Sodas': 3, 'Crèmes et Chantilly': 1, 'Pains': 1, 'Œufs': 1}


        New feature:

        >>> df["Chips et Tortillas Value"].iloc[0]
        1
        """
        additional_node_params = {}
        if is_scalar(key):
            # We only need to assign value if we have been passed in a single scalar value.
            additional_node_params["value"] = key
        # construct operation structure of the get value node output
        op_struct = self._feature_obj.graph.extract_operation_structure(
            node=self._feature_obj.node, keep_all_source_columns=True
        )
        get_value_node = GetValueFromDictionaryNode(name="temp", parameters=additional_node_params)

        series_operator = DefaultSeriesBinaryOperator(self._feature_obj, key)
        output_dtype_info = get_value_node.derive_dtype_info([op_struct])
        return series_operator.operate(
            node_type=NodeType.GET_VALUE,
            output_var_type=output_dtype_info.dtype,
            additional_node_params=additional_node_params,
        )

    def get_rank(self, key: KeyType, descending: bool = False) -> Feature:
        """
        Computes the rank of a specific key in the Cross Aggregate feature. If there are multiple keys with the same
        value, those keys will have the same rank, which equals the smallest rank among those keys. The key that is
        used for looking up the rank may either be a lookup feature or a scalar value.

        Parameters
        ----------
        key: KeyType
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

        >>> counts = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.get_rank("Chips et Tortillas")
        >>> new_feature.name = "Chips et Tortillas Rank"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        {'Chips et Tortillas': 1, 'Colas, Thés glacés et Sodas': 3, 'Crèmes et Chantilly': 1, 'Pains': 1, 'Œufs': 1}


        New feature:

        >>> df["Chips et Tortillas Rank"].iloc[0]
        1.0
        """
        additional_node_params: Dict[str, Any] = {
            "descending": descending,
        }
        if is_scalar(key):
            # We only need to assign value if we have been passed in a single scalar value.
            additional_node_params["value"] = key

        series_operator = DefaultSeriesBinaryOperator(self._feature_obj, key)
        return series_operator.operate(
            node_type=NodeType.GET_RANK,
            output_var_type=DBVarType.FLOAT,
            additional_node_params=additional_node_params,
        )

    def get_relative_frequency(self, key: KeyType) -> Feature:
        """
        Computes the relative frequency of a specific key in the Cross Aggregate feature. The key
        may either be a lookup feature or a scalar value. If the key does not exist, the relative
        frequency will be 0.

        Parameters
        ----------
        key: KeyType
            Key to lookup the value for.

        Returns
        -------
        Feature
            A new Feature object.

        Examples
        --------

        Create a new feature by computing the relative frequency for a particular key:

        >>> counts = catalog.get_feature("CustomerProductGroupCounts_7d")
        >>> new_feature = counts.cd.get_relative_frequency("Chips et Tortillas")
        >>> new_feature.name = "Chips et Tortillas Relative Frequency"


        Preview the features:

        >>> features = fb.FeatureGroup([counts, new_feature])
        >>> df = features.preview(
        ...     pd.DataFrame([
        ...         {
        ...             "POINT_IN_TIME": "2022-04-15 10:00:00",
        ...             "GROCERYCUSTOMERGUID": "2f4c1578-29d6-44b7-83da-7c5bfb981fa0",
        ...         }
        ...     ])
        ... )


        Dictionary feature:

        >>> df["CustomerProductGroupCounts_7d"].iloc[0]
        {'Chips et Tortillas': 1, 'Colas, Thés glacés et Sodas': 3, 'Crèmes et Chantilly': 1, 'Pains': 1, 'Œufs': 1}


        New feature:

        >>> df["Chips et Tortillas Relative Frequency"].iloc[0]
        0.14285714285714302
        """
        additional_node_params = {}
        if is_scalar(key):
            # We only need to assign value if we have been passed in a single scalar value.
            additional_node_params["value"] = key

        series_operator = DefaultSeriesBinaryOperator(self._feature_obj, key)
        return series_operator.operate(
            node_type=NodeType.GET_RELATIVE_FREQUENCY,
            output_var_type=DBVarType.FLOAT,
            additional_node_params=additional_node_params,
        )
