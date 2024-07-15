"""
This module functions used to patch the Feast library.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Union

from collections import defaultdict

import pandas as pd

# pylint: disable=no-name-in-module
from feast import OnDemandFeatureView
from feast.base_feature_view import BaseFeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import FieldStatus, GetOnlineFeaturesResponse
from feast.type_map import python_values_to_proto_values
from google.protobuf.timestamp_pb2 import Timestamp  # type: ignore[import]


def augment_response_with_on_demand_transforms(
    online_features_response: GetOnlineFeaturesResponse,
    feature_refs: List[str],
    requested_on_demand_feature_views: List[OnDemandFeatureView],
    full_feature_names: bool,
) -> None:
    """
    The main difference between this and the original Feast implementation is that this function explicitly
    casts the transformed feature values to the expected data type. This is necessary because the
    original implementation attempts to infer the data type from the values, which can lead to incorrect
    data types being used.

    Parameters
    ----------
    online_features_response: GetOnlineFeaturesResponse
        Protobuf object to populate
    feature_refs: List[str]
        List of all feature references to be returned.
    requested_on_demand_feature_views: List[OnDemandFeatureView]
        List of all odfvs that have been requested.
    full_feature_names: bool
        A boolean that provides the option to add the feature view prefixes to the feature names,
        changing them from the format "feature" to "feature_view__feature" (e.g., "daily_transactions" changes to
        "customer_fv__daily_transactions").
    """
    # pylint: disable=too-many-locals
    requested_odfv_map = {odfv.name: odfv for odfv in requested_on_demand_feature_views}
    requested_odfv_feature_names = requested_odfv_map.keys()

    odfv_feature_refs = defaultdict(list)
    for feature_ref in feature_refs:
        view_name, feature_name = feature_ref.split(":")
        if view_name in requested_odfv_feature_names:
            odfv_feature_refs[view_name].append(
                f"{requested_odfv_map[view_name].projection.name_to_use()}__{feature_name}"
                if full_feature_names
                else feature_name
            )

    initial_response = OnlineResponse(online_features_response)
    initial_response_df = initial_response.to_df(include_event_timestamps=True)

    # Apply on demand transformations and augment the result rows
    odfv_result_names = set()
    for odfv_name, _feature_refs in odfv_feature_refs.items():
        odfv = requested_odfv_map[odfv_name]
        transformed_features_df = odfv.get_transformed_features_df(
            initial_response_df,
            full_feature_names,
        )
        selected_subset = [f for f in transformed_features_df.columns if f in _feature_refs]
        # this is an additional step introduced to extract the correct dtypes for the transformed features
        odfv_dtype_map = {
            (
                f"{odfv.projection.name_to_use()}__{feature.name}"
                if full_feature_names
                else feature.name
            ): feature.dtype.to_value_type()
            for feature in odfv.features
        }

        # pass the expected dtypes to the proto_values_to_proto_values function
        # (original implementation pass UNKNOWN as the dtype and let the function infer the dtype)
        proto_values = [
            python_values_to_proto_values(
                transformed_features_df[feature].values, odfv_dtype_map[feature]
            )
            for feature in selected_subset
        ]

        odfv_result_names |= set(selected_subset)

        online_features_response.metadata.feature_names.val.extend(selected_subset)
        for feature_idx in range(len(selected_subset)):
            online_features_response.results.append(
                GetOnlineFeaturesResponse.FeatureVector(
                    values=proto_values[feature_idx],
                    statuses=[FieldStatus.PRESENT] * len(proto_values[feature_idx]),
                    event_timestamps=[Timestamp()] * len(proto_values[feature_idx]),
                )
            )


class DataFrameWrapper(pd.DataFrame):
    """
    Wrapper class for pandas DataFrame to support alias column names. This feature is used to improve the
    runtime & memory performance of the get_transformed_features_df function.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.attrs["_alias"] = {}

    def add_column_alias(self, column_name: str, alias: str) -> None:
        """
        Add a column alias

        Parameters
        ----------
        column_name: str
            Column name
        alias: str
            Alias name of the column
        """
        self.attrs["_alias"][alias] = column_name

    def __getitem__(self, key: Any) -> Union[pd.Series, pd.DataFrame]:
        if not isinstance(key, str) and isinstance(key, Iterable):
            return pd.DataFrame({_key: self.__getitem__(_key) for _key in key})

        if isinstance(key, str) and key in self.attrs["_alias"]:
            key = self.attrs["_alias"][key]

        return super().__getitem__(key)


def get_transformed_features_df(
    feature_view: OnDemandFeatureView,
    df_with_features: pd.DataFrame,
    full_feature_names: bool = False,
) -> pd.DataFrame:
    """
    The main difference between this and the original Feast implementation is that an extended pandas
    DataFrame with alias column names support is used to improve the runtime & memory performance of the
    function.

    Parameters
    ----------
    feature_view: OnDemandFeatureView
        OnDemandFeatureView object
    df_with_features: pd.DataFrame
        Dataframe with features
    full_feature_names: bool
        A boolean that provides the option to add the feature view prefixes to the feature names,

    Returns
    -------
    pd.DataFrame
        Dataframe with transformed features
    """
    # Original implementation assigns a new column on each iteration, this implementation uses a wrapper
    # class to improve the runtime & memory performance.
    df_with_features = DataFrameWrapper(df_with_features)
    for source_fv_projection in feature_view.source_feature_view_projections.values():
        for feature in source_fv_projection.features:
            full_feature_ref = f"{source_fv_projection.name}__{feature.name}"
            if full_feature_ref in df_with_features.keys():
                # Make sure the partial feature name is always present
                df_with_features.add_column_alias(full_feature_ref, feature.name)
            elif feature.name in df_with_features.keys():
                # Make sure the full feature name is always present
                df_with_features.add_column_alias(feature.name, full_feature_ref)

    # Compute transformed values and apply to each result row
    df_with_transformed_features = feature_view.udf(df_with_features)

    # Work out whether the correct columns names are used.
    rename_columns: Dict[str, str] = {}
    for feature in feature_view.features:
        short_name = feature.name
        long_name = f"{feature_view.projection.name_to_use()}__{feature.name}"
        if short_name in df_with_transformed_features.columns and full_feature_names:
            rename_columns[short_name] = long_name
        elif not full_feature_names:
            # Long name must be in dataframe.
            rename_columns[long_name] = short_name

    return pd.DataFrame(df_with_transformed_features).rename(columns=rename_columns)


def _hash_feature(feature: Field) -> int:
    """
    Returns a hash value for the given feature field.

    Parameters
    ----------
    feature: Field
        The feature field to hash.

    Returns
    -------
    int
    """
    return hash(
        (
            feature.name,
            hash(feature.dtype),
            hash(feature.description),
            hash(frozenset(feature.tags.items())),
        )
    )


def with_projection(
    feature_view: BaseFeatureView, feature_view_projection: FeatureViewProjection
) -> Any:
    """
    Returns a copy of this base feature view with the feature view projection set to
    the given projection.

    Parameters
    ----------
    feature_view: BaseFeatureView
        The base feature view to copy.
    feature_view_projection: FeatureViewProjection
        The feature view projection to assign to the copy.

    Returns
    -------
    Any

    Raises
    -------
    ValueError
        The name or features of the projection do not match.
    """
    if feature_view_projection.name != feature_view.name:
        raise ValueError(
            f"The projection for the {feature_view.name} FeatureView cannot be applied because it differs in name. "
            f"The projection is named {feature_view_projection.name} and the name indicates which "
            "FeatureView the projection is for."
        )

    feature_view_hashes = {_hash_feature(feat) for feat in feature_view.features}
    for feature in feature_view_projection.features:
        if _hash_feature(feature) not in feature_view_hashes:
            raise ValueError(
                f"The projection for {feature_view.name} cannot be applied because it contains {feature.name} "
                f"which the FeatureView doesn't have."
            )

    # pylint: disable=invalid-name,unnecessary-dunder-call
    cp = feature_view.__copy__()
    cp.projection = feature_view_projection

    return cp
