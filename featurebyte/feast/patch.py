"""
This module functions used to patch the Feast library.
"""
from __future__ import annotations

from typing import List

from collections import defaultdict

# pylint: disable=no-name-in-module
from feast import OnDemandFeatureView
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
            f"{odfv.projection.name_to_use()}__{feature.name}"
            if full_feature_names
            else feature.name: feature.dtype.to_value_type()
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
