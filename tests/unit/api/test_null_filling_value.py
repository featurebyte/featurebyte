"""This is a test module for null filling value extractor."""

import textwrap

import freezegun
import pytest

from featurebyte import RequestColumn
from tests.util.helper import check_null_filling_value, deploy_features_through_api


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(
    patched_catalog_get_create_payload,
    mock_deployment_flow,
):
    """Enable feast integration & patch catalog ID for all tests in this module"""
    _ = patched_catalog_get_create_payload, mock_deployment_flow
    yield


def test_feature_without_null_filling_value(
    float_feature,
    latest_event_timestamp_feature,
    non_time_based_feature,
):
    """Test feature null filling value."""
    for feature in [float_feature, latest_event_timestamp_feature, non_time_based_feature]:
        check_null_filling_value(feature.graph, feature.node_name, None)


def test_feature_with_null_filling_value(
    float_feature, latest_event_timestamp_feature, non_time_based_feature
):
    """Test feature null filling value."""
    original_float_feature = float_feature.copy()

    # all the null values are filled with 0
    float_feature.fillna(0)
    check_null_filling_value(float_feature.graph, float_feature.node_name, 0)

    # apply post-processing to the null filled feature
    float_feature = 3 * (float_feature + 7)
    check_null_filling_value(float_feature.graph, float_feature.node_name, 21)

    # add null filled feature with non-null filled feature
    another_feature = float_feature + original_float_feature
    check_null_filling_value(another_feature.graph, another_feature.node_name, None)

    # construct a feature with request column
    day_diff_feat = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    check_null_filling_value(day_diff_feat.graph, day_diff_feat.node_name, None)

    # fill null values with 0
    day_diff_feat.fillna(0)
    check_null_filling_value(day_diff_feat.graph, day_diff_feat.node_name, 0)

    # check on non-time based feature
    non_time_based_feature.fillna(0)
    check_null_filling_value(non_time_based_feature.graph, non_time_based_feature.node_name, 0)


@freezegun.freeze_time("2024-02-01")
def test_feature_with_null_filling_value_has_odfv(float_feature, non_time_based_feature):
    """Test feature null filling value."""
    float_feature.fillna(0)
    float_feature.save()
    non_time_based_feature.fillna(0)
    non_time_based_feature.save()
    complex_feature = float_feature + non_time_based_feature
    complex_feature.name = "complex_feature"
    complex_feature.save()

    deploy_features_through_api([float_feature, non_time_based_feature, complex_feature])

    odfv_info = float_feature.cached_model.offline_store_info.odfv_info
    assert (
        odfv_info.codes.strip()
        == textwrap.dedent(
            f"""
        import datetime
        import json
        import numpy as np
        import pandas as pd
        import scipy as sp


        def odfv_sum_1d_v240201_{float_feature.id}(
            inputs: pd.DataFrame,
        ) -> pd.DataFrame:
            df = pd.DataFrame()
            df["sum_1d_V240201"] = inputs["sum_1d_V240201"].fillna(0)
            # Time-to-live (TTL) handling to clean up expired data
            request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
            cutoff = request_time - pd.Timedelta(seconds=3600)
            feature_timestamp = pd.to_datetime(
                inputs["sum_1d_V240201__ts"], unit="s", utc=True
            )
            mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
            inputs.loc[~mask, "sum_1d_V240201"] = np.nan
            df["sum_1d_V240201"] = inputs["sum_1d_V240201"]
            df.fillna(np.nan, inplace=True)
            return df
        """
        ).strip()
    )

    odfv_info = non_time_based_feature.cached_model.offline_store_info.odfv_info
    assert (
        odfv_info.codes.strip()
        == textwrap.dedent(
            f"""
        import datetime
        import json
        import numpy as np
        import pandas as pd
        import scipy as sp


        def odfv_non_time_time_sum_amount_feature_v240201_{non_time_based_feature.id}(
            inputs: pd.DataFrame,
        ) -> pd.DataFrame:
            df = pd.DataFrame()
            df["non_time_time_sum_amount_feature_V240201"] = inputs[
                "non_time_time_sum_amount_feature_V240201"
            ].fillna(0)
            return df
        """
        ).strip()
    )

    odfv_info = complex_feature.cached_model.offline_store_info.odfv_info
    assert (
        odfv_info.codes.strip()
        == textwrap.dedent(
            f"""
        import datetime
        import json
        import numpy as np
        import pandas as pd
        import scipy as sp


        def odfv_complex_feature_v240201_{complex_feature.id}(
            inputs: pd.DataFrame,
        ) -> pd.DataFrame:
            df = pd.DataFrame()
            feat = inputs["__complex_feature_V240201__part1"].fillna(0)
            feat_1 = inputs["__complex_feature_V240201__part0"].fillna(0)

            # TTL handling for __complex_feature_V240201__part0 column
            request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
            cutoff = request_time - pd.Timedelta(seconds=3600)
            feat_ts = pd.to_datetime(
                inputs["__complex_feature_V240201__part0__ts"], utc=True, unit="s"
            )
            mask = (feat_ts >= cutoff) & (feat_ts <= request_time)
            inputs.loc[~mask, "__complex_feature_V240201__part0"] = np.nan
            feat_2 = pd.Series(
                np.where(pd.isna(feat_1) | pd.isna(feat), np.nan, feat_1 + feat),
                index=feat_1.index,
            )
            df["complex_feature_V240201"] = feat_2
            return df
        """
        ).strip()
    )
