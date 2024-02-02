"""This is a test module for null filling value extractor."""
from featurebyte import RequestColumn
from tests.util.helper import check_null_filling_value


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
