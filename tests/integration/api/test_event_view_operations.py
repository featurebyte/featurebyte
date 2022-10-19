"""
This module contains session to EventView integration tests
"""
import numpy as np
import pandas as pd
import pytest

from featurebyte import AggFunc, EventData, EventView, FeatureList, SourceType, to_timedelta
from tests.util.helper import get_lagged_series_pandas


def test_query_object_operation_on_sqlite_source(
    sqlite_session, transaction_data, sqlite_feature_store
):
    """
    Test loading event view from sqlite source
    """
    _ = sqlite_session
    assert sqlite_feature_store.list_tables() == ["test_table"]

    sqlite_database_table = sqlite_feature_store.get_table(
        database_name=None,
        schema_name=None,
        table_name="test_table",
    )
    expected_dtypes = pd.Series(
        {
            "event_timestamp": "TIMESTAMP",
            "created_at": "INT",
            "cust_id": "INT",
            "user id": "INT",
            "product_action": "VARCHAR",
            "session_id": "INT",
            "amount": "FLOAT",
        }
    )
    pd.testing.assert_series_equal(expected_dtypes, sqlite_database_table.dtypes)

    event_data = EventData.from_tabular_source(
        tabular_source=sqlite_database_table,
        name="sqlite_event_data",
        event_timestamp_column="created_at",
    )
    event_view = EventView.from_event_data(event_data)
    assert event_view.columns == [
        "event_timestamp",
        "created_at",
        "cust_id",
        "user id",
        "product_action",
        "session_id",
        "amount",
    ]

    # need to specify the constant as float, otherwise results will get truncated
    event_view["cust_id_x_session_id"] = event_view["cust_id"] * event_view["session_id"] / 1000.0
    event_view["lucky_customer"] = event_view["cust_id_x_session_id"] > 140.0

    # construct expected results
    expected = transaction_data.copy()
    expected["cust_id_x_session_id"] = (expected["cust_id"] * expected["session_id"]) / 1000.0
    expected["lucky_customer"] = (expected["cust_id_x_session_id"] > 140.0).astype(int)

    # check agreement
    output = event_view.preview(limit=expected.shape[0])
    # sqlite returns str for timestamp columns, formatted up to %f precision with quirks
    expected["event_timestamp"] = expected["event_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    expected["event_timestamp"] = expected["event_timestamp"].str.replace(".000000", "")
    pd.testing.assert_frame_equal(output, expected[output.columns], check_dtype=False)


def iet_entropy(view, group_by_col, window, name):
    """Create feature to capture the entropy of inter-event interval time"""
    view = view.copy()
    ts_col = view[view.timestamp_column]
    a = view["a"] = (ts_col - ts_col.lag(group_by_col)).dt.day
    view["a * log(a)"] = a * (a + 0.1).log()  # add 0.1 to avoid log(0.0)
    b = view.groupby(group_by_col).aggregate(
        "a",
        method=AggFunc.SUM,
        windows=[window],
        feature_names=[f"sum(a) ({window})"],
    )[f"sum(a) ({window})"]

    feature = (
        view.groupby(group_by_col).aggregate(
            "a * log(a)",
            method=AggFunc.SUM,
            windows=[window],
            feature_names=["sum(a * log(a))"],
        )["sum(a * log(a))"]
        * -1
        / b
        + (b + 0.1).log()  # add 0.1 to avoid log(0.0)
    )

    feature.name = name
    return feature


def pyramid_sum(event_view, group_by_col, window, numeric_column, name):
    """Create a list of assign operations to check the pruning algorithm works properly"""
    column_num = 3
    columns = []
    event_view = event_view.copy()
    for i in range(1, column_num + 1):
        col_name = f"column_{i}"
        columns.append(col_name)
        event_view[col_name] = i * event_view[numeric_column]
        if i % 2 == 0:
            event_view[col_name] = (1 / i) * event_view[col_name]
        else:
            temp_col_name = f"{col_name}_tmp"
            event_view[temp_col_name] = (2 / i) * event_view[col_name]
            event_view[col_name] = 0.5 * event_view[temp_col_name]

    # construct a geometric series [1, 2, 4, ...]
    for r in range(column_num - 1):
        for idx in reversed(range(r, column_num - 1)):
            col_idx = idx + 1
            event_view[f"column_{col_idx+1}"] = (
                event_view[f"column_{col_idx}"] + event_view[f"column_{col_idx+1}"]
            )

    output = None
    for idx in range(column_num):
        col_idx = idx + 1
        feat = event_view.groupby(group_by_col).aggregate(
            f"column_{col_idx}", method="sum", windows=[window], feature_names=[f"column_{col_idx}"]
        )[f"column_{col_idx}"]
        if output is None:
            output = feat
        else:
            output = output + feat

    output.name = name
    return output


def assert_feature_preview_output_equal(actual, expected):
    """
    Check output of Feature / FeatureGroup / FeatureList preview is as expected
    """
    assert isinstance(actual, pd.DataFrame)
    assert actual.shape[0] == 1
    assert sorted(actual.keys()) == sorted(expected.keys())

    actual = actual.iloc[0].to_dict()

    # Databricks returned datetime columns always have timezone (UTC). Remove timezone so that the
    # actual and expected (no timezone) can be compared.
    actual["POINT_IN_TIME"] = actual["POINT_IN_TIME"].tz_localize(None)

    def _isnumeric(x):
        try:
            float(x)
        except (TypeError, ValueError):
            return False
        return True

    for k in actual:
        if _isnumeric(expected[k]):
            np.testing.assert_allclose(actual[k], expected[k], rtol=1e-5)
        else:
            assert actual[k] == expected[k]


@pytest.mark.parametrize("event_data", ["databricks", "snowflake"], indirect=True)
def test_query_object_operation(transaction_data_upper_case, event_data, feature_manager):
    """
    Test EventView operations for an EventData
    """
    source_type = event_data.feature_store.type
    count_dict_supported = source_type == SourceType.SNOWFLAKE

    # create event view
    event_view = EventView.from_event_data(event_data)
    assert event_view.columns == [
        "EVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "USER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "AMOUNT",
    ]

    # need to specify the constant as float, otherwise results will get truncated
    event_view["CUST_ID_X_SESSION_ID"] = event_view["CUST_ID"] * event_view["SESSION_ID"] / 1000.0
    event_view["LUCKY_CUSTOMER"] = event_view["CUST_ID_X_SESSION_ID"] > 140.0

    # apply more event view operations
    event_view["AMOUNT"].fillna(0)

    # check accessor operations
    check_string_operations(event_view, "PRODUCT_ACTION")
    check_datetime_operations(event_view, "EVENT_TIMESTAMP")

    # check casting operations
    check_cast_operations(event_view, source_type=source_type)

    # check numeric operations
    check_numeric_operations(event_view)

    # construct expected results
    expected = transaction_data_upper_case.copy()
    expected["CUST_ID_X_SESSION_ID"] = (expected["CUST_ID"] * expected["SESSION_ID"]) / 1000.0
    expected["LUCKY_CUSTOMER"] = (expected["CUST_ID_X_SESSION_ID"] > 140.0).astype(int)
    expected["AMOUNT"] = expected["AMOUNT"].fillna(0)

    # check agreement
    output = event_view.preview(limit=expected.shape[0])
    output["CUST_ID_X_SESSION_ID"] = output["CUST_ID_X_SESSION_ID"].astype(
        float
    )  # type is not correct here
    columns = [
        col for col in output.columns if not col.startswith("str_") and not col.startswith("dt_")
    ]
    pd.testing.assert_frame_equal(output[columns], expected[columns], check_dtype=False)

    # create some features
    event_view["derived_value_column"] = 1.0 * event_view["USER ID"]
    feature_group = event_view.groupby("USER ID").aggregate(
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_2h", "COUNT_24h"],
    )
    feature_group_per_category = event_view.groupby("USER ID", category="PRODUCT_ACTION").aggregate(
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_BY_ACTION_2h", "COUNT_BY_ACTION_24h"],
    )
    # add features based on transformations on count per category
    feature_counts_24h = feature_group_per_category["COUNT_BY_ACTION_24h"]
    feature_group_per_category["ENTROPY_BY_ACTION_24h"] = feature_counts_24h.cd.entropy()
    feature_group_per_category["MOST_FREQUENT_ACTION_24h"] = feature_counts_24h.cd.most_frequent()
    feature_group_per_category["NUM_UNIQUE_ACTION_24h"] = feature_counts_24h.cd.unique_count()
    feature_group_per_category[
        "NUM_UNIQUE_ACTION_24h_exclude_missing"
    ] = feature_counts_24h.cd.unique_count(include_missing=False)

    feature_counts_2h = feature_group_per_category["COUNT_BY_ACTION_2h"]
    feature_group_per_category[
        "ACTION_SIMILARITY_2h_to_24h"
    ] = feature_counts_2h.cd.cosine_similarity(feature_counts_24h)

    # preview the features
    preview_param = {
        "POINT_IN_TIME": "2001-01-02 10:00:00",
        "user id": 1,
    }

    # preview count features
    df_feature_preview = feature_group.preview(preview_param)
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "user id": 1,
            "COUNT_2h": 3,
            "COUNT_24h": 14,
        },
    )

    if count_dict_supported:
        # preview count per category features
        df_feature_preview = feature_group_per_category.preview(preview_param)
        assert df_feature_preview.shape[0] == 1
        assert df_feature_preview.iloc[0].to_dict() == {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "user id": 1,
            "COUNT_BY_ACTION_2h": '{\n  "add": 2,\n  "purchase": 1\n}',
            "COUNT_BY_ACTION_24h": '{\n  "__MISSING__": 1,\n  "add": 6,\n  "detail": 2,\n  "purchase": 4,\n  "remove": 1\n}',
            "ENTROPY_BY_ACTION_24h": 1.376055285260417,
            "MOST_FREQUENT_ACTION_24h": "add",
            "NUM_UNIQUE_ACTION_24h": 5,
            "NUM_UNIQUE_ACTION_24h_exclude_missing": 4,
            "ACTION_SIMILARITY_2h_to_24h": 0.9395523512235261,
        }

    # preview one feature only
    df_feature_preview = feature_group["COUNT_2h"].preview(preview_param)
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "user id": 1,
            "COUNT_2h": 3,
        },
    )

    # preview a not-yet-assigned feature
    new_feature = feature_group["COUNT_2h"] / feature_group["COUNT_24h"]
    df_feature_preview = new_feature.preview(preview_param)
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "user id": 1,
            "Unnamed": 0.2142857143,
        },
    )

    run_test_conditional_assign_feature(feature_group)

    # assign new feature and preview again
    feature_group["COUNT_2h / COUNT_24h"] = new_feature
    df_feature_preview = feature_group.preview(preview_param)
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "user id": 1,
            "COUNT_2h": 3,
            "COUNT_24h": 14,
            "COUNT_2h / COUNT_24h": 0.21428599999999998,
        },
    )

    # check casting on feature
    df_feature_preview = (
        (feature_group["COUNT_2h"].astype(int) + 1).astype(float).preview(preview_param)
    )
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "user id": 1,
            "Unnamed": 4.0,
        },
    )

    special_feature = create_feature_with_filtered_event_view(event_view)
    if source_type == SourceType.SNOWFLAKE:
        # should only save once since the feature names are the same
        special_feature.save()  # pylint: disable=no-member

    # add iet entropy
    feature_group["iet_entropy_24h"] = iet_entropy(
        event_view, "USER ID", window="24h", name="iet_entropy_24h"
    )
    feature_group["pyramid_sum_24h"] = pyramid_sum(
        event_view, "USER ID", window="24h", numeric_column="AMOUNT", name="pyramid_sum_24h"
    )
    feature_group["amount_sum_24h"] = event_view.groupby("USER ID").aggregate(
        "AMOUNT", method="sum", windows=["24h"], feature_names=["amount_sum_24h"]
    )["amount_sum_24h"]

    # preview a more complex feature group (multiple group by, some have the same tile_id)
    features = [
        feature_group["COUNT_2h"],
        feature_group["iet_entropy_24h"],
        feature_group["pyramid_sum_24h"],
        feature_group["amount_sum_24h"],
        special_feature,
    ]
    if count_dict_supported:
        features.append(feature_group_per_category["COUNT_BY_ACTION_24h"])

    feature_list_combined = FeatureList(
        features,
        name="My FeatureList",
    )
    feature_group_combined = feature_list_combined[feature_list_combined.feature_names]
    df_feature_preview = feature_group_combined.preview(preview_param)
    expected_amount_sum_24h = 220.18
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "user id": 1,
        "COUNT_2h": 2,
        "COUNT_BY_ACTION_24h": '{\n  "__MISSING__": 1,\n  "add": 6,\n  "detail": 2,\n  "purchase": 4,\n  "remove": 1\n}',
        "NUM_PURCHASE_7d": 4,
        "iet_entropy_24h": 0.6971221346393941,
        "pyramid_sum_24h": 7 * expected_amount_sum_24h,  # 1 + 2 + 4 = 7
        "amount_sum_24h": expected_amount_sum_24h,
    }
    if not count_dict_supported:
        expected.pop("COUNT_BY_ACTION_24h")
    assert_feature_preview_output_equal(df_feature_preview, expected)

    if source_type == SourceType.DATABRICKS:
        return

    # Check using a derived numeric column as category
    check_day_of_week_counts(event_view, preview_param)

    run_and_test_get_historical_features(feature_group, feature_group_per_category)


def create_feature_with_filtered_event_view(event_view):
    """
    Create a feature with filtered event view using string literal
    """
    event_view = event_view[event_view["PRODUCT_ACTION"] == "purchase"]
    feature_group = event_view.groupby("USER ID").aggregate(
        method="count",
        windows=["7d"],
        feature_names=["NUM_PURCHASE_7d"],
    )
    feature = feature_group["NUM_PURCHASE_7d"]
    return feature


def run_test_conditional_assign_feature(feature_group):
    """
    Test conditional assignment operations on Feature
    """
    feature_count_24h = feature_group["COUNT_24h"]
    preview_param = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "user id": 1,
    }
    result = feature_count_24h.preview(preview_param)
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_24h": 14})

    # Assign feature conditionally. Should be reflected in both Feature and FeatureGroup
    mask = feature_count_24h == 14.0
    feature_count_24h[mask] = 900
    result = feature_count_24h.preview(preview_param)
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_24h": 900})
    result = feature_group.preview(preview_param)
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_2h": 3, "COUNT_24h": 900})

    # Assign conditionally again (revert the above). Should be reflected in both Feature and
    # FeatureGroup
    mask = feature_count_24h == 900.0
    feature_count_24h[mask] = 14.0
    result = feature_count_24h.preview(preview_param)
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_24h": 14})
    result = feature_group.preview(preview_param)
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_2h": 3, "COUNT_24h": 14})

    # Assign to an unnamed Feature conditionally. Should not be reflected in Feature only and has no
    # effect on FeatureGroup
    temp_feature = feature_count_24h * 10
    mask = temp_feature == 140.0
    temp_feature[mask] = 900
    result = temp_feature.preview(preview_param)
    assert_feature_preview_output_equal(result, {**preview_param, "Unnamed": 900})
    result = feature_group.preview(preview_param)
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_2h": 3, "COUNT_24h": 14})

    # Assign to copied Series should not be reflected in FeatureGroup
    cloned_feature = feature_group["COUNT_24h"].copy()
    cloned_feature[cloned_feature == 14] = 0
    result = feature_group.preview(preview_param)
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_2h": 3, "COUNT_24h": 14})


def run_and_test_get_historical_features(feature_group, feature_group_per_category):
    """Test getting historical features from FeatureList"""
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"] * 5),
            "user id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    feature_list = FeatureList(
        [
            feature_group["COUNT_2h"],
            feature_group["COUNT_24h"],
            feature_group_per_category["COUNT_BY_ACTION_24h"],
            feature_group_per_category["ENTROPY_BY_ACTION_24h"],
            feature_group_per_category["MOST_FREQUENT_ACTION_24h"],
            feature_group_per_category["NUM_UNIQUE_ACTION_24h"],
            feature_group["COUNT_2h / COUNT_24h"],
            feature_group_per_category["ACTION_SIMILARITY_2h_to_24h"],
        ],
        name="My FeatureList",
    )
    df_historical_expected = pd.DataFrame(
        {
            "POINT_IN_TIME": df_training_events["POINT_IN_TIME"],
            "user id": df_training_events["user id"],
            "COUNT_2h": [3, 1, 1, 0, 0, 3, 0, 0, 1, 0],
            "COUNT_24h": [14, 12, 13, 11, 13, 18, 18, 13, 14, 0],
            "COUNT_BY_ACTION_24h": [
                '{\n  "__MISSING__": 1,\n  "add": 6,\n  "detail": 2,\n  "purchase": 4,\n  "remove": 1\n}',
                '{\n  "__MISSING__": 5,\n  "add": 1,\n  "detail": 2,\n  "remove": 4\n}',
                '{\n  "__MISSING__": 3,\n  "detail": 4,\n  "purchase": 4,\n  "remove": 2\n}',
                '{\n  "__MISSING__": 4,\n  "add": 5,\n  "detail": 1,\n  "purchase": 1\n}',
                '{\n  "__MISSING__": 2,\n  "add": 2,\n  "detail": 3,\n  "purchase": 4,\n  "remove": 2\n}',
                '{\n  "__MISSING__": 4,\n  "add": 4,\n  "detail": 2,\n  "purchase": 6,\n  "remove": 2\n}',
                '{\n  "__MISSING__": 3,\n  "add": 1,\n  "detail": 5,\n  "purchase": 6,\n  "remove": 3\n}',
                '{\n  "__MISSING__": 4,\n  "add": 1,\n  "detail": 1,\n  "purchase": 1,\n  "remove": 6\n}',
                '{\n  "__MISSING__": 3,\n  "add": 2,\n  "detail": 3,\n  "purchase": 2,\n  "remove": 4\n}',
                None,
            ],
            "ENTROPY_BY_ACTION_24h": [
                1.3760552852604169,
                1.236684869140504,
                1.3516811946858949,
                1.162225544921092,
                1.564957250242801,
                1.5229550675313184,
                1.4798484184768594,
                1.3114313374732374,
                1.5740973368489728,
                np.nan,
            ],
            "MOST_FREQUENT_ACTION_24h": [
                "add",
                "__MISSING__",
                "detail",
                "add",
                "purchase",
                "purchase",
                "purchase",
                "remove",
                "remove",
                None,
            ],
            "NUM_UNIQUE_ACTION_24h": [5.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 5.0, 0.0],
            "COUNT_2h / COUNT_24h": [
                0.214286,
                0.083333,
                0.076923,
                0.0,
                0.0,
                0.166667,
                0.0,
                0.0,
                0.071429,
                np.nan,  # Note: zero divide by zero
            ],
            "ACTION_SIMILARITY_2h_to_24h": [
                0.9395523512235255,
                0.5897678246195885,
                0.4472135954999579,
                np.nan,
                np.nan,
                0.8207826816681232,
                np.nan,
                np.nan,
                0.4629100498862757,
                np.nan,
            ],
        }
    )
    df_historical_features = feature_list.get_historical_features(df_training_events)
    # When using fetch_pandas_all(), the dtype of "USER ID" column is int8 (int64 otherwise)
    pd.testing.assert_frame_equal(df_historical_features, df_historical_expected, check_dtype=False)

    # Test again using the same feature list and data but with serving names mapping
    _test_get_historical_features_with_serving_names(
        feature_list, df_training_events, df_historical_expected
    )


def _test_get_historical_features_with_serving_names(
    feature_list, df_training_events, df_historical_expected
):
    """Test getting historical features from FeatureList with alternative serving names"""

    mapping = {"user id": "new_user id"}

    # Instead of providing the default serving name "user id", provide "new_user id" in data
    df_training_events = df_training_events.rename(mapping, axis=1)
    df_historical_expected = df_historical_expected.rename(mapping, axis=1)
    assert "new_user id" in df_training_events
    assert "new_user id" in df_historical_expected

    df_historical_features = feature_list.get_historical_features(
        df_training_events,
        serving_names_mapping=mapping,
    )
    pd.testing.assert_frame_equal(df_historical_features, df_historical_expected, check_dtype=False)


def check_string_operations(event_view, column_name, limit=100):
    """Test string operations"""
    event_view = event_view.copy()
    varchar_series = event_view[column_name]
    pandas_frame = varchar_series.preview(limit=limit)
    pandas_series = pandas_frame[pandas_frame.columns[0]]

    event_view["str_len"] = varchar_series.str.len()
    event_view["str_lower"] = varchar_series.str.lower()
    event_view["str_upper"] = varchar_series.str.upper()
    event_view["str_strip"] = varchar_series.str.strip("e")
    event_view["str_lstrip"] = varchar_series.str.lstrip("p")
    event_view["str_rstrip"] = varchar_series.str.rstrip("l")
    event_view["str_replace"] = varchar_series.str.replace("a", "i")
    event_view["str_pad"] = varchar_series.str.pad(10, side="both", fillchar="-")
    event_view["str_contains"] = varchar_series.str.contains("ai")
    event_view["str_slice"] = varchar_series.str[:5]

    str_columns = [col for col in event_view.columns if col.startswith("str_")]
    str_df = event_view[str_columns].preview(limit=limit)

    pd.testing.assert_series_equal(str_df["str_len"], pandas_series.str.len(), check_names=False)
    pd.testing.assert_series_equal(
        str_df["str_lower"], pandas_series.str.lower(), check_names=False
    )
    pd.testing.assert_series_equal(
        str_df["str_upper"], pandas_series.str.upper(), check_names=False
    )
    pd.testing.assert_series_equal(
        str_df["str_strip"], pandas_series.str.strip("e"), check_names=False
    )
    pd.testing.assert_series_equal(
        str_df["str_lstrip"], pandas_series.str.lstrip("p"), check_names=False
    )
    pd.testing.assert_series_equal(
        str_df["str_rstrip"], pandas_series.str.rstrip("l"), check_names=False
    )
    pd.testing.assert_series_equal(
        str_df["str_replace"],
        pandas_series.str.replace("a", "i"),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        str_df["str_pad"],
        pandas_series.str.pad(10, side="both", fillchar="-"),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        str_df["str_contains"],
        pandas_series.str.contains("ai"),
        check_names=False,
    )
    pd.testing.assert_series_equal(str_df["str_slice"], pandas_series.str[:5], check_names=False)


def check_datetime_operations(event_view, column_name, limit=100):
    """Test datetime operations"""
    event_view = event_view.copy()
    datetime_series = event_view[column_name]

    # add datetime extracted properties
    properties = [
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "day_of_week",
        "hour",
        "minute",
        "second",
    ]
    columns = []
    for prop in properties:
        name = f"dt_{prop}"
        event_view[name] = getattr(datetime_series.dt, prop)
        columns.append(name)

    # check timedelta constructed from date difference
    event_view["event_interval"] = datetime_series - datetime_series.lag("CUST_ID")
    event_view["event_interval_second"] = event_view["event_interval"].dt.second
    event_view["event_interval_hour"] = event_view["event_interval"].dt.hour
    event_view["event_interval_minute"] = event_view["event_interval"].dt.minute
    event_view["event_interval_microsecond"] = event_view["event_interval"].dt.microsecond

    # add timedelta constructed from to_timedelta
    timedelta = to_timedelta(event_view["event_interval_microsecond"].astype(int), "microsecond")
    event_view["timestamp_added"] = datetime_series + timedelta
    event_view["timestamp_added_from_timediff"] = datetime_series + event_view["event_interval"]
    event_view["timestamp_added_constant"] = datetime_series + pd.Timedelta("1d")
    event_view["timedelta_hour"] = timedelta.dt.hour

    # filter on event_interval
    event_view_filtered = event_view[event_view["event_interval_second"] > 500000]
    df_filtered = event_view_filtered.preview(limit=limit)
    assert (df_filtered["event_interval_second"] > 500000).all()

    # check datetime extracted properties
    dt_df = event_view.preview(limit=limit)
    pandas_series = dt_df[column_name]
    for prop in properties:
        series_prop = getattr(pandas_series.dt, prop)
        pd.testing.assert_series_equal(
            dt_df[f"dt_{prop}"],
            series_prop,
            check_names=False,
            check_dtype=False,
        )

    # check timedelta extracted properties
    pandas_previous_timestamp = get_lagged_series_pandas(
        dt_df, "EVENT_TIMESTAMP", "EVENT_TIMESTAMP", "CUST_ID"
    )
    pandas_event_interval_second = (pandas_series - pandas_previous_timestamp).dt.total_seconds()
    pandas_event_interval_minute = (
        pandas_series - pandas_previous_timestamp
    ).dt.total_seconds() / 60
    pandas_event_interval_hour = (
        pandas_series - pandas_previous_timestamp
    ).dt.total_seconds() / 3600
    pd.testing.assert_series_equal(
        dt_df["event_interval"].astype(float), pandas_event_interval_second, check_names=False
    )
    pd.testing.assert_series_equal(
        dt_df["event_interval_second"].astype(float),
        pandas_event_interval_second,
        check_names=False,
    )
    pd.testing.assert_series_equal(
        dt_df["event_interval_minute"].astype(float),
        pandas_event_interval_minute,
        check_names=False,
    )
    pd.testing.assert_series_equal(
        dt_df["event_interval_hour"].astype(float), pandas_event_interval_hour, check_names=False
    )
    # check date increment by timedelta
    pandas_timestamp_added = pandas_series + pd.to_timedelta(
        dt_df["event_interval_microsecond"].astype(float), "microsecond"
    )
    pandas_timestamp_added_constant = pandas_series + pd.Timedelta("1d")
    pd.testing.assert_series_equal(
        dt_df["timestamp_added"],
        pandas_timestamp_added,
        check_names=False,
    )
    pd.testing.assert_series_equal(
        dt_df["timestamp_added_from_timediff"],
        pandas_timestamp_added,
        check_names=False,
    )
    pd.testing.assert_series_equal(
        dt_df["timestamp_added_constant"],
        pandas_timestamp_added_constant,
        check_names=False,
    )
    pandas_timedelta_hour = (
        pd.to_timedelta(
            dt_df["event_interval_microsecond"].astype(float), unit="microsecond"
        ).dt.total_seconds()
        / 3600
    )
    pd.testing.assert_series_equal(
        dt_df["timedelta_hour"].astype(float), pandas_timedelta_hour, check_names=False
    )


def check_cast_operations(event_view, source_type, limit=100):
    """Check casting operations"""
    event_view = event_view.copy()
    event_view["AMOUNT_INT"] = event_view["AMOUNT"].astype(int)
    event_view["AMOUNT_STR"] = event_view["AMOUNT"].astype(str)
    event_view["AMOUNT_FLOAT"] = event_view["AMOUNT"].astype(float)
    event_view["INT_FROM_BOOL"] = (event_view["AMOUNT"] > 50).astype(int)
    event_view["FLOAT_FROM_BOOL"] = (event_view["AMOUNT"] > 50).astype(float)
    df = event_view.preview(limit=limit)

    # compare string representation to make sure that the values are converted to int rather than
    # just being floored ("2" instead of "2.0")
    expected = df["AMOUNT"].astype(int).astype(str).tolist()
    assert df["AMOUNT_INT"].astype(str).tolist() == expected

    if source_type == SourceType.SNOWFLAKE:
        pd.testing.assert_series_equal(
            df["AMOUNT_STR"],
            df["AMOUNT"].astype(str).apply(lambda x: "0" if x == "0.0" else x),
            check_names=False,
        )
    else:
        pd.testing.assert_series_equal(
            df["AMOUNT_STR"], df["AMOUNT"].astype(str), check_names=False
        )

    pd.testing.assert_series_equal(
        df["AMOUNT_FLOAT"], df["AMOUNT"].astype(float), check_names=False
    )

    assert df["INT_FROM_BOOL"].tolist() == (df["AMOUNT"] > 50).astype(int).tolist()
    assert df["FLOAT_FROM_BOOL"].tolist() == (df["AMOUNT"] > 50).astype(float).tolist()


def check_numeric_operations(event_view, limit=100):
    """Check casting operations"""
    event_view = event_view.copy()

    event_view["AMOUNT_ABS"] = (event_view["AMOUNT"] * (-1)).abs()
    event_view["AMOUNT_SQRT"] = event_view["AMOUNT"].sqrt()
    event_view["AMOUNT_POW_2"] = event_view["AMOUNT"].pow(2)
    event_view["AMOUNT_FLOOR"] = event_view["AMOUNT"].floor()
    event_view["AMOUNT_CEIL"] = event_view["AMOUNT"].ceil()
    event_view["AMOUNT_INT_MOD_5"] = event_view["AMOUNT"].astype(int) % 5
    event_view["AMOUNT_LOG"] = (event_view["AMOUNT"] + 1).log()
    event_view["AMOUNT_LOG_EXP"] = event_view["AMOUNT_LOG"].exp()
    df = event_view.preview(limit=limit)

    pd.testing.assert_series_equal(df["AMOUNT_ABS"], (df["AMOUNT"] * (-1)).abs(), check_names=False)
    pd.testing.assert_series_equal(df["AMOUNT_SQRT"], np.sqrt(df["AMOUNT"]), check_names=False)
    pd.testing.assert_series_equal(df["AMOUNT_POW_2"], df["AMOUNT"].pow(2), check_names=False)
    pd.testing.assert_series_equal(
        df["AMOUNT_FLOOR"], np.floor(df["AMOUNT"]), check_names=False, check_dtype=False
    )
    pd.testing.assert_series_equal(
        df["AMOUNT_CEIL"], np.ceil(df["AMOUNT"]), check_names=False, check_dtype=False
    )
    pd.testing.assert_series_equal(
        df["AMOUNT_INT_MOD_5"].astype(int), df["AMOUNT"].astype(int) % 5, check_names=False
    )
    pd.testing.assert_series_equal(df["AMOUNT_LOG"], np.log(df["AMOUNT"] + 1), check_names=False)
    pd.testing.assert_series_equal(
        df["AMOUNT_LOG_EXP"], np.exp(np.log(df["AMOUNT"] + 1)), check_names=False
    )


def check_day_of_week_counts(event_view, preview_param):
    """Check using derived numeric column as category"""
    event_view["event_day_of_week"] = event_view["EVENT_TIMESTAMP"].dt.day_of_week
    day_of_week_counts = event_view.groupby("USER ID", category="event_day_of_week").aggregate(
        method="count",
        windows=["24h"],
        feature_names=["DAY_OF_WEEK_COUNTS_24h"],
    )
    day_of_week_counts["DAY_OF_WEEK_ENTROPY_24h"] = day_of_week_counts[
        "DAY_OF_WEEK_COUNTS_24h"
    ].cd.entropy()
    df_feature_preview = day_of_week_counts.preview(
        preview_param,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "user id": 1,
        "DAY_OF_WEEK_COUNTS_24h": '{\n  "0": 9,\n  "1": 5\n}',
        "DAY_OF_WEEK_ENTROPY_24h": 0.651756561172653,
    }
