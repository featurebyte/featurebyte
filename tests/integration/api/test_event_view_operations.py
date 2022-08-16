"""
This module contains session to EventView integration tests
"""
from decimal import Decimal

import numpy as np
import pandas as pd

from featurebyte.api.event_data import EventData
from featurebyte.api.event_view import EventView
from featurebyte.api.feature_list import FeatureList
from featurebyte.api.feature_store import FeatureStore
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.feature import FeatureReadiness


def test_query_object_operation_on_sqlite_source(
    sqlite_session, transaction_data, config, sqlite_feature_store
):
    """
    Test loading event view from sqlite source
    """
    _ = sqlite_session
    assert sqlite_feature_store.list_tables(credentials=config.credentials) == ["test_table"]

    sqlite_database_table = sqlite_feature_store.get_table(
        database_name=None,
        schema_name=None,
        table_name="test_table",
        credentials=config.credentials,
    )
    expected_dtypes = pd.Series(
        {
            "event_timestamp": "TIMESTAMP",
            "created_at": "INT",
            "cust_id": "INT",
            "user_id": "INT",
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
        credentials=config.credentials,
    )
    event_view = EventView.from_event_data(event_data)
    assert event_view.columns == [
        "event_timestamp",
        "created_at",
        "cust_id",
        "user_id",
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
    output = event_view.preview(limit=expected.shape[0], credentials=config.credentials)
    # sqlite returns str for timestamp columns
    expected["event_timestamp"] = expected["event_timestamp"].astype(str)
    pd.testing.assert_frame_equal(output, expected[output.columns], check_dtype=False)


def check_feature_and_remove_registry(feature, feature_manager):
    """
    Check feature properties & registry values
    """
    assert feature.readiness == FeatureReadiness.DRAFT
    extended_feature_model = ExtendedFeatureModel(
        **feature.dict(by_alias=True), feature_store=feature.feature_store
    )
    feat_reg_df = feature_manager.retrieve_feature_registries(extended_feature_model)
    assert len(feat_reg_df) == 1
    assert feat_reg_df.iloc[0]["NAME"] == feature.name
    assert feat_reg_df.iloc[0]["VERSION"] == feature.version
    assert feat_reg_df.iloc[0]["READINESS"] == "DRAFT"
    feature_manager.remove_feature_registry(extended_feature_model)


def test_query_object_operation_on_snowflake_source(
    transaction_data_upper_case,
    config,
    event_data,
    feature_manager,
):
    """
    Test loading event view from snowflake source
    """
    # create event view
    event_view = EventView.from_event_data(event_data)
    assert event_view.columns == [
        "EVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "USER_ID",
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
    check_string_operations(event_view.PRODUCT_ACTION)
    check_datetime_operations(event_view.EVENT_TIMESTAMP)

    # construct expected results
    expected = transaction_data_upper_case.copy()
    expected["CUST_ID_X_SESSION_ID"] = (expected["CUST_ID"] * expected["SESSION_ID"]) / 1000.0
    expected["LUCKY_CUSTOMER"] = (expected["CUST_ID_X_SESSION_ID"] > 140.0).astype(int)
    expected["AMOUNT"] = expected["AMOUNT"].fillna(0)

    # check agreement
    output = event_view.preview(limit=expected.shape[0], credentials=config.credentials)
    output["CUST_ID_X_SESSION_ID"] = output["CUST_ID_X_SESSION_ID"].astype(
        float
    )  # type is not correct here
    pd.testing.assert_frame_equal(output, expected[output.columns], check_dtype=False)

    # create some features
    feature_group = event_view.groupby("USER_ID").aggregate(
        "USER_ID",
        "count",
        windows=["2h", "24h"],
        feature_names=["COUNT_2h", "COUNT_24h"],
    )
    feature_group_per_category = event_view.groupby("USER_ID", category="PRODUCT_ACTION").aggregate(
        "USER_ID",
        "count",
        windows=["24h"],
        feature_names=["COUNT_BY_ACTION_24h"],
    )

    # preview the features
    preview_param = {
        "POINT_IN_TIME": "2001-01-02 10:00:00",
        "uid": 1,
    }

    # preview count features
    df_feature_preview = feature_group.preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "uid": 1,
        "COUNT_2h": 1,
        "COUNT_24h": 4,
    }

    # preview count per category features
    df_feature_preview = feature_group_per_category.preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "uid": 1,
        "COUNT_BY_ACTION_24h": '{\n  "__MISSING__": 1,\n  "add": 1,\n  "detail": 1,\n  "remove": 1\n}',
    }

    # preview one feature only
    df_feature_preview = feature_group["COUNT_2h"].preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "uid": 1,
        "COUNT_2h": 1,
    }

    # preview a not-yet-assigned feature
    new_feature = feature_group["COUNT_2h"] / feature_group["COUNT_24h"]
    df_feature_preview = new_feature.preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "uid": 1,
        "Unnamed": Decimal("0.25"),
    }

    run_test_conditional_assign_feature(config, feature_group)

    # assign new feature and preview again
    feature_group["COUNT_2h / COUNT_24h"] = new_feature
    df_feature_preview = feature_group.preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "uid": 1,
        "COUNT_2h": 1,
        "COUNT_24h": 4,
        "COUNT_2h / COUNT_24h": Decimal("0.25"),
    }
    special_feature = create_feature_with_filtered_event_view(event_view)
    special_feature.save()  # pylint: disable=no-member
    check_feature_and_remove_registry(special_feature, feature_manager)

    run_and_test_get_historical_features(config, feature_group, feature_group_per_category)


def create_feature_with_filtered_event_view(event_view):
    """
    Create a feature with filtered event view using string literal
    """
    event_view = event_view[event_view["PRODUCT_ACTION"] == "purchase"]
    feature_group = event_view.groupby("USER_ID").aggregate(
        "USER_ID",
        "count",
        windows=["7d"],
        feature_names=["NUM_PURCHASE_7d"],
    )
    feature = feature_group["NUM_PURCHASE_7d"]
    return feature


def get_feature_preview_as_dict(obj, preview_param, config):
    df_feature_preview = obj.preview(preview_param, credentials=config.credentials)
    assert df_feature_preview.shape[0] == 1
    return df_feature_preview.iloc[0].to_dict()


def run_test_conditional_assign_feature(config, feature_group):
    """
    Test conditional assignment operations on Feature
    """
    feature_count_24h = feature_group["COUNT_24h"]
    preview_param = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "uid": 1,
    }
    result = get_feature_preview_as_dict(feature_count_24h, preview_param, config)
    assert result == {**preview_param, "COUNT_24h": 4}

    # Assign feature conditionally. Should be reflected in both Feature and FeatureGroup
    mask = feature_count_24h == 4.0
    feature_count_24h[mask] = 900
    result = get_feature_preview_as_dict(feature_count_24h, preview_param, config)
    assert result == {**preview_param, "COUNT_24h": 900}
    result = get_feature_preview_as_dict(feature_group, preview_param, config)
    assert result == {**preview_param, "COUNT_2h": 1, "COUNT_24h": 900}

    # Assign conditionally again (revert the above). Should be reflected in both Feature and
    # FeatureGroup
    mask = feature_count_24h == 900.0
    feature_count_24h[mask] = 4.0
    result = get_feature_preview_as_dict(feature_count_24h, preview_param, config)
    assert result == {**preview_param, "COUNT_24h": 4}
    result = get_feature_preview_as_dict(feature_group, preview_param, config)
    assert result == {**preview_param, "COUNT_2h": 1, "COUNT_24h": 4}

    # Assign to an unnamed Feature conditionally. Should not be reflected in Feature only and has no
    # effect on FeatureGroup
    temp_feature = feature_count_24h * 10
    mask = temp_feature == 40.0
    temp_feature[mask] = 900
    result = get_feature_preview_as_dict(temp_feature, preview_param, config)
    assert result == {**preview_param, "Unnamed": 900}
    result = get_feature_preview_as_dict(feature_group, preview_param, config)
    assert result == {**preview_param, "COUNT_2h": 1, "COUNT_24h": 4}


def run_and_test_get_historical_features(config, feature_group, feature_group_per_category):
    """Test getting historical features from FeatureList"""
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"] * 5),
            "uid": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    feature_list = FeatureList(
        [
            feature_group["COUNT_2h"],
            feature_group["COUNT_24h"],
            feature_group_per_category["COUNT_BY_ACTION_24h"],
            feature_group["COUNT_2h / COUNT_24h"],
        ],
        name="My FeatureList",
    )
    df_historical_expected = pd.DataFrame(
        {
            "POINT_IN_TIME": df_training_events["POINT_IN_TIME"],
            "uid": df_training_events["uid"],
            "COUNT_2h": [1, 1, 0, 0, 1, 1, 0, 0, 1, 0],
            "COUNT_24h": [4, 3, 8, 6, 3, 7, 4, 4, 8, 0],
            "COUNT_BY_ACTION_24h": [
                '{\n  "__MISSING__": 1,\n  "add": 1,\n  "detail": 1,\n  "remove": 1\n}',
                '{\n  "__MISSING__": 1,\n  "purchase": 1,\n  "remove": 1\n}',
                '{\n  "__MISSING__": 3,\n  "add": 1,\n  "detail": 2,\n  "remove": 2\n}',
                '{\n  "__MISSING__": 1,\n  "add": 2,\n  "detail": 1,\n  "purchase": 1,\n  "remove": 1\n}',
                '{\n  "detail": 1,\n  "purchase": 2\n}',
                '{\n  "__MISSING__": 3,\n  "add": 2,\n  "purchase": 1,\n  "remove": 1\n}',
                '{\n  "__MISSING__": 1,\n  "detail": 1,\n  "purchase": 1,\n  "remove": 1\n}',
                '{\n  "__MISSING__": 1,\n  "add": 1,\n  "detail": 1,\n  "purchase": 1\n}',
                '{\n  "__MISSING__": 2,\n  "add": 1,\n  "detail": 1,\n  "purchase": 3,\n  "remove": 1\n}',
                None,
            ],
            "COUNT_2h / COUNT_24h": [
                0.250000,
                0.333333,
                0,
                0,
                0.333333,
                0.142857,
                0,
                0,
                0.125000,
                np.nan,  # Note: zero divide by zero
            ],
        }
    )
    df_historical_features = feature_list.get_historical_features(
        df_training_events, credentials=config.credentials
    )
    # When using fetch_pandas_all(), the dtype of "USER_ID" column is int8 (int64 otherwise)
    pd.testing.assert_frame_equal(df_historical_features, df_historical_expected, check_dtype=False)

    # Test again using the same feature list and data but with serving names mapping
    _test_get_historical_features_with_serving_names(
        config, feature_list, df_training_events, df_historical_expected
    )


def _test_get_historical_features_with_serving_names(
    config, feature_list, df_training_events, df_historical_expected
):
    """Test getting historical features from FeatureList with alternative serving names"""

    mapping = {"uid": "new_uid"}

    # Instead of providing the default serving name "uid", provide "new_uid" in data
    df_training_events = df_training_events.rename(mapping, axis=1)
    df_historical_expected = df_historical_expected.rename(mapping, axis=1)
    assert "new_uid" in df_training_events
    assert "new_uid" in df_historical_expected

    df_historical_features = feature_list.get_historical_features(
        df_training_events,
        credentials=config.credentials,
        serving_names_mapping=mapping,
    )
    pd.testing.assert_frame_equal(df_historical_features, df_historical_expected, check_dtype=False)


def check_string_operations(varchar_series, limit=100):
    """Test string operations"""
    pandas_frame = varchar_series.preview(limit=limit)
    pandas_series = pandas_frame[pandas_frame.columns[0]]

    str_len = varchar_series.str.len().preview(limit=limit)
    str_lower = varchar_series.str.lower().preview(limit=limit)
    str_upper = varchar_series.str.upper().preview(limit=limit)
    str_strip = varchar_series.str.strip("e").preview(limit=limit)
    str_lstrip = varchar_series.str.lstrip("p").preview(limit=limit)
    str_rstrip = varchar_series.str.rstrip("l").preview(limit=limit)
    str_replace = varchar_series.str.replace("a", "i").preview(limit=limit)
    str_pad = varchar_series.str.pad(10, side="both", fillchar="-").preview(limit=limit)
    str_contains = varchar_series.str.contains("ai").preview(limit=limit)
    str_slice = varchar_series.str[:5].preview(limit=limit)

    pd.testing.assert_series_equal(
        str_len[str_len.columns[0]], pandas_series.str.len(), check_names=False
    )
    pd.testing.assert_series_equal(
        str_lower[str_lower.columns[0]], pandas_series.str.lower(), check_names=False
    )
    pd.testing.assert_series_equal(
        str_upper[str_upper.columns[0]], pandas_series.str.upper(), check_names=False
    )
    pd.testing.assert_series_equal(
        str_strip[str_strip.columns[0]], pandas_series.str.strip("e"), check_names=False
    )
    pd.testing.assert_series_equal(
        str_lstrip[str_lstrip.columns[0]], pandas_series.str.lstrip("p"), check_names=False
    )
    pd.testing.assert_series_equal(
        str_rstrip[str_rstrip.columns[0]], pandas_series.str.rstrip("l"), check_names=False
    )
    pd.testing.assert_series_equal(
        str_replace[str_replace.columns[0]],
        pandas_series.str.replace("a", "i"),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        str_pad[str_pad.columns[0]],
        pandas_series.str.pad(10, side="both", fillchar="-"),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        str_contains[str_contains.columns[0]],
        pandas_series.str.contains("ai"),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        str_slice[str_slice.columns[0]], pandas_series.str[:5], check_names=False
    )


def check_datetime_operations(datetime_series, limit=100):
    """Test datetime operations"""
    pandas_frame = datetime_series.preview(limit=limit)
    pandas_series = pandas_frame[pandas_frame.columns[0]]

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
    for prop in properties:
        dt_prop = getattr(datetime_series.dt, prop).preview(limit=limit)
        series_prop = getattr(pandas_series.dt, prop)
        pd.testing.assert_series_equal(
            dt_prop[dt_prop.columns[0]],
            series_prop,
            check_names=False,
            check_dtype=False,
        )
