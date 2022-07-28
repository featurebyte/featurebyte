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


def test_query_object_operation_on_sqlite_source(sqlite_session, transaction_data, config):
    """
    Test loading event view from sqlite source
    """
    _ = sqlite_session
    sqlite_database_source = FeatureStore(**config.feature_stores["sqlite_datasource"].dict())
    assert sqlite_database_source.list_tables(credentials=config.credentials) == ["test_table"]

    sqlite_database_table = sqlite_database_source.get_table(
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
    extended_feature_model = ExtendedFeatureModel(**feature.dict(by_alias=True))
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

    # construct expected results
    expected = transaction_data_upper_case.copy()
    expected["CUST_ID_X_SESSION_ID"] = (expected["CUST_ID"] * expected["SESSION_ID"]) / 1000.0
    expected["LUCKY_CUSTOMER"] = (expected["CUST_ID_X_SESSION_ID"] > 140.0).astype(int)

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
        "UID": 1,
    }

    # preview count features
    df_feature_preview = feature_group.preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "UID": 1,
        "COUNT_2h": 1,
        "COUNT_24h": 9,
    }

    # preview count per category features
    df_feature_preview = feature_group_per_category.preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "UID": 1,
        "COUNT_BY_ACTION_24h": '{\n  "add": 2,\n  "purchase": 3,\n  "remove": 4\n}',
    }

    # preview one feature only
    df_feature_preview = feature_group["COUNT_2h"].preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "UID": 1,
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
        "UID": 1,
        "Unnamed": Decimal("0.111111"),
    }

    # assign new feature and preview again
    feature_group["COUNT_2h DIV COUNT_24h"] = new_feature
    df_feature_preview = feature_group.preview(
        preview_param,
        credentials=config.credentials,
    )
    assert df_feature_preview.shape[0] == 1
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "UID": 1,
        "COUNT_2h": 1,
        "COUNT_24h": 9,
        "COUNT_2h DIV COUNT_24h": Decimal("0.111111"),
    }
    special_feature = feature_group["COUNT_2h DIV COUNT_24h"]
    special_feature.save()  # pylint: disable=no-member
    check_feature_and_remove_registry(special_feature, feature_manager)

    run_and_test_get_historical_features(config, feature_group, feature_group_per_category)


def run_and_test_get_historical_features(config, feature_group, feature_group_per_category):
    """Test getting historical features from FeatureList"""
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"] * 5),
            "UID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    feature_list = FeatureList(
        [
            feature_group["COUNT_2h"],
            feature_group["COUNT_24h"],
            feature_group_per_category["COUNT_BY_ACTION_24h"],
            feature_group["COUNT_2h DIV COUNT_24h"],
        ],
        name="My FeatureList",
    )
    df_historical_expected = pd.DataFrame(
        {
            "POINT_IN_TIME": df_training_events["POINT_IN_TIME"],
            "UID": df_training_events["UID"],
            "COUNT_2h": [1.0, 1.0, np.nan, 1.0, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            "COUNT_24h": [9.0, 7.0, 2.0, 5.0, 5.0, 4.0, 4.0, 7.0, 5.0, np.nan],
            "COUNT_BY_ACTION_24h": [
                '{\n  "add": 2,\n  "purchase": 3,\n  "remove": 4\n}',
                '{\n  "__MISSING__": 2,\n  "add": 3,\n  "detail": 1,\n  "purchase": 1\n}',
                '{\n  "__MISSING__": 1,\n  "remove": 1\n}',
                '{\n  "__MISSING__": 1,\n  "add": 2,\n  "detail": 2\n}',
                '{\n  "add": 1,\n  "detail": 1,\n  "purchase": 2,\n  "remove": 1\n}',
                '{\n  "add": 1,\n  "detail": 1,\n  "remove": 2\n}',
                '{\n  "add": 1,\n  "purchase": 2,\n  "remove": 1\n}',
                '{\n  "__MISSING__": 3,\n  "add": 1,\n  "purchase": 1,\n  "remove": 2\n}',
                '{\n  "add": 3,\n  "detail": 1,\n  "remove": 1\n}',
                None,
            ],
            "COUNT_2h DIV COUNT_24h": [
                0.111111,
                0.142857,
                np.nan,
                0.2,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
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

    mapping = {"UID": "NEW_UID"}

    # Instead of providing the default serving name "UID", provide "NEW_UID" in data
    df_training_events = df_training_events.rename(mapping, axis=1)
    df_historical_expected = df_historical_expected.rename(mapping, axis=1)
    assert "NEW_UID" in df_training_events
    assert "NEW_UID" in df_historical_expected

    df_historical_features = feature_list.get_historical_features(
        df_training_events,
        credentials=config.credentials,
        serving_names_mapping=mapping,
    )
    pd.testing.assert_frame_equal(df_historical_features, df_historical_expected, check_dtype=False)
