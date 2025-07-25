"""
This module contains session to EventView integration tests
"""

import json
import os
import time
from datetime import datetime
from unittest import mock
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId
from sqlglot import parse_one

from featurebyte import (
    AggFunc,
    CalendarWindow,
    CronFeatureJobSetting,
    Entity,
    FeatureList,
    HistoricalFeatureTable,
    RequestColumn,
    SourceType,
    to_timedelta,
)
from featurebyte.enum import InternalName, TimeIntervalUnit
from featurebyte.exception import RecordCreationException
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from tests.util.helper import (
    assert_preview_result_equal,
    compute_historical_feature_table_dataframe_helper,
    create_observation_table_by_upload,
    fb_assert_frame_equal,
    get_dataframe_from_materialized_table,
    get_lagged_series_pandas,
    iet_entropy,
    tz_localize_if_needed,
)


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
            event_view[f"column_{col_idx + 1}"] = (
                event_view[f"column_{col_idx}"] + event_view[f"column_{col_idx + 1}"]
            )

    output = None
    for idx in range(column_num):
        col_idx = idx + 1
        feat = event_view.groupby(group_by_col).aggregate_over(
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


@pytest.fixture(name="mock_session_manager")
def get_mocked_session_manager(session):
    with mock.patch(
        "featurebyte.service.session_manager.SessionManagerService.get_feature_store_session"
    ) as mocked_session:
        mocked_session.return_value = session
        yield


def test_event_view_ops(event_view, transaction_data_upper_case, source_type):
    """
    Test operations that can be performed on an EventView before creating features
    """
    # need to specify the constant as float, otherwise results will get truncated
    event_view["CUST_ID_X_SESSION_ID"] = event_view["CUST_ID"] * event_view["SESSION_ID"] / 1000.0
    event_view["LUCKY_CUSTOMER"] = event_view["CUST_ID_X_SESSION_ID"] > 140.0

    # apply more event view operations
    event_view["ÀMOUNT"].fillna(0)

    # check accessor operations
    check_string_operations(event_view, "PRODUCT_ACTION")

    # check casting operations
    check_cast_operations(event_view, source_type=event_view.feature_store.type)

    # check numeric operations
    check_numeric_operations(event_view)

    # construct expected results
    expected = transaction_data_upper_case.copy()
    expected["CUST_ID_X_SESSION_ID"] = (expected["CUST_ID"] * expected["SESSION_ID"]) / 1000.0
    expected["LUCKY_CUSTOMER"] = (expected["CUST_ID_X_SESSION_ID"] > 140.0).astype(int)
    expected["ÀMOUNT"] = expected["ÀMOUNT"].fillna(0)

    # check agreement
    output = event_view.preview(limit=expected.shape[0])
    output["CUST_ID_X_SESSION_ID"] = output["CUST_ID_X_SESSION_ID"].astype(
        float
    )  # type is not correct here
    columns = [
        col for col in output.columns if not col.startswith("str_") and not col.startswith("dt_")
    ]

    # only snowflake supports timezone in datetime columns
    if source_type not in ["snowflake"]:
        expected["ËVENT_TIMESTAMP"] = pd.to_datetime(
            expected["ËVENT_TIMESTAMP"], utc=True
        ).dt.tz_localize(None)
    fb_assert_frame_equal(output[columns], expected[columns], sort_by_columns=["TRANSACTION_ID"])


def test_feature_operations__feature_group_preview(feature_group):
    """
    Test operations on Feature objects
    """
    preview_param = {
        "POINT_IN_TIME": "2001-01-02 10:00:00",
        "üser id": 1,
    }

    # preview feature group
    df_feature_preview = feature_group.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "COUNT_2h": 3,
            "COUNT_24h": 14,
        },
    )

    # preview one feature only
    df_feature_preview = feature_group["COUNT_2h"].preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "COUNT_2h": 3,
        },
    )

    # preview a not-yet-assigned feature
    new_feature = feature_group["COUNT_2h"] / feature_group["COUNT_24h"]
    df_feature_preview = new_feature.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "Unnamed": 0.2142857143,
        },
    )


def test_feature_preview__same_entity_multiple_point_in_times(feature_group):
    """
    Test previewing features when the same entity has multiple point in times in the request data
    """
    preview_data = pd.DataFrame({
        "POINT_IN_TIME": ["2001-01-20 10:00:00", "2001-01-02 10:00:00"],
        "üser id": [1, 1],
    })
    df_feature_preview = feature_group.preview(preview_data)
    df_expected = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-01-20 10:00:00", "2001-01-02 10:00:00"]),
        "üser id": [1, 1],
        "COUNT_2h": [np.nan, 3],
        "COUNT_24h": [17, 14],
    })
    fb_assert_frame_equal(df_feature_preview, df_expected)


def test_isnull_compare_with_bool(event_view):
    """
    Test a special case of using isnull with bool literal
    """
    filtered_view = event_view[event_view["ÀMOUNT"].isnull() == False]  # noqa
    df = filtered_view.preview()
    assert df["ÀMOUNT"].notnull().all()


def test_feature_operations__conditional_assign(feature_group):
    """
    Test operations on Feature objects - conditional assignment
    """
    run_test_conditional_assign_feature(feature_group)


def test_feature_operations__complex_feature_preview(
    event_view, feature_group, feature_group_per_category
):
    """
    Test feature operations - complex feature preview
    """
    source_type = event_view.feature_store.type

    preview_param = {
        "POINT_IN_TIME": "2001-01-02 10:00:00",
        "üser id": 1,
    }
    # add iet entropy
    feature_group["iet_entropy_24h"] = iet_entropy(
        event_view, "ÜSER ID", window="24h", name="iet_entropy_24h"
    )
    feature_group["pyramid_sum_24h"] = pyramid_sum(
        event_view, "ÜSER ID", window="24h", numeric_column="ÀMOUNT", name="pyramid_sum_24h"
    )
    feature_group["amount_sum_24h"] = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT", method="sum", windows=["24h"], feature_names=["amount_sum_24h"]
    )["amount_sum_24h"]

    special_feature = create_feature_with_filtered_event_view(event_view)
    if source_type == SourceType.SNOWFLAKE:
        # should only save once since the feature names are the same
        special_feature.save()

    # preview a more complex feature group (multiple group by, some have the same tile_id)
    features = [
        feature_group["COUNT_2h"],
        feature_group["iet_entropy_24h"],
        feature_group["pyramid_sum_24h"],
        feature_group["amount_sum_24h"],
        special_feature,
        feature_group_per_category["COUNT_BY_ACTION_24h"],
    ]

    feature_list_combined = FeatureList(features, name="My FeatureList")
    feature_group_combined = feature_list_combined[feature_list_combined.feature_names]
    df_feature_preview = feature_group_combined.preview(pd.DataFrame([preview_param]))
    expected_amount_sum_24h = 582.14
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "üser id": 1,
        "COUNT_2h": 3,
        "COUNT_BY_ACTION_24h": '{\n  "__MISSING__": 1,\n  "detail": 2,\n  "purchase": 4,\n  "rëmove": 1,\n  "àdd": 6\n}',
        "NUM_PURCHASE_7d": 6,
        "iet_entropy_24h": 1.661539,
        "pyramid_sum_24h": 7 * expected_amount_sum_24h,  # 1 + 2 + 4 = 7
        "amount_sum_24h": expected_amount_sum_24h,
    }

    assert_preview_result_equal(
        df_feature_preview, expected, dict_like_columns=["COUNT_BY_ACTION_24h"]
    )


def test_feature_operations(event_view, feature_group, feature_group_per_category):
    """
    Test operations on Feature objects
    """
    source_type = event_view.feature_store.type
    count_dict_supported = source_type != SourceType.DATABRICKS

    preview_param = {
        "POINT_IN_TIME": "2001-01-02 10:00:00",
        "üser id": 1,
    }

    if count_dict_supported:
        # preview count per category features
        df_feature_preview = feature_group_per_category.preview(pd.DataFrame([preview_param]))
        expected = {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "COUNT_BY_ACTION_2h": '{\n  "purchase": 1,\n  "àdd": 2\n}',
            "COUNT_BY_ACTION_24h": '{\n  "__MISSING__": 1,\n  "detail": 2,\n  "purchase": 4,\n  "rëmove": 1,\n  "àdd": 6\n}',
            "ENTROPY_BY_ACTION_24h": 1.376055285260417,
            "MOST_FREQUENT_ACTION_24h": "àdd",
            "NUM_UNIQUE_ACTION_24h": 5,
            "NUM_UNIQUE_ACTION_24h_exclude_missing": 4,
            "ACTION_SIMILARITY_2h_to_24h": 0.9395523512235261,
        }
        assert_preview_result_equal(
            df_feature_preview,
            expected,
            dict_like_columns=["COUNT_BY_ACTION_2h", "COUNT_BY_ACTION_24h"],
        )

    # assign new feature and preview again
    new_feature = feature_group["COUNT_2h"] / feature_group["COUNT_24h"]
    feature_group["COUNT_2h DIV COUNT_24h"] = new_feature
    df_feature_preview = feature_group.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "COUNT_2h": 3,
            "COUNT_24h": 14,
            "COUNT_2h DIV COUNT_24h": 0.21428599999999998,
        },
    )

    # check casting on feature
    df_feature_preview = (
        (feature_group["COUNT_2h"].astype(int) + 1)
        .astype(float)
        .preview(pd.DataFrame([preview_param]))
    )
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "Unnamed": 4.0,
        },
    )


def test_feature_operations__check_day_of_week_counts(event_view):
    """
    Test operations on Feature objects - check day of week counts
    """
    source_type = event_view.feature_store.type
    if source_type == SourceType.DATABRICKS:
        return

    preview_param = {
        "POINT_IN_TIME": "2001-01-02 10:00:00",
        "üser id": 1,
    }

    # Check using a derived numeric column as category
    check_day_of_week_counts(event_view, preview_param)


def create_feature_with_filtered_event_view(event_view):
    """
    Create a feature with filtered event view using string literal
    """
    event_view = event_view[event_view["PRODUCT_ACTION"] == "purchase"]
    feature_group = event_view.groupby("ÜSER ID").aggregate_over(
        value_column=None,
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
        "üser id": 1,
    }
    result = feature_count_24h.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_24h": 14})

    # Assign feature conditionally. Should be reflected in both Feature and FeatureGroup
    feature_group[feature_count_24h == 14.0, "COUNT_24h"] = 900
    result = feature_count_24h.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_24h": 900})
    result = feature_group.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_2h": 3, "COUNT_24h": 900})

    # Assign conditionally again (revert the above). Should be reflected in both Feature and
    # FeatureGroup
    mask = feature_count_24h == 900.0
    feature_count_24h[mask] = 14.0
    result = feature_count_24h.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_24h": 14})
    result = feature_group.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_2h": 3, "COUNT_24h": 14})

    # Assign conditionally a series
    double_feature_count_24h = feature_count_24h * 2
    feature_count_24h[mask] = double_feature_count_24h[mask]
    result = feature_count_24h.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_24h": 28})

    # Undo above
    mask = feature_count_24h == 28.0
    feature_count_24h[mask] = 14.0
    result = feature_count_24h.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_24h": 14})

    # Assign to an unnamed Feature conditionally. Should not be reflected in Feature only and has no
    # effect on FeatureGroup
    temp_feature = feature_count_24h * 10
    mask = temp_feature == 140.0
    temp_feature[mask] = 900
    result = temp_feature.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "Unnamed": 900})
    result = feature_group.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_2h": 3, "COUNT_24h": 14})

    # Assign to copied Series should not be reflected in FeatureGroup
    cloned_feature = feature_group["COUNT_24h"].copy()
    cloned_feature[cloned_feature == 14] = 0
    result = feature_group.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(result, {**preview_param, "COUNT_2h": 3, "COUNT_24h": 14})


@pytest.fixture
def patched_num_features_per_query():
    """
    Patch the NUM_FEATURES_PER_QUERY parameter to trigger executing feature query in batches
    """
    with patch("featurebyte.session.session_helper.NUM_FEATURES_PER_QUERY", 4):
        yield


@pytest.fixture(name="new_user_id_entity", scope="session")
def new_user_id_entity_fixture():
    """
    Fixture for a new user id entity
    """
    entity = Entity(name="new user id", serving_names=["new_user id"])
    entity.save()
    return entity


def get_training_events_and_expected_result():
    """Returns training events and expected historical result"""
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"] * 5),
        "üser id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    })
    df_historical_expected = pd.DataFrame({
        "POINT_IN_TIME": df_training_events["POINT_IN_TIME"],
        "üser id": df_training_events["üser id"],
        "COUNT_2h": [3, 1, 1, np.nan, np.nan, 3, np.nan, np.nan, 1, np.nan],
        "COUNT_24h": [14, 12, 13, 11, 13, 18, 18, 13, 14, np.nan],
        "COUNT_BY_ACTION_24h": [
            '{\n  "__MISSING__": 1,\n  "detail": 2,\n  "purchase": 4,\n  "rëmove": 1,\n  "àdd": 6\n}',
            '{\n  "__MISSING__": 5,\n  "detail": 2,\n  "rëmove": 4,\n  "àdd": 1\n}',
            '{\n  "__MISSING__": 3,\n  "detail": 4,\n  "purchase": 4,\n  "rëmove": 2\n}',
            '{\n  "__MISSING__": 4,\n  "detail": 1,\n  "purchase": 1,\n  "àdd": 5\n}',
            '{\n  "__MISSING__": 2,\n  "detail": 3,\n  "purchase": 4,\n  "rëmove": 2,\n  "àdd": 2\n}',
            '{\n  "__MISSING__": 4,\n  "detail": 2,\n  "purchase": 6,\n  "rëmove": 2,\n  "àdd": 4\n}',
            '{\n  "__MISSING__": 3,\n  "detail": 5,\n  "purchase": 6,\n  "rëmove": 3,\n  "àdd": 1\n}',
            '{\n  "__MISSING__": 4,\n  "detail": 1,\n  "purchase": 1,\n  "rëmove": 6,\n  "àdd": 1\n}',
            '{\n  "__MISSING__": 3,\n  "detail": 3,\n  "purchase": 2,\n  "rëmove": 4,\n  "àdd": 2\n}',
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
            "àdd",
            "__MISSING__",
            "detail",
            "àdd",
            "purchase",
            "purchase",
            "purchase",
            "rëmove",
            "rëmove",
            None,
        ],
        "NUM_UNIQUE_ACTION_24h": [5.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 5.0, 0.0],
        "COUNT_2h DIV COUNT_24h": [
            0.214286,
            0.083333,
            0.076923,
            np.nan,
            np.nan,
            0.166667,
            np.nan,
            np.nan,
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
        "TS_MIN_24h": [
            pd.Timestamp("2001-01-02 08:42:19.000673"),
            pd.Timestamp("2001-01-02 10:56:57.000561"),
            pd.Timestamp("2001-01-02 08:06:01.000282"),
            pd.Timestamp("2001-01-02 08:41:46.000438"),
            pd.Timestamp("2001-01-02 06:54:51.000699"),
            pd.Timestamp("2001-01-02 10:29:32.000059"),
            pd.Timestamp("2001-01-02 06:12:39.000024"),
            pd.Timestamp("2001-01-02 07:59:55.000736"),
            pd.Timestamp("2001-01-02 07:19:27.000175"),
            pd.NaT,
        ],
        "TS_MAX_24h": [
            pd.Timestamp("2001-01-02 08:42:19.000673"),
            pd.Timestamp("2001-01-02 10:56:57.000561"),
            pd.Timestamp("2001-01-02 08:06:01.000282"),
            pd.Timestamp("2001-01-02 08:41:46.000438"),
            pd.Timestamp("2001-01-02 06:54:51.000699"),
            pd.Timestamp("2001-01-02 10:29:32.000059"),
            pd.Timestamp("2001-01-02 06:12:39.000024"),
            pd.Timestamp("2001-01-02 07:59:55.000736"),
            pd.Timestamp("2001-01-02 07:19:27.000175"),
            pd.NaT,
        ],
    })
    return df_training_events, df_historical_expected


def check_historical_features_system_metrics(config, historical_feature_table_id=None):
    """
    Check system metrics for historical features
    """
    params = {"metrics_type": "historical_features"}
    if historical_feature_table_id is not None:
        params["historical_feature_table_id"] = str(historical_feature_table_id)

    client = config.get_client()
    response = client.get("/system_metrics", params=params)
    assert response.status_code == 200
    response_dict = response.json()

    if historical_feature_table_id:
        assert response_dict["total"] == 1
    metrics = response_dict["data"][0]
    assert set(metrics["metrics_data"].keys()) == {
        "historical_feature_table_id",
        "tile_compute_seconds",
        "tile_compute_metrics",
        "feature_compute_seconds",
        "feature_cache_update_seconds",
        "total_seconds",
        "metrics_type",
    }


@pytest.mark.parametrize(
    "in_out_formats",
    [
        ("dataframe", "dataframe"),
        ("dataframe", "table"),
        ("table", "table"),  # input is observation table
        ("uploaded_table", "table"),  # input is observation table from uploaded parquet file
    ],
)
@pytest.mark.usefixtures("patched_num_features_per_query")
@pytest.mark.asyncio
async def test_get_historical_features(
    session,
    data_source,
    feature_list_with_combined_feature_groups,
    in_out_formats,
    user_entity,
    new_user_id_entity,
    config,
):
    """
    Test getting historical features from FeatureList
    """
    _ = user_entity, new_user_id_entity
    input_format, output_format = in_out_formats
    assert input_format in {"dataframe", "table", "uploaded_table"}
    assert output_format in {"dataframe", "table"}

    feature_list = feature_list_with_combined_feature_groups
    df_training_events, df_historical_expected = get_training_events_and_expected_result()

    if "table" in input_format:
        df_historical_expected.insert(0, "__FB_TABLE_ROW_INDEX", np.arange(1, 11))

    if output_format == "table":
        (
            df_historical_features,
            historical_feature_table_id,
        ) = await compute_historical_feature_table_dataframe_helper(
            feature_list=feature_list,
            df_observation_set=df_training_events,
            session=session,
            data_source=data_source,
            input_format=input_format,
            return_id=True,
        )
    else:
        historical_feature_table_id = None
        existing_historical_feature_tables = HistoricalFeatureTable.list().shape[0]
        df_historical_features = feature_list.compute_historical_features(df_training_events)
        assert HistoricalFeatureTable.list().shape[0] == existing_historical_feature_tables

    # When input_format is "table", it is created by creating a source table using db session's
    # register_table method. In BigQuery's case, this method doesn't guarantee that the original
    # DataFrame's ordering is preserved. In other words, we cannot expect that the row index is
    # np.arange(1, 11) as setup in df_historical_expected.
    row_index_not_comparable = session.source_type == "bigquery" and input_format == "table"
    ignore_columns = ["__FB_TABLE_ROW_INDEX"] if row_index_not_comparable else None

    check_historical_features_system_metrics(config, historical_feature_table_id)

    # When using fetch_pandas_all(), the dtype of "ÜSER ID" column is int8 (int64 otherwise)
    fb_assert_frame_equal(
        df_historical_features,
        df_historical_expected,
        dict_like_columns=["COUNT_BY_ACTION_24h"],
        sort_by_columns=["POINT_IN_TIME", "üser id"] if output_format == "table" else None,
        ignore_columns=ignore_columns,
    )

    # Test again using the same feature list and table but with serving names mapping
    await _test_get_historical_features_with_serving_names(
        feature_list,
        df_training_events,
        df_historical_expected,
        session,
        data_source,
        input_format=input_format,
        output_format=output_format,
        ignore_columns=ignore_columns,
    )


async def _test_get_historical_features_with_serving_names(
    feature_list,
    df_training_events,
    df_historical_expected,
    session,
    data_source,
    input_format,
    output_format,
    ignore_columns,
):
    """Test getting historical features from FeatureList with alternative serving names"""

    mapping = {"üser id": "new_user id"}

    # Instead of providing the default serving name "user id", provide "new_user id" in table
    if not (input_format == "table" and output_format == "table"):
        # When both input and output are tables, we can let the creation of the observation table helper handle
        # the column remapping.
        df_training_events = df_training_events.rename(mapping, axis=1)
        assert "new_user id" in df_training_events
    df_historical_expected = df_historical_expected.rename(mapping, axis=1)
    assert "new_user id" in df_historical_expected

    if output_format == "table":
        df_historical_features = await compute_historical_feature_table_dataframe_helper(
            feature_list=feature_list,
            df_observation_set=df_training_events,
            session=session,
            data_source=data_source,
            serving_names_mapping=mapping,
            input_format=input_format,
        )
    else:
        df_historical_features = feature_list.compute_historical_features(
            df_training_events,
            serving_names_mapping=mapping,
        )
    fb_assert_frame_equal(
        df_historical_features,
        df_historical_expected,
        dict_like_columns=["COUNT_BY_ACTION_24h"],
        sort_by_columns=["POINT_IN_TIME", "new_user id"] if output_format == "table" else None,
        ignore_columns=ignore_columns,
    )


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
    event_view["str_new"] = varchar_series.str.replace("ai", "a%i")
    event_view["str_contains_special_char"] = event_view["str_new"].str.contains("a%i")
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
    new_series = pandas_series.str.replace("ai", "a%i")
    pd.testing.assert_series_equal(
        str_df["str_contains_special_char"],
        new_series.str.contains("a%i"),
        check_names=False,
    )
    pd.testing.assert_series_equal(str_df["str_slice"], pandas_series.str[:5], check_names=False)


def assert_datetime_almost_equal(s1: pd.Series, s2: pd.Series):
    """
    Assert that two datetime series are almost equal with difference of up to 1 microsecond
    """
    assert pd.to_timedelta(s1 - s2).dt.total_seconds().abs().max() <= 1e-6


@pytest_asyncio.fixture(name="observation_table_and_df_historical_expected", scope="module")
async def observation_table_and_df_historical_expected_fixture():
    """Observation table fixture"""
    df_training_events, df_historical_expected = get_training_events_and_expected_result()
    df_historical_expected.insert(0, "__FB_TABLE_ROW_INDEX", np.arange(1, 11))
    observation_table = create_observation_table_by_upload(df_training_events)
    return observation_table, df_historical_expected


@pytest.mark.asyncio
async def test_get_historical_features__feature_table_cache(
    session,
    feature_group,
    feature_group_per_category,
    user_entity,
    new_user_id_entity,
    feature_table_cache_metadata_service,
    observation_table_and_df_historical_expected,
):
    """Test feature table cache create/update"""
    _ = user_entity, new_user_id_entity

    feature_list_1 = FeatureList(
        [
            feature_group["COUNT_2h"],
            feature_group_per_category["COUNT_BY_ACTION_24h"],
            feature_group_per_category["MOST_FREQUENT_ACTION_24h"],
        ],
        name="My FeatureList 1",
    )

    feature_list_2 = FeatureList(
        [
            feature_group["COUNT_2h"],
            feature_group["COUNT_24h"],
            feature_group_per_category["COUNT_BY_ACTION_24h"],
            feature_group_per_category["ENTROPY_BY_ACTION_24h"],
            feature_group_per_category["MOST_FREQUENT_ACTION_24h"],
            feature_group_per_category["NUM_UNIQUE_ACTION_24h"],
            feature_group_per_category["ACTION_SIMILARITY_2h_to_24h"],
        ],
        name="My FeatureList 2",
    )

    observation_table, df_historical_expected = observation_table_and_df_historical_expected
    historical_feature_table_name = f"historical_feature_table_{ObjectId()}"
    historical_feature_table = feature_list_1.compute_historical_feature_table(
        observation_table,
        historical_feature_table_name,
    )

    assert len(historical_feature_table.features_info) == 3
    assert set(feat_info.feature_name for feat_info in historical_feature_table.features_info) == {
        "COUNT_2h",
        "COUNT_BY_ACTION_24h",
        "MOST_FREQUENT_ACTION_24h",
    }

    df_historical_features_1 = await get_dataframe_from_materialized_table(
        session, historical_feature_table
    )
    df = historical_feature_table.to_pandas()
    assert df.shape[1] == df_historical_features_1.shape[1] - 1  # no row index
    cols = [
        col
        for col in df_historical_features_1.columns.tolist()
        if col != InternalName.TABLE_ROW_INDEX
    ]
    assert df.columns.tolist() == cols

    expected_cols = [
        "__FB_TABLE_ROW_INDEX",
        "POINT_IN_TIME",
        "üser id",
    ] + feature_list_1.feature_names
    _df_historical_expected = df_historical_expected[
        [col for col in df_historical_expected.columns if col in expected_cols]
    ]
    fb_assert_frame_equal(
        df_historical_features_1,
        _df_historical_expected,
        dict_like_columns=["COUNT_BY_ACTION_24h"],
        sort_by_columns=["POINT_IN_TIME", "üser id"],
    )

    historical_feature_table_name = f"historical_feature_table_{ObjectId()}"
    historical_feature_table = feature_list_2.compute_historical_feature_table(
        observation_table,
        historical_feature_table_name,
    )
    df_historical_features_2 = await get_dataframe_from_materialized_table(
        session, historical_feature_table
    )
    df = historical_feature_table.to_pandas()
    assert df.shape[1] == df_historical_features_2.shape[1] - 1  # no row index
    cols = [
        col
        for col in df_historical_features_2.columns.tolist()
        if col != InternalName.TABLE_ROW_INDEX
    ]
    assert df.columns.tolist() == cols

    expected_cols = [
        "__FB_TABLE_ROW_INDEX",
        "POINT_IN_TIME",
        "üser id",
    ] + feature_list_2.feature_names
    _df_historical_expected = df_historical_expected[
        [col for col in df_historical_expected.columns if col in expected_cols]
    ]
    fb_assert_frame_equal(
        df_historical_features_2,
        _df_historical_expected,
        dict_like_columns=["COUNT_BY_ACTION_24h"],
        sort_by_columns=["POINT_IN_TIME", "üser id"],
    )

    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table.id,
    )
    assert len(cached_definitions) == len(
        set(feature_list_1.feature_names + feature_list_2.feature_names)
    )
    df = await session.execute_query(
        sql_to_string(
            parse_one(
                f"""
                SELECT * FROM "{session.database_name}"."{session.schema_name}"."{cached_definitions[0].table_name}"
                """
            ),
            source_type=session.source_type,
        )
    )
    assert df.shape[0] == df_historical_expected.shape[0]


@pytest.mark.asyncio
async def test_get_historical_features__features_info(
    feature_group,
    user_entity,
    new_user_id_entity,
    observation_table_and_df_historical_expected,
):
    """Test feature table cache create/update"""
    _ = user_entity, new_user_id_entity

    unsaved_feature = feature_group["COUNT_2h"] + 1
    unsaved_feature.name = "unsaved_feature"
    feature_list = FeatureList([unsaved_feature], name="unsaved feature list")

    # compute historical feature table
    observation_table, _ = observation_table_and_df_historical_expected
    hist_feat_table = feature_list.compute_historical_feature_table(
        observation_set=observation_table,
        historical_feature_table_name=f"hist_feat_table_{ObjectId()}",
    )

    # check that features_info are saved properly
    assert len(hist_feat_table.features_info) == 1
    assert set(feat_info.feature_name for feat_info in hist_feat_table.features_info) == {
        "unsaved_feature"
    }

    # test historical feature table preview
    hist_feat_table_preview = hist_feat_table.preview()
    assert hist_feat_table_preview.shape[0] == 10, hist_feat_table_preview

    # delete the historical feature table
    hist_feat_table.delete()

    # check saved feature list
    feature_list.save()

    # compute historical feature table
    hist_feat_table = feature_list.compute_historical_feature_table(
        observation_set=observation_table,
        historical_feature_table_name=f"hist_feat_table_{ObjectId()}",
    )

    # check that features_info are saved properly
    assert len(hist_feat_table.features_info) == 1
    assert set(feat_info.feature_name for feat_info in hist_feat_table.features_info) == {
        "unsaved_feature"
    }
    assert hist_feat_table.feature_names == ["unsaved_feature"]
    assert hist_feat_table.feature_ids == [unsaved_feature.id]

    # delete the historical feature table
    hist_feat_table.delete()


@pytest.mark.asyncio
async def test_get_target__feature_table_cache(
    event_view,
    user_entity,
    new_user_id_entity,
    transaction_data_upper_case,
    feature_table_cache_metadata_service,
):
    """Test feature table cache create/update for target"""
    _ = (
        user_entity,
        new_user_id_entity,
        transaction_data_upper_case,
        feature_table_cache_metadata_service,
    )

    target = event_view.groupby("ÜSER ID").forward_aggregate(
        method="avg",
        value_column="ÀMOUNT",
        window="24h",
        target_name="avg_24h_target",
        fill_value=None,
    )

    df_training_events, _ = get_training_events_and_expected_result()

    expected_targets = pd.Series([
        59.69888889,
        44.19846154,
        62.31333333,
        44.30076923,
        51.52,
        54.336,
        51.28,
        34.06888889,
        53.68,
        np.nan,
    ])
    df_expected = pd.concat([df_training_events, expected_targets], axis=1)
    df_expected.columns = ["POINT_IN_TIME", "üser id", "avg_24h_target"]

    observation_table = create_observation_table_by_upload(df_training_events)
    target_table = target.compute_target_table(observation_table, f"new_table_{time.time()}")
    df = target_table.to_pandas()
    assert df.columns.tolist() == ["POINT_IN_TIME", "üser id", "avg_24h_target"]
    pd.testing.assert_frame_equal(df, df_expected, check_dtype=False)


def test_datetime_operations(event_view, source_type):
    """Test datetime operations"""
    event_view = event_view.copy()

    column_name = "ËVENT_TIMESTAMP"
    limit = 100
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

    df_filtered_col = event_view_filtered["event_interval_second"].preview()
    assert (df_filtered_col["event_interval_second"] > 500000).all()

    # check datetime extracted properties
    dt_df = event_view.preview(limit=limit)

    pandas_series = dt_df[column_name]

    # The correct date properties should be extracted using events' local time. This will add
    # timezone offset to the event timestamp (UTC) to obtain the local time.
    event_timestamp_utc = pd.to_datetime(pandas_series, utc=True).dt.tz_localize(None)
    offset_info = dt_df["TZ_OFFSET"].str.extract("(?P<sign>[+-])(?P<hour>\d\d):(?P<minute>\d\d)")
    offset_info["sign"] = offset_info["sign"].replace({"-": -1, "+": 1}).astype(int)
    offset_info["total_minutes"] = offset_info["sign"] * offset_info["hour"].astype(
        int
    ) * 60 + offset_info["minute"].astype(int)
    event_timestamp_localized = event_timestamp_utc + pd.to_timedelta(
        offset_info["total_minutes"], unit="m"
    )

    for prop in properties:
        if prop == "week":
            series_prop = event_timestamp_localized.dt.isocalendar().week
        else:
            series_prop = getattr(event_timestamp_localized.dt, prop)
        pd.testing.assert_series_equal(
            dt_df[f"dt_{prop}"],
            series_prop,
            check_names=False,
            check_dtype=False,
        )

    # check timedelta extracted properties
    pandas_previous_timestamp = get_lagged_series_pandas(
        dt_df, "ËVENT_TIMESTAMP", "ËVENT_TIMESTAMP", "CUST_ID"
    )

    pandas_event_interval_second = pd.to_timedelta(
        pandas_series - pandas_previous_timestamp
    ).dt.total_seconds()
    pandas_event_interval_minute = (
        pd.to_timedelta(pandas_series - pandas_previous_timestamp).dt.total_seconds() / 60
    )
    pandas_event_interval_hour = (
        pd.to_timedelta(pandas_series - pandas_previous_timestamp).dt.total_seconds() / 3600
    )
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
    assert_datetime_almost_equal(dt_df["timestamp_added"], pandas_timestamp_added)
    assert_datetime_almost_equal(dt_df["timestamp_added_from_timediff"], pandas_timestamp_added)
    assert_datetime_almost_equal(dt_df["timestamp_added_constant"], pandas_timestamp_added_constant)
    pandas_timedelta_hour = (
        pd.to_timedelta(
            dt_df["event_interval_microsecond"].astype(float), unit="microsecond"
        ).dt.total_seconds()
        / 3600
    )
    pd.testing.assert_series_equal(
        dt_df["timedelta_hour"].astype(float), pandas_timedelta_hour, check_names=False
    )


def test_datetime_comparison__fixed_timestamp_non_tz(event_view, source_type):
    """Test datetime comparison with a fixed timestamp (non-tz)"""

    timestamp_column = "ËVENT_TIMESTAMP"

    # Fixed timestamp without timezone - assumed to be in UTC
    fixed_timestamp_non_tz = pd.Timestamp("2001-01-15 10:00:00")
    event_view["result"] = event_view[timestamp_column] > fixed_timestamp_non_tz

    df = event_view.preview(limit=100)

    if source_type == SourceType.SNOWFLAKE:
        # Convert to UTC and remove timezone to allow comparison with the fixed timestamp
        timestamp_series = df[timestamp_column].apply(
            lambda x: x.tz_convert("UTC").tz_localize(None)
        )
    else:
        # Spark returns timestamp converted to UTC and without timezone
        timestamp_series = df[timestamp_column]

    expected = timestamp_series > fixed_timestamp_non_tz

    pd.testing.assert_series_equal(df["result"], expected, check_names=False)


def test_datetime_comparison__fixed_timestamp_tz(event_view, source_type):
    """Test datetime comparison with a fixed timestamp (tz-aware)"""

    timestamp_column = "ËVENT_TIMESTAMP"

    fixed_timestamp_with_tz = pd.Timestamp("2001-01-15 10:00:00+08:00")
    event_view["result"] = event_view[timestamp_column] > fixed_timestamp_with_tz

    df = event_view.preview(limit=100)

    if source_type == SourceType.SNOWFLAKE:
        fixed_timestamp = fixed_timestamp_with_tz
    else:
        # Spark returns timestamp converted to UTC and without timezone. To allow comparison,
        # convert the fixed timestamp to UTC and remove timezone.
        fixed_timestamp = fixed_timestamp_with_tz.tz_convert("UTC").tz_localize(None)

    expected = df[timestamp_column] > fixed_timestamp

    pd.testing.assert_series_equal(df["result"], expected, check_names=False)


def check_cast_operations(event_view, source_type, limit=100):
    """Check casting operations"""
    event_view = event_view.copy()
    event_view["AMOUNT_INT"] = event_view["ÀMOUNT"].astype(int)
    event_view["AMOUNT_STR"] = event_view["ÀMOUNT"].astype(str)
    event_view["AMOUNT_FLOAT"] = event_view["ÀMOUNT"].astype(float)
    event_view["INT_FROM_BOOL"] = (event_view["ÀMOUNT"] > 50).astype(int)
    event_view["FLOAT_FROM_BOOL"] = (event_view["ÀMOUNT"] > 50).astype(float)
    df = event_view.preview(limit=limit)

    # compare string representation to make sure that the values are converted to int rather than
    # just being floored ("2" instead of "2.0")
    expected = df["ÀMOUNT"].astype(int).astype(str).tolist()
    assert df["AMOUNT_INT"].astype(str).tolist() == expected

    if source_type == SourceType.SNOWFLAKE:
        pd.testing.assert_series_equal(
            df["AMOUNT_STR"],
            df["ÀMOUNT"].astype(str).apply(lambda x: "0" if x == "0.0" else x),
            check_names=False,
        )
    elif source_type == SourceType.BIGQUERY:
        pd.testing.assert_series_equal(
            df["AMOUNT_STR"],
            df["ÀMOUNT"].astype(str).str.replace(".0$", "", regex=True),
            check_names=False,
        )
    else:
        pd.testing.assert_series_equal(
            df["AMOUNT_STR"], df["ÀMOUNT"].astype(str), check_names=False
        )

    pd.testing.assert_series_equal(
        df["AMOUNT_FLOAT"], df["ÀMOUNT"].astype(float), check_names=False
    )

    assert df["INT_FROM_BOOL"].tolist() == (df["ÀMOUNT"] > 50).astype(int).tolist()
    assert df["FLOAT_FROM_BOOL"].tolist() == (df["ÀMOUNT"] > 50).astype(float).tolist()


def check_numeric_operations(event_view, limit=100):
    """Check casting operations"""
    event_view = event_view.copy()

    event_view["AMOUNT_ABS"] = (event_view["ÀMOUNT"] * (-1)).abs()
    event_view["AMOUNT_SQRT"] = event_view["ÀMOUNT"].sqrt()
    event_view["AMOUNT_POW_2"] = event_view["ÀMOUNT"].pow(2)
    event_view["AMOUNT_FLOOR"] = event_view["ÀMOUNT"].floor()
    event_view["AMOUNT_CEIL"] = event_view["ÀMOUNT"].ceil()
    event_view["AMOUNT_INT_MOD_5"] = event_view["ÀMOUNT"].astype(int) % 5
    event_view["AMOUNT_LOG"] = (event_view["ÀMOUNT"] + 1).log()
    event_view["AMOUNT_LOG_EXP"] = event_view["AMOUNT_LOG"].exp()
    event_view["ONE_MINUS_AMOUNT"] = 1 - event_view["ÀMOUNT"]

    # Trigo functions
    event_view["AMOUNT_COS"] = event_view["ÀMOUNT"].cos()
    event_view["AMOUNT_SIN"] = event_view["ÀMOUNT"].sin()
    event_view["AMOUNT_TAN"] = event_view["ÀMOUNT"].tan()
    event_view["AMOUNT_ACOS"] = event_view["AMOUNT_COS"].acos()
    event_view["AMOUNT_ASIN"] = event_view["AMOUNT_COS"].asin()
    event_view["AMOUNT_ATAN"] = event_view["AMOUNT_COS"].atan()
    df = event_view.preview(limit=limit)

    pd.testing.assert_series_equal(df["AMOUNT_ABS"], (df["ÀMOUNT"] * (-1)).abs(), check_names=False)
    pd.testing.assert_series_equal(df["AMOUNT_SQRT"], np.sqrt(df["ÀMOUNT"]), check_names=False)
    pd.testing.assert_series_equal(df["AMOUNT_POW_2"], df["ÀMOUNT"].pow(2), check_names=False)
    pd.testing.assert_series_equal(
        df["AMOUNT_FLOOR"], np.floor(df["ÀMOUNT"]), check_names=False, check_dtype=False
    )
    pd.testing.assert_series_equal(
        df["AMOUNT_CEIL"], np.ceil(df["ÀMOUNT"]), check_names=False, check_dtype=False
    )
    pd.testing.assert_series_equal(
        df["AMOUNT_INT_MOD_5"].astype(int), df["ÀMOUNT"].astype(int) % 5, check_names=False
    )
    pd.testing.assert_series_equal(df["AMOUNT_LOG"], np.log(df["ÀMOUNT"] + 1), check_names=False)
    pd.testing.assert_series_equal(
        df["AMOUNT_LOG_EXP"], np.exp(np.log(df["ÀMOUNT"] + 1)), check_names=False
    )
    pd.testing.assert_series_equal(df["ONE_MINUS_AMOUNT"], 1 - df["ÀMOUNT"], check_names=False)
    pd.testing.assert_series_equal(df["AMOUNT_COS"], np.cos(df["ÀMOUNT"]), check_names=False)
    pd.testing.assert_series_equal(df["AMOUNT_SIN"], np.sin(df["ÀMOUNT"]), check_names=False)
    pd.testing.assert_series_equal(df["AMOUNT_TAN"], np.tan(df["ÀMOUNT"]), check_names=False)
    pd.testing.assert_series_equal(
        df["AMOUNT_ACOS"], np.arccos(df["AMOUNT_COS"]), check_names=False
    )
    pd.testing.assert_series_equal(
        df["AMOUNT_ASIN"], np.arcsin(df["AMOUNT_COS"]), check_names=False
    )
    pd.testing.assert_series_equal(
        df["AMOUNT_ATAN"], np.arctan(df["AMOUNT_COS"]), check_names=False
    )


def check_day_of_week_counts(event_view, preview_param):
    """Check using derived numeric column as category"""
    event_view["event_day_of_week"] = event_view["ËVENT_TIMESTAMP"].dt.day_of_week
    day_of_week_counts = event_view.groupby("ÜSER ID", category="event_day_of_week").aggregate_over(
        value_column=None,
        method="count",
        windows=["24h"],
        feature_names=["DAY_OF_WEEK_COUNTS_24h"],
    )
    day_of_week_counts["DAY_OF_WEEK_ENTROPY_24h"] = day_of_week_counts[
        "DAY_OF_WEEK_COUNTS_24h"
    ].cd.entropy()
    df_feature_preview = day_of_week_counts.preview(
        pd.DataFrame([preview_param]),
    )
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "üser id": 1,
        "DAY_OF_WEEK_COUNTS_24h": '{"0":9,"1":3,"6":2}',
        "DAY_OF_WEEK_ENTROPY_24h": 0.75893677276206,
    }
    assert_preview_result_equal(
        df_feature_preview, expected, dict_like_columns=["DAY_OF_WEEK_COUNTS_24h"]
    )


@pytest.fixture(name="non_time_based_feature")
def get_non_time_based_feature_fixture(item_table):
    """
    Get a non-time-based feature.

    This is a non-time-based feature as it is built from ItemTable.
    """
    item_view = item_table.get_view()

    # Compute count feature for only even order numbers. When the feature is added to the EventView,
    # for odd numbered order ids, the feature value should be 0 instead of NaN.
    item_view["order_number"] = item_view["order_id"].str.replace("T", "").astype(int)
    item_view = item_view[item_view["order_number"] % 2 == 0]

    return item_view.groupby("order_id").aggregate(
        value_column=None,
        method=AggFunc.COUNT,
        feature_name="non_time_count_feature",
    )


def test_add_feature(event_view, non_time_based_feature, scd_table, source_type):
    """
    Test add feature
    """
    original_column_names = [col.name for col in event_view.columns_info]

    # add feature
    event_view = event_view.add_feature(
        "transaction_count", non_time_based_feature, "TRANSACTION_ID"
    )
    event_view = event_view.add_feature(
        "transaction_count_2", non_time_based_feature, "TRANSACTION_ID"
    )

    # test columns are updated as expected
    event_view_preview = event_view.preview(5000)
    new_columns = event_view_preview.columns.tolist()
    expected_updated_column_names = [
        *original_column_names,
        "transaction_count",
        "transaction_count_2",
    ]
    assert new_columns == expected_updated_column_names

    # test that count feature have missing values
    assert event_view_preview["transaction_count"].isna().sum() > 2000
    assert event_view_preview["transaction_count"].equals(event_view_preview["transaction_count_2"])

    # test that one of the feature join keys is correct
    order_id_to_match = event_view_preview[event_view_preview["transaction_count"].notnull()].iloc[
        0
    ]["TRANSACTION_ID"]
    feature_preview = non_time_based_feature.preview(
        pd.DataFrame([{"POINT_IN_TIME": "2001-11-15 10:00:00", "order_id": order_id_to_match}])
    )
    event_view_feature_value = event_view_preview[
        event_view_preview["TRANSACTION_ID"] == order_id_to_match
    ].iloc[0]
    feature_preview_value = feature_preview["non_time_count_feature"][0]
    assert event_view_feature_value["transaction_count"] == feature_preview_value

    # test double aggregation of feature by doing an aggregation over the newly added feature column
    transaction_counts = event_view.groupby("PRODUCT_ACTION").aggregate_over(
        value_column="transaction_count",
        method="sum",
        windows=["24h"],
        feature_names=["transaction_count_sum_24h"],
        fill_value=2,
    )
    timestamp_str = "2001-01-13 12:00:00"
    df_feature_preview = transaction_counts.preview(
        pd.DataFrame([{"POINT_IN_TIME": timestamp_str, "PRODUCT_ACTION": "purchase"}]),
    )
    assert df_feature_preview.shape[0] == 1

    def _check_first_row_matches(df, expected_dict):
        # databricks return POINT_IN_TIME with "Etc/UTC" timezone
        tz_localize_if_needed(df, source_type)
        assert df.iloc[0].to_dict() == expected_dict

    _check_first_row_matches(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp(timestamp_str),
            "PRODUCT_ACTION": "purchase",
            "transaction_count_sum_24h": 56,
        },
    )

    feature_list = FeatureList([transaction_counts], name="temp_list")
    df_historical = feature_list.compute_historical_features(
        pd.DataFrame([{"POINT_IN_TIME": timestamp_str, "PRODUCT_ACTION": "purchase"}]),
    )
    _check_first_row_matches(
        df_historical,
        {
            "POINT_IN_TIME": pd.Timestamp(timestamp_str),
            "PRODUCT_ACTION": "purchase",
            "transaction_count_sum_24h": 56,
        },
    )


def test_add_feature_on_view_with_join(event_view, scd_table, non_time_based_feature):
    """
    Test add feature when the input EventView involves a join
    """
    # update the view with a join first
    scd_view = scd_table.get_view()
    event_view = event_view.join(scd_view)
    original_column_names = [col.name for col in event_view.columns_info]

    # add feature
    event_view = event_view.add_feature(
        "transaction_count", non_time_based_feature, "TRANSACTION_ID"
    )

    # ensure the updated view continues to work as expected
    event_view["User Status New"] = event_view["User Status"] + "_suffix"

    # test columns are updated as expected
    event_view_preview = event_view.sample()
    new_columns = event_view_preview.columns.tolist()
    expected_updated_column_names = [*original_column_names, "transaction_count", "User Status New"]
    assert set(new_columns) == set(expected_updated_column_names)

    # check column materialized correctly
    pd.testing.assert_series_equal(
        event_view_preview["User Status New"],
        event_view_preview["User Status"] + "_suffix",
        check_names=False,
    )

    # check pruning behaviour
    item_table_name = "ITEM_DATA_TABLE"

    # 1. transaction_count requires referencing item table
    view_subset = event_view[["transaction_count"]]
    sql = view_subset.preview_sql()
    assert item_table_name in sql
    assert view_subset.preview().columns.tolist() == view_subset.columns

    # 2. "User Status New" only requires scd table but not item table
    view_subset = event_view[["User Status New"]]
    sql = view_subset.preview_sql()
    assert item_table_name not in sql
    assert view_subset.preview().columns.tolist() == view_subset.columns


def test_latest_per_category_aggregation(event_view):
    """
    Test latest per category aggregation with value column of string type
    """
    feature_group = event_view.groupby("CUST_ID", category="ÜSER ID").aggregate_over(
        value_column="PRODUCT_ACTION",
        method="latest",
        windows=["30d"],
        feature_names=["LATEST_ACTION_DICT_30d"],
    )
    df = feature_group.preview(pd.DataFrame([{"POINT_IN_TIME": "2001-01-26", "cust_id": 545}]))
    expected = json.loads(
        '{\n  "1": "àdd",\n  "3": "purchase",\n  "5": "rëmove",\n  "8": "àdd",\n  "9": "purchase"\n}'
    )
    assert df.iloc[0]["LATEST_ACTION_DICT_30d"] == expected


@mock.patch.dict(os.environ, {"FEATUREBYTE_TILE_ID_VERSION": "1"})
def test_non_float_tile_value_added_to_tile_table(event_view, source_type):
    """
    Test case to ensure non-float tile value can be added to an existing tile table without issues
    """
    feature_group_1 = event_view.groupby("ÜSER ID").aggregate_over(
        value_column=None,
        method="count",
        windows=["2h"],
        feature_names=["COUNT_2h"],
    )
    feature_list_1 = FeatureList([feature_group_1], name="feature_list_1")
    feature_group_2 = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ËVENT_TIMESTAMP",
        method="latest",
        windows=["7d"],
        feature_names=["LATEST_EVENT_TIMESTAMP_BY_USER"],
    )
    feature_list_2 = FeatureList([feature_group_2], name="feature_list_2")

    def _get_tile_table_id(feature_obj):
        return ExtendedFeatureModel(**feature_obj.model_dump(by_alias=True)).tile_specs[0].tile_id

    assert _get_tile_table_id(feature_group_1["COUNT_2h"]) == _get_tile_table_id(
        feature_group_2["LATEST_EVENT_TIMESTAMP_BY_USER"]
    )

    # This request triggers tile table creation
    observations_set = pd.DataFrame({"POINT_IN_TIME": ["2001-01-02 10:00:00"], "üser id": 1})
    _ = feature_list_1.compute_historical_features(observations_set)

    # This request causes the tile values corresponding to latest event timestamp to be added to the
    # same tile table
    df = feature_list_2.compute_historical_features(observations_set)
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "üser id": 1,
        "LATEST_EVENT_TIMESTAMP_BY_USER": pd.Timestamp("2001-01-02 08:42:19.000673"),
    }


def test_create_observation_table(event_view):
    new_event_view = event_view.copy()
    new_event_view["POINT_IN_TIME"] = new_event_view["ËVENT_TIMESTAMP"]
    observation_table = new_event_view.create_observation_table(
        f"observation_table_name_{ObjectId()}",
        columns_rename_mapping={
            "CUST_ID": "cust_id",
            "ÜSER ID": "üser id",
            "TRANSACTION_ID": "order_id",
        },
        primary_entities=["Customer"],
    )
    expected_entity_ids = {col.entity_id for col in new_event_view.columns_info if col.entity_id}
    assert set(observation_table.entity_ids) == expected_entity_ids


def test_create_observation_table__errors_with_no_entities(event_view):
    new_event_view = event_view.copy()
    new_event_view["POINT_IN_TIME"] = new_event_view["ËVENT_TIMESTAMP"]
    new_event_view = new_event_view[["POINT_IN_TIME", "SESSION_ID"]]
    with pytest.raises(RecordCreationException) as exc:
        new_event_view.create_observation_table(
            f"observation_table_name_{ObjectId()}",
            primary_entities=[],
        )
    assert "At least one entity column" in str(exc)

    # Test that no error if we skip the entity validation check
    new_event_view.create_observation_table(
        f"observation_table_name_{ObjectId()}",
        primary_entities=[],
        skip_entity_validation_checks=True,
    )


def test_count_distinct_features(count_distinct_feature_group):
    """Test count distinct features"""
    observation_set = pd.DataFrame([{"POINT_IN_TIME": "2001-02-02 10:00:00", "order_id": "T0"}])
    feature_list = FeatureList([count_distinct_feature_group], name="test_count_distinct_fl")

    # test historical feature computation
    df_hist = feature_list.compute_historical_features(observation_set=observation_set)

    # test feature preview
    fl_preview = feature_list.preview(observation_set=observation_set)

    # check values
    expected = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-02-02 10:00:00"),
            "order_id": "T0",
            "cust_count_of_items_1w": 10,
            "cust_count_distinct_items_1w": 10,
            "cust_avg_count_of_items_per_type_1w": 1.0,
            "cust_count_distinct_item_types_1w": 10,
            "cust_count_distinct_item_types_2w": 10,
            "cust_consistency_of_item_type_1w": 1.0,
        }
    ])
    pd.testing.assert_frame_equal(df_hist, expected, check_dtype=False)
    pd.testing.assert_frame_equal(fl_preview, expected, check_dtype=False)


def test_event_view_calendar_aggregation(event_table_with_timestamp_schema, source_type):
    """
    Test calendar aggregation on EventView
    """
    event_view = event_table_with_timestamp_schema.get_view()
    feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column=None,
        method="count",
        windows=[CalendarWindow(unit=TimeIntervalUnit.MONTH, size=2)],
        feature_names=["count_calendar_2m"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 * * *",
            timezone="Asia/Singapore",
        ),
    )["count_calendar_2m"]
    feature_list = FeatureList([feature], name="my_list")

    # test historical feature computation
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-02-01 10:00:00"] * 5),
        "üser id": [1, 2, 3, 4, 5],
    })
    df_features = feature_list.compute_historical_features(observation_set=df_training_events)
    df_expected = df_training_events.copy()
    df_expected["count_calendar_2m"] = [469, 429, 474, 440, 440]
    fb_assert_frame_equal(
        df_features,
        df_expected,
        sort_by_columns=["POINT_IN_TIME", "üser id"],
    )


def test_event_view_with_timestamp_schema(event_table_with_timestamp_schema, source_type, config):
    """
    Test features on EventView with timestamp schema
    """
    event_view = event_table_with_timestamp_schema.get_view()

    feature_name = "event_table_with_timestamp_schema_count_7d"
    feature_1 = event_view.groupby("ÜSER ID").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=[feature_name],
    )[feature_name]

    feature_name = "event_table_with_timestamp_schema_count_distinct_action_7d"
    feature_2 = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="PRODUCT_ACTION",
        method="count_distinct",
        windows=["7d"],
        feature_names=[feature_name],
    )[feature_name]

    features = [feature_1, feature_2]
    feature_name = "event_table_with_timestamp_schema_latest_event_7d"
    feature_latest_event = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ËVENT_TIMESTAMP",
        method="latest",
        feature_names=[feature_name],
        windows=["7d"],
    )[feature_name]
    feature_3 = (RequestColumn.point_in_time() - feature_latest_event).dt.day
    feature_3.name = "time_since_latest_event_7d"
    features.append(feature_3)

    feature_list = FeatureList(features, name="test_event_view_with_timestamp_schema_list")

    # test historical feature computation
    df_training_events = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-02-01 10:00:00"] * 5),
        "üser id": [1, 2, 3, 4, 5],
    })
    obs_table = create_observation_table_by_upload(df_training_events)
    feature_table = feature_list.compute_historical_feature_table(obs_table, str(ObjectId()))
    df_features = feature_table.to_pandas()
    df_expected = df_training_events.copy()
    df_expected["event_table_with_timestamp_schema_count_7d"] = [124, 93, 106, 89, 95]
    df_expected["event_table_with_timestamp_schema_count_distinct_action_7d"] = [5, 5, 5, 5, 5]
    df_expected["time_since_latest_event_7d"] = [0.439398, 0.524965, 0.465752, 0.438229, 0.419919]
    fb_assert_frame_equal(
        df_features,
        df_expected,
        sort_by_columns=["POINT_IN_TIME", "üser id"],
    )

    # test deployment and serving
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    mock_datetime_value = datetime(2001, 1, 10, 12)
    with patch("featurebyte.service.feature_manager.datetime") as feature_manager_datetime:
        feature_manager_datetime.utcnow.return_value = mock_datetime_value
        with patch(
            "featurebyte.service.feature_materialize.datetime", autospec=True
        ) as feature_materialize_datetime:
            feature_materialize_datetime.utcnow.return_value = mock_datetime_value
            deployment.enable()

    entity_serving_names = [
        {
            "üser id": 1,
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)
    client = config.get_client()
    with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 10, 12)
        with patch("croniter.croniter.timestamp_to_datetime") as mock_croniter:
            mock_croniter.return_value = datetime(2001, 1, 10, 10)
            res = client.post(
                f"/deployment/{deployment.id}/online_features",
                json=data.json_dict(),
            )

    assert res.status_code == 200
    df_features = pd.DataFrame(res.json()["features"])
    df_expected = pd.DataFrame(entity_serving_names)
    df_expected["time_since_latest_event_7d"] = 0.087812
    df_expected["event_table_with_timestamp_schema_count_distinct_action_7d"] = 5
    df_expected["event_table_with_timestamp_schema_count_7d"] = 107
    fb_assert_frame_equal(df_features, df_expected)
