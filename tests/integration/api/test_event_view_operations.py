"""
This module contains session to EventView integration tests
"""
import json
import os.path
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from sqlglot import expressions

from featurebyte import (
    AggFunc,
    EventTable,
    FeatureJobSetting,
    FeatureList,
    SourceType,
    to_timedelta,
)
from featurebyte.config import Configurations
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.query_graph.sql.common import get_fully_qualified_table_name, sql_to_string
from tests.util.helper import (
    assert_preview_result_equal,
    fb_assert_frame_equal,
    get_lagged_series_pandas,
    iet_entropy,
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
            event_view[f"column_{col_idx+1}"] = (
                event_view[f"column_{col_idx}"] + event_view[f"column_{col_idx+1}"]
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


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@mock.patch("featurebyte.service.feature_store_warehouse.FeatureStoreWarehouseService.list_columns")
@mock.patch("featurebyte.app.get_persistent")
def test_feature_list_saving_in_bad_state__feature_id_is_different(
    mock_persistent,
    mock_list_columns,
    mongo_persistent,
    test_dir,
    config,
    feature_store,
    mock_session_manager,
    noop_validate_feature_store_id_not_used_in_warehouse,
):
    """
    Test feature list saving in bad state due to some feature has been saved (when the feature id is different)
    """
    _ = (
        feature_store,
        mock_session_manager,
        noop_validate_feature_store_id_not_used_in_warehouse,
    )
    mock_persistent.return_value = mongo_persistent[0]
    client = Configurations().get_client()
    route_fixture_path_pairs = [
        ("/entity", "entity.json"),
        ("/feature_store", "feature_store.json"),
        ("/event_table", "event_table.json"),
    ]
    base_path = os.path.join(test_dir, "fixtures/request_payloads")
    for route, fixture_path in route_fixture_path_pairs:
        with open(f"{base_path}/{fixture_path}") as fhandle:
            payload = json.loads(fhandle.read())
            response = client.post(route, json=payload)
            assert response.status_code == 201

            if route == "/event_table":
                column_specs = [ColumnSpec(**col_info) for col_info in payload["columns_info"]]
                mock_list_columns.return_value = column_specs

    event_table = EventTable.get_by_id(id=response.json()["_id"])
    event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="10m", frequency="30m", time_modulo_frequency="5m"
        )
    )
    event_table.cust_id.as_entity("customer")
    event_view = event_table.get_view()
    feature_group = event_view.groupby("cust_id").aggregate_over(
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_2h", "COUNT_24h"],
    )
    feature_2h = feature_group["COUNT_2h"] + 123
    feature_2h.name = "COUNT_2h"
    feature_2h.save()
    assert feature_2h.saved

    # check that feature info of count aggregation is not empty
    assert feature_2h.info()["metadata"] is not None

    # resolve the error by retrieving the feature with the same name
    # (check that ID value are updated after saved)
    feature_list = FeatureList([feature_group], name="my_fl")
    feature_list.save(conflict_resolution="retrieve")
    assert feature_list[feature_2h.name].id == feature_2h.id


@pytest.fixture(name="event_view")
def event_view_fixture(event_table):
    # create event view
    event_view = event_table.get_view()
    assert event_view.columns == [
        "ËVENT_TIMESTAMP",
        "CREATED_AT",
        "CUST_ID",
        "ÜSER ID",
        "PRODUCT_ACTION",
        "SESSION_ID",
        "ÀMOUNT",
        "TRANSACTION_ID",
    ]
    return event_view


@pytest.fixture(name="feature_group")
def feature_group_fixture(event_view):
    """
    Fixture for a simple FeatureGroup with count features
    """
    event_view["derived_value_column"] = 1.0 * event_view["ÜSER ID"]
    feature_group = event_view.groupby("ÜSER ID").aggregate_over(
        method="count",
        windows=["2h", "24h"],
        feature_names=["COUNT_2h", "COUNT_24h"],
    )
    return feature_group


@pytest.fixture(name="feature_group_per_category")
def feature_group_per_category_fixture(event_view):
    """
    Fixture for a FeatureGroup with dictionary features
    """

    feature_group_per_category = event_view.groupby(
        "ÜSER ID", category="PRODUCT_ACTION"
    ).aggregate_over(
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

    return feature_group_per_category


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
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
    check_datetime_operations(event_view, "ËVENT_TIMESTAMP")

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
    if source_type == "spark":
        expected["ËVENT_TIMESTAMP"] = pd.to_datetime(
            expected["ËVENT_TIMESTAMP"], utc=True
        ).dt.tz_localize(None)
    pd.testing.assert_frame_equal(output[columns], expected[columns], check_dtype=False)


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
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


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_isnull_compare_with_bool(event_view):
    """
    Test a special case of using isnull with bool literal
    """
    filtered_view = event_view[event_view["ÀMOUNT"].isnull() == False]
    df = filtered_view.preview()
    assert df["ÀMOUNT"].notnull().all()


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_feature_operations__conditional_assign(feature_group):
    """
    Test operations on Feature objects - conditional assignment
    """
    run_test_conditional_assign_feature(feature_group)


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_feature_operations__complex_feature_preview(
    event_view, feature_group, feature_group_per_category
):
    """
    Test feature operations - complex feature preview
    """
    source_type = event_view.feature_store.type
    count_dict_supported = source_type != SourceType.DATABRICKS
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
        special_feature.save()  # pylint: disable=no-member

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
    if not count_dict_supported:
        expected.pop("COUNT_BY_ACTION_24h")
    assert_preview_result_equal(
        df_feature_preview, expected, dict_like_columns=["COUNT_BY_ACTION_24h"]
    )


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
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
    feature_group["COUNT_2h / COUNT_24h"] = new_feature
    df_feature_preview = feature_group.preview(pd.DataFrame([preview_param]))
    assert_feature_preview_output_equal(
        df_feature_preview,
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "COUNT_2h": 3,
            "COUNT_24h": 14,
            "COUNT_2h / COUNT_24h": 0.21428599999999998,
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


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
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
    check_day_of_week_counts(event_view, preview_param, source_type)


def create_feature_with_filtered_event_view(event_view):
    """
    Create a feature with filtered event view using string literal
    """
    event_view = event_view[event_view["PRODUCT_ACTION"] == "purchase"]
    feature_group = event_view.groupby("ÜSER ID").aggregate_over(
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


async def create_observation_table_from_dataframe(session, df, data_source):
    """
    Create an ObservationTable from a pandas DataFrame
    """
    unique_id = ObjectId()
    db_table_name = f"df_{unique_id}"
    await session.register_table(db_table_name, df, temporary=False)
    return data_source.get_table(
        db_table_name,
        database_name=session.database_name,
        schema_name=session.schema_name,
    ).create_observation_table(f"observation_table_{unique_id}")


async def get_dataframe_from_materialized_table(session, materialized_table):
    """
    Retrieve pandas DataFrame from a materialized table
    """
    query = sql_to_string(
        expressions.select("*").from_(
            get_fully_qualified_table_name(materialized_table.location.table_details.dict())
        ),
        source_type=session.source_type,
    )
    return await session.execute_query(query)


async def get_historical_features_async_dataframe_helper(
    feature_list, df_observation_set, session, data_source, **kwargs
):
    """
    Helper to call get_historical_features_async using DataFrame as input, converted to an
    intermediate observation table
    """
    observation_table = await create_observation_table_from_dataframe(
        session, df_observation_set, data_source
    )
    modeling_table_name = f"modeling_table_{ObjectId()}"
    modeling_table = feature_list.get_historical_features_async(
        observation_table, modeling_table_name, **kwargs
    )
    df_historical_features = await get_dataframe_from_materialized_table(session, modeling_table)
    return df_historical_features


@pytest.mark.parametrize("use_async_workflow", [True])
@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
@pytest.mark.asyncio
async def test_get_historical_features(
    session, data_source, feature_group, feature_group_per_category, use_async_workflow
):
    """
    Test getting historical features from FeatureList
    """
    feature_group["COUNT_2h / COUNT_24h"] = feature_group["COUNT_2h"] / feature_group["COUNT_24h"]
    df_training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": pd.to_datetime(["2001-01-02 10:00:00", "2001-01-02 12:00:00"] * 5),
            "üser id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
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
            "üser id": df_training_events["üser id"],
            "COUNT_2h": [3, 1, 1, 0, 0, 3, 0, 0, 1, 0],
            "COUNT_24h": [14, 12, 13, 11, 13, 18, 18, 13, 14, 0],
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

    if use_async_workflow:
        df_historical_features = await get_historical_features_async_dataframe_helper(
            feature_list=feature_list,
            df_observation_set=df_training_events,
            session=session,
            data_source=data_source,
        )
    else:
        df_historical_features = feature_list.get_historical_features(df_training_events)

    # When using fetch_pandas_all(), the dtype of "ÜSER ID" column is int8 (int64 otherwise)
    fb_assert_frame_equal(
        df_historical_features, df_historical_expected, dict_like_columns=["COUNT_BY_ACTION_24h"]
    )

    if not use_async_workflow:
        # check that making multiple request calls produces the same result
        max_batch_size = int((len(df_training_events) / 2.0) + 1)
        df_historical_multi = feature_list.get_historical_features(
            df_training_events, max_batch_size=max_batch_size
        )
        sort_cols = list(df_training_events.columns)
        fb_assert_frame_equal(
            df_historical_features.sort_values(sort_cols).reset_index(drop=True),
            df_historical_multi.sort_values(sort_cols).reset_index(drop=True),
            dict_like_columns=["COUNT_BY_ACTION_24h"],
        )

    # Test again using the same feature list and table but with serving names mapping
    await _test_get_historical_features_with_serving_names(
        feature_list,
        df_training_events,
        df_historical_expected,
        session,
        data_source,
        use_async_workflow,
    )


async def _test_get_historical_features_with_serving_names(
    feature_list,
    df_training_events,
    df_historical_expected,
    session,
    data_source,
    use_async_workflow,
):
    """Test getting historical features from FeatureList with alternative serving names"""

    mapping = {"üser id": "new_user id"}

    # Instead of providing the default serving name "user id", provide "new_user id" in table
    df_training_events = df_training_events.rename(mapping, axis=1)
    df_historical_expected = df_historical_expected.rename(mapping, axis=1)
    assert "new_user id" in df_training_events
    assert "new_user id" in df_historical_expected

    if use_async_workflow:
        df_historical_features = await get_historical_features_async_dataframe_helper(
            feature_list=feature_list,
            df_observation_set=df_training_events,
            session=session,
            data_source=data_source,
            serving_names_mapping=mapping,
        )
    else:
        df_historical_features = feature_list.get_historical_features(
            df_training_events,
            serving_names_mapping=mapping,
        )
    fb_assert_frame_equal(
        df_historical_features,
        df_historical_expected,
        dict_like_columns=["COUNT_BY_ACTION_24h"],
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

    df_filtered_col = event_view_filtered["event_interval_second"].preview()
    assert (df_filtered_col["event_interval_second"] > 500000).all()

    # check datetime extracted properties
    dt_df = event_view.preview(limit=limit)
    pandas_series = dt_df[column_name]
    for prop in properties:
        series_prop = pandas_series.apply(lambda x: getattr(x, prop))
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


def test_datetime_comparison__fixed_timestamp_non_tz(event_view, source_type):
    """Test datetime comparison with a fixed timestamp (non-tz)"""

    timestamp_column = "ËVENT_TIMESTAMP"

    # Fixed timestamp without timezone - assumed to be in UTC
    fixed_timestamp_non_tz = pd.Timestamp("2001-01-15 10:00:00")
    event_view["result"] = event_view[timestamp_column] > fixed_timestamp_non_tz

    df = event_view.preview(limit=100)

    if source_type == "snowflake":
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

    if source_type == "snowflake":
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


def check_day_of_week_counts(event_view, preview_param, source_type):
    """Check using derived numeric column as category"""
    event_view["event_day_of_week"] = event_view["ËVENT_TIMESTAMP"].dt.day_of_week
    day_of_week_counts = event_view.groupby("ÜSER ID", category="event_day_of_week").aggregate_over(
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
    if source_type == "snowflake":
        expected_counts = '{\n  "0": 4,\n  "1": 9,\n  "2": 1\n}'
        expected_entropy = 0.830471712436292
    else:
        expected_counts = '{"0": 9, "1": 5}'
        expected_entropy = 0.651756561172653
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "üser id": 1,
        "DAY_OF_WEEK_COUNTS_24h": expected_counts,
        "DAY_OF_WEEK_ENTROPY_24h": expected_entropy,
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
        method=AggFunc.COUNT,
        feature_name="non_time_count_feature",
    )


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_add_feature(event_view, non_time_based_feature, scd_table):
    """
    Test add feature
    """
    original_column_names = [col.name for col in event_view.columns_info]

    # add feature
    event_view.add_feature("transaction_count", non_time_based_feature, "TRANSACTION_ID")
    event_view.add_feature("transaction_count_2", non_time_based_feature, "TRANSACTION_ID")

    # test columns are updated as expected
    event_view_preview = event_view.preview(5000)
    new_columns = event_view_preview.columns.tolist()
    expected_updated_column_names = [
        *original_column_names,
        "transaction_count",
        "transaction_count_2",
    ]
    assert new_columns == expected_updated_column_names

    # test that count feature should not have any missing values
    assert event_view_preview["transaction_count"].isna().sum() == 0
    assert event_view_preview["transaction_count"].equals(event_view_preview["transaction_count_2"])

    # test that one of the feature join keys is correct
    order_id_to_match = "T0"
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
    assert df_feature_preview.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp(timestamp_str),
        "PRODUCT_ACTION": "purchase",
        "transaction_count_sum_24h": 56,
    }


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_add_feature_on_view_with_join(event_view, scd_table, non_time_based_feature):
    """
    Test add feature when the input EventView involves a join
    """
    # update the view with a join first
    scd_view = scd_table.get_view()
    event_view.join(scd_view)
    original_column_names = [col.name for col in event_view.columns_info]

    # add feature
    event_view.add_feature("transaction_count", non_time_based_feature, "TRANSACTION_ID")

    # ensure the updated view continues to work as expected
    event_view["User Status New"] = event_view["User Status"] + "_suffix"

    # test columns are updated as expected
    event_view_preview = event_view.sample()
    new_columns = event_view_preview.columns.tolist()
    expected_updated_column_names = [*original_column_names, "transaction_count", "User Status New"]
    assert new_columns == expected_updated_column_names

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


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
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
    assert json.loads(df.iloc[0]["LATEST_ACTION_DICT_30d"]) == expected


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
def test_non_float_tile_value_added_to_tile_table(event_view, source_type):
    """
    Test case to ensure non-float tile value can be added to an existing tile table without issues
    """
    feature_group_1 = event_view.groupby("ÜSER ID").aggregate_over(
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
        return ExtendedFeatureModel(**feature_obj.dict()).tile_specs[0].tile_id

    assert _get_tile_table_id(feature_group_1["COUNT_2h"]) == _get_tile_table_id(
        feature_group_2["LATEST_EVENT_TIMESTAMP_BY_USER"]
    )

    # This request triggers tile table creation
    observations_set = pd.DataFrame({"POINT_IN_TIME": ["2001-01-02 10:00:00"], "üser id": 1})
    _ = feature_list_1.get_historical_features(observations_set)

    # This request causes the tile values corresponding to latest event timestamp to be added to the
    # same tile table
    df = feature_list_2.get_historical_features(observations_set)

    expected_feature_value = pd.Timestamp("2001-01-02 08:42:19.000673+0000", tz="UTC")
    if source_type == "spark":
        expected_feature_value = expected_feature_value.tz_convert(None)
    assert df.iloc[0].to_dict() == {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "üser id": 1,
        "LATEST_EVENT_TIMESTAMP_BY_USER": expected_feature_value,
    }
