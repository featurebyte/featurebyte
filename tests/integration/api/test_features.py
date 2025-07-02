"""
Tests for more features
"""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from sqlglot import expressions

from featurebyte import (
    Entity,
    FeatureJobSetting,
    FeatureList,
    RequestColumn,
    SourceType,
    TimestampSchema,
)
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import quoted_identifier
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY
from tests.util.deployment import deploy_and_get_online_features
from tests.util.helper import (
    create_observation_table_from_dataframe,
    fb_assert_frame_equal,
    tz_localize_if_needed,
)


def test_features_without_entity(event_table):
    """
    Test working with purely time based features without any entity
    """
    event_view = event_table.get_view()
    feature_group = event_view.groupby([]).aggregate_over(
        value_column=None,
        method="count",
        windows=["2h", "24h"],
        feature_names=["ALL_COUNT_2h", "ALL_COUNT_24h"],
    )
    df = pd.DataFrame({
        "POINT_IN_TIME": pd.date_range("2001-01-01", periods=5, freq="d"),
    })

    feature_list_1 = FeatureList([feature_group["ALL_COUNT_2h"]], name="all_count_2h_list")
    feature_list_2 = FeatureList([feature_group["ALL_COUNT_24h"]], name="all_count_24h_list")

    # Test historical features with on demand tile generation
    df_features_1 = (
        feature_list_1.compute_historical_features(df)
        .sort_values("POINT_IN_TIME")
        .reset_index(drop=True)
    )
    df_features_2 = (
        feature_list_2.compute_historical_features(df)
        .sort_values("POINT_IN_TIME")
        .reset_index(drop=True)
    )

    # Test saving and deploying
    deployment_1, deployment_2 = None, None
    try:
        feature_list_1.save()
        deployment_1 = feature_list_1.deploy(make_production_ready=True)
        deployment_1.enable()

        feature_list_2.save()
        deployment_2 = feature_list_2.deploy(make_production_ready=True)
        deployment_2.enable()

        # Test getting historical requests
        df_features_deployed_1 = (
            feature_list_1.compute_historical_features(df)
            .sort_values("POINT_IN_TIME")
            .reset_index(drop=True)
        )
        df_features_deployed_2 = (
            feature_list_2.compute_historical_features(df)
            .sort_values("POINT_IN_TIME")
            .reset_index(drop=True)
        )
    finally:
        for deployment in [deployment_1, deployment_2]:
            if deployment:
                deployment.disable()

    pd.testing.assert_frame_equal(df_features_1, df_features_deployed_1)
    pd.testing.assert_frame_equal(df_features_2, df_features_deployed_2)


def test_combined_simple_aggregate_and_window_aggregate(event_table, item_table, source_type):
    """
    Test combining simple aggregate and window aggregate
    """
    event_view = event_table.get_view()
    item_view = item_table.get_view()
    item_feature = item_view.groupby("order_id").aggregate(
        value_column=None, method="count", feature_name="my_item_feature"
    )
    event_view = event_view.add_feature("added_feature", item_feature, "TRANSACTION_ID")

    window_feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="added_feature",
        method="sum",
        windows=["7d"],
        feature_names=["added_feature_sum_7d"],
    )["added_feature_sum_7d"]

    feature = window_feature + item_feature
    feature.name = "combined_feature"
    df_observation = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-11-15 10:00:00"]),
        "order_id": ["T1"],
    })
    df_preview = feature.preview(df_observation)
    tz_localize_if_needed(df_preview, source_type)
    expected = [
        {
            "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
            "order_id": "T1",
            "combined_feature": 69,
        }
    ]
    assert df_preview.to_dict("records") == expected

    # preview using observation table
    observation_table = item_view.create_observation_table(
        "OBSERVATION_TABLE_FROM_ITEM_VIEW_FOR_PREVIEW",
        sample_rows=5,
        columns=[event_view.timestamp_column, "order_id"],
        columns_rename_mapping={event_view.timestamp_column: "POINT_IN_TIME"},
    )
    df_feature_preview = feature.preview(observation_table)
    tz_localize_if_needed(df_feature_preview, source_type)
    assert df_feature_preview.shape[0] <= 5
    assert df_feature_preview.shape[1] == 3

    feature_list = FeatureList([feature], name="my_feature_list")
    df_feature_list_preview = feature_list.preview(observation_table)
    tz_localize_if_needed(df_feature_list_preview, source_type)
    fb_assert_frame_equal(
        df_feature_preview, df_feature_list_preview, sort_by_columns=["POINT_IN_TIME", "order_id"]
    )


def test_preview_with_numpy_array(item_table, source_type):
    item_view = item_table.get_view()
    item_feature = item_view.groupby("order_id").aggregate(
        value_column=None, method="count", feature_name="my_item_feature"
    )
    df_observation = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2001-11-15 10:00:00"]),
        "order_id": ["T1"],
        "array_field": [np.array([0.0, 1.0])],
    })
    df_preview = item_feature.preview(df_observation)
    tz_localize_if_needed(df_preview, source_type)

    expected = [
        {
            "POINT_IN_TIME": pd.Timestamp("2001-11-15 10:00:00"),
            "array_field": [0, 1],
            "my_item_feature": 3,
            "order_id": "T1",
        }
    ]
    assert df_preview.to_dict("records") == expected


def test_relative_frequency_with_filter(event_table, scd_table):
    """
    Test complex feature involving an aggregation feature and a lookup feature, one with filter and
    another without
    """
    event_view = event_table.get_view()
    scd_view = scd_table.get_view()

    event_view = event_view.join(scd_view, on="ÜSER ID")
    lookup_feature = event_view["User Status"].as_feature("User Status Feature")

    filtered_view = event_view[event_view["ÀMOUNT"] > 2]
    dict_feature = filtered_view.groupby("ÜSER ID", category="User Status").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=["my_feature"],
    )["my_feature"]

    feature = dict_feature.cd.get_relative_frequency(lookup_feature)
    feature.name = "complex_feature"
    feature_list = FeatureList([feature], name="my_feature_list")
    preview_param = {
        "POINT_IN_TIME": "2002-01-02 10:00:00",
        "order_id": "T4390",
        "üser id": 1,
    }
    observations_set = pd.DataFrame([preview_param])
    df = feature_list.compute_historical_features(observations_set)
    expected = [
        {
            "POINT_IN_TIME": pd.Timestamp("2002-01-02 10:00:00"),
            "order_id": "T4390",
            "üser id": 1,
            "complex_feature": 0.125,
        }
    ]
    assert df.to_dict("records") == expected


def test_relative_frequency_with_non_string_keys(event_table, scd_table):
    """
    Test relative frequency with non-string keys
    """
    event_view = event_table.get_view()
    scd_view = scd_table.get_view()

    event_view["non_string_key"] = event_view["ËVENT_TIMESTAMP"].dt.day_of_week
    scd_view["non_string_key"] = scd_view["Effective Timestamp"].dt.day_of_week

    dict_feature = event_view.groupby("ÜSER ID", category="non_string_key").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=["dict_feature"],
    )["dict_feature"]
    key_feature = scd_view["non_string_key"].as_feature("non_string_key_feature")
    feature = dict_feature.cd.get_relative_frequency(key_feature)
    feature.name = "final_feature"
    feature_list = FeatureList([dict_feature, key_feature, feature], name="feature_list")

    preview_param = {
        "POINT_IN_TIME": "2002-01-02 10:00:00",
        "üser id": 1,
    }
    observations_set = pd.DataFrame([preview_param])
    df = feature_list.compute_historical_features(observations_set)
    expected = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2002-01-02 10:00:00"),
            "üser id": 1,
            "dict_feature": {"0": 3, "1": 2, "2": 2, "3": 1, "4": 1, "5": 2},
            "non_string_key_feature": 1,
            "final_feature": 0.181818,
        }
    ])
    fb_assert_frame_equal(df, expected, dict_like_columns=["dict_feature"])


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
@pytest.mark.asyncio
async def test_latest_array_with_feature_table_cache(event_table, session, data_source):
    """
    Test latest feature with array type source column
    """
    event_view = event_table.get_view()

    feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ARRAY_STRING",
        method="latest",
        windows=["14d"],
        feature_names=["latest_array_string_feature"],
    )["latest_array_string_feature"]
    feature_2 = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ARRAY",
        method="latest",
        windows=["14d"],
        feature_names=["latest_array_feature"],
    )["latest_array_feature"]
    feature_list = FeatureList([feature, feature_2], name="latest_array_feature_list")
    preview_param = {
        "POINT_IN_TIME": pd.Timestamp("2002-01-02 10:00:00"),
        "üser id": 1,
    }
    observations_df = pd.DataFrame([preview_param])
    observation_table = await create_observation_table_from_dataframe(
        session, observations_df, data_source
    )
    df = feature_list.compute_historical_feature_table(
        observation_table, f"table_{ObjectId()}"
    ).to_pandas()
    if session.source_type != SourceType.SNOWFLAKE:
        expected_latest_array_feature = [
            "0.5198448427193927",
            "0.130800057877837",
            "0.8889922844595354",
            "0.14526240204589713",
            "0.051483377270627906",
        ]
    else:
        expected_latest_array_feature = [
            0.5198448427193927,
            0.130800057877837,
            0.8889922844595354,
            0.1452624020458971,
            0.05148337727062791,
        ]
    expected = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2002-01-02 10:00:00"),
            "üser id": 1,
            "latest_array_string_feature": ["a", "b", "c"],
            "latest_array_feature": expected_latest_array_feature,
        }
    ])
    fb_assert_frame_equal(df, expected)


@pytest.mark.asyncio
async def test_date_type_in_offline_store_tables(
    session_without_datasets, data_source, source_type, config
):
    """
    Test that DATE type columns in offline store tables can be used in features
    """
    session = session_without_datasets
    df_events = pd.DataFrame({
        "ts": pd.to_datetime([
            "2022-04-10",
            "2022-04-15",
            "2022-04-20",
            "2022-04-25",
            "2022-04-30",
        ]),
        "event_id": [1, 2, 3, 4, 5],
        "cust_id": [1000, 1000, 1000, 1000, 1000],
    })
    table_prefix = f"id_{str(ObjectId())}".upper()
    table_name = f"{table_prefix}_EVENT"
    await session.register_table(table_name, df_events)

    # Create a table with DATE type column
    select_expr = expressions.select(
        expressions.alias_(
            expressions.Cast(
                this=quoted_identifier("ts"),
                to=expressions.DataType.build("DATE"),
            ),
            alias="ts",
            quoted=True,
        ),
        quoted_identifier("event_id"),
        quoted_identifier("cust_id"),
    ).from_(quoted_identifier(table_name))
    await session.create_table_as(
        TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name=table_name,
        ),
        select_expr,
        replace=True,
    )

    event_source_table = data_source.get_source_table(
        table_name=f"{table_prefix}_EVENT",
        database_name=session.database_name,
        schema_name=session.schema_name,
    )
    event_table = event_source_table.create_event_table(
        name=f"{source_type}_{table_prefix}_EVENT_DATA",
        event_id_column="event_id",
        event_timestamp_column="ts",
        event_timestamp_schema=TimestampSchema(is_utc=True),
    )
    event_table.update_default_feature_job_setting(
        FeatureJobSetting(period="24h", offset="1h", blind_spot="2h"),
    )
    serving_name = f"{table_prefix}_customer_id"
    customer_entity = Entity.create(f"{table_prefix}_customer", [serving_name])
    event_table["cust_id"].as_entity(customer_entity.name)
    event_view = event_table.get_view()
    date_feature = event_view.groupby("cust_id").aggregate_over(
        "ts",
        "latest",
        windows=["90d"],
        feature_names=["latest_event_ts_90d"],
    )["latest_event_ts_90d"]
    feature = (RequestColumn.point_in_time() - date_feature).dt.hour
    feature.name = "test_date_type_in_offline_store_tables_feature"
    feature_list = FeatureList([feature], name="test_date_type_in_offline_store_tables")
    online_features = deploy_and_get_online_features(
        config.get_client(),
        feature_list,
        datetime(2022, 5, 3),
        [{serving_name: 1000}],
    )
    assert online_features.iloc[0]["test_date_type_in_offline_store_tables_feature"] == 72.0
