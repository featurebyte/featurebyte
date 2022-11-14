"""
This module contains integration tests for FeatureSnowflake
"""
import copy
from datetime import datetime, timedelta

import pandas as pd
import pytest
import pytest_asyncio
from pandas.testing import assert_frame_equal

from featurebyte.enum import InternalName
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.utils.snowflake.sql import escape_column_names


@pytest_asyncio.fixture(name="online_enabled_feature_spec")
async def online_enabled_feature_spec_fixture(
    snowflake_session,
    snowflake_feature,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager,
):
    """
    Test online_enable
    """
    feature_sql = "SELECT *, 'test_quote' FROM TEMP_TABLE"
    feature_store_table_name = "feature_store_table_1"
    expected_tile_id = snowflake_feature_expected_tile_spec_dict["tile_id"]

    online_feature_spec = OnlineFeatureSpec(
        feature=snowflake_feature,
        feature_sql=feature_sql,
        feature_store_table_name=feature_store_table_name,
    )

    await feature_manager.online_enable(online_feature_spec)

    tasks = await snowflake_session.execute_query(
        f"SHOW TASKS LIKE '%{snowflake_feature.tile_specs[0].tile_id}%'"
    )
    assert len(tasks) == 2
    assert tasks["name"].iloc[0] == f"SHELL_TASK_{expected_tile_id.upper()}_OFFLINE"
    assert tasks["schedule"].iloc[0] == "USING CRON 5 0 * * * UTC"
    assert tasks["state"].iloc[0] == "started"
    assert tasks["name"].iloc[1] == f"SHELL_TASK_{expected_tile_id.upper()}_ONLINE"
    assert tasks["schedule"].iloc[1] == "USING CRON 5-59/30 * * * * UTC"
    assert tasks["state"].iloc[1] == "started"

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{online_feature_spec.tile_ids[0]}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [online_feature_spec.tile_ids[0]],
            "FEATURE_NAME": [online_feature_spec.feature.name],
            "FEATURE_VERSION": [online_feature_spec.feature.version.to_str()],
            "FEATURE_READINESS": [str(online_feature_spec.feature.readiness)],
            "FEATURE_EVENT_DATA_IDS": [
                ",".join([str(i) for i in online_feature_spec.event_data_ids])
            ],
            "FEATURE_SQL": [feature_sql],
            "FEATURE_STORE_TABLE_NAME": [feature_store_table_name],
            "FEATURE_ENTITY_COLUMN_NAMES": [
                ",".join(escape_column_names(online_feature_spec.entity_column_names))
            ],
            "IS_DELETED": [False],
        }
    )
    result = result.drop(columns=["CREATED_AT"])
    assert_frame_equal(result, expected_df)

    yield online_feature_spec

    await snowflake_session.execute_query(
        f"DELETE FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{online_feature_spec.tile_ids[0]}'"
    )


@pytest.mark.asyncio
async def test_online_disable(
    snowflake_session,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager,
    online_enabled_feature_spec,
):
    """
    Test online_disable
    """
    online_feature_spec = online_enabled_feature_spec
    tile_id = online_feature_spec.tile_ids[0]

    await feature_manager.online_disable(online_feature_spec)

    result = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{tile_id}%'")
    assert len(result) == 2
    assert result.iloc[0]["state"] == "suspended"
    assert result.iloc[1]["state"] == "suspended"

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [tile_id],
            "FEATURE_NAME": [online_feature_spec.feature.name],
            "FEATURE_VERSION": [online_feature_spec.feature.version.to_str()],
            "IS_DELETED": [True],
        }
    )
    result = result[["TILE_ID", "FEATURE_NAME", "FEATURE_VERSION", "IS_DELETED"]]
    assert_frame_equal(result, expected_df)


@pytest.mark.asyncio
async def test_online_disable__tile_in_use(
    snowflake_session,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager,
    online_enabled_feature_spec,
):
    """
    Test online_disable
    """
    online_feature_spec = online_enabled_feature_spec
    tile_id = online_feature_spec.tile_ids[0]

    online_feature_spec_2 = copy.deepcopy(online_feature_spec)
    online_feature_spec_2.feature.name = online_feature_spec_2.feature.name + "_2"
    await feature_manager.online_enable(online_feature_spec_2)

    await feature_manager.online_disable(online_feature_spec)

    result = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{tile_id}%'")
    assert len(result) == 2
    assert result.iloc[0]["state"] == "started"
    assert result.iloc[1]["state"] == "started"

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}' AND FEATURE_NAME = '{online_feature_spec.feature.name}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [tile_id],
            "FEATURE_NAME": [online_feature_spec.feature.name],
            "FEATURE_VERSION": [online_feature_spec.feature.version.to_str()],
            "IS_DELETED": [True],
        }
    )
    result = result[["TILE_ID", "FEATURE_NAME", "FEATURE_VERSION", "IS_DELETED"]]
    assert_frame_equal(result, expected_df)

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}' AND FEATURE_NAME = '{online_feature_spec_2.feature.name}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [tile_id],
            "FEATURE_NAME": [online_feature_spec_2.feature.name],
            "FEATURE_VERSION": [
                online_feature_spec_2.feature.version.to_str(),
            ],
            "IS_DELETED": [False],
        }
    )
    result = result[["TILE_ID", "FEATURE_NAME", "FEATURE_VERSION", "IS_DELETED"]]
    assert_frame_equal(result, expected_df)


@pytest.mark.asyncio
async def test_online_disable___re_enable(
    snowflake_session,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager,
    online_enabled_feature_spec,
):
    """
    Test online_disable
    """
    online_feature_spec = online_enabled_feature_spec
    tile_id = online_feature_spec.tile_ids[0]

    await feature_manager.online_disable(online_feature_spec)

    result = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{tile_id}%'")
    assert len(result) == 2
    assert result.iloc[0]["state"] == "suspended"
    assert result.iloc[1]["state"] == "suspended"

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}' AND IS_DELETED = TRUE"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1

    # re-enable the feature jobs
    await feature_manager.online_enable(online_feature_spec)

    result = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{tile_id}%'")
    assert len(result) == 2
    assert result.iloc[0]["state"] == "started"
    assert result.iloc[1]["state"] == "started"

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}' AND IS_DELETED = FALSE"
    result = await snowflake_session.execute_query(sql)

    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [tile_id],
            "FEATURE_NAME": [online_feature_spec.feature.name],
            "FEATURE_VERSION": [online_feature_spec.feature.version.to_str()],
            "FEATURE_SQL": [online_feature_spec.feature_sql],
            "FEATURE_STORE_TABLE_NAME": [online_feature_spec.feature_store_table_name],
            "FEATURE_ENTITY_COLUMN_NAMES": [
                ",".join(escape_column_names(online_feature_spec.entity_column_names))
            ],
            "IS_DELETED": [False],
        }
    )
    result = result[
        [
            "TILE_ID",
            "FEATURE_NAME",
            "FEATURE_VERSION",
            "FEATURE_SQL",
            "FEATURE_STORE_TABLE_NAME",
            "FEATURE_ENTITY_COLUMN_NAMES",
            "IS_DELETED",
        ]
    ]
    assert_frame_equal(result, expected_df)


@pytest.mark.asyncio
async def test_get_last_tile_index(
    snowflake_feature,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager,
    tile_manager,
    online_enabled_feature_spec,
):
    """
    Test get_last_tile_index
    """
    _ = online_enabled_feature_spec

    await tile_manager.insert_tile_registry(tile_spec=snowflake_feature.tile_specs[0])
    last_index_df = await feature_manager.retrieve_last_tile_index(snowflake_feature)
    expected_tile_id = snowflake_feature_expected_tile_spec_dict["tile_id"]

    assert len(last_index_df) == 1
    assert last_index_df.iloc[0]["TILE_ID"] == expected_tile_id
    assert last_index_df.iloc[0]["LAST_TILE_INDEX_ONLINE"] == -1


@pytest.mark.asyncio
async def test_get_tile_monitor_summary(
    snowflake_feature, feature_manager, snowflake_session, online_enabled_feature_spec
):
    """
    Test retrieve_feature_tile_inconsistency_data
    """
    _ = online_enabled_feature_spec

    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    table_name = "TEMP_TABLE"
    tile_id = snowflake_feature.tile_specs[0].tile_id
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 95"
    monitor_tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 100"

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_LAST_START_DATE}', "
        f"183, 3, 5, '{entity_col_names}', '{value_col_names}', '{tile_id}', 'ONLINE', null)"
    )
    await snowflake_session.execute_query(sql)

    sql = (
        f"call SP_TILE_MONITOR('{monitor_tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, "
        f"'{entity_col_names}', '{value_col_names}', '{tile_id}', 'ONLINE')"
    )
    await snowflake_session.execute_query(sql)
    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 5

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5

    result = await feature_manager.retrieve_feature_tile_inconsistency_data(
        query_start_ts=(datetime.utcnow() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S"),
        query_end_ts=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    )
    assert len(result) == 5
    expected_df = pd.DataFrame.from_dict(
        {
            "NAME": ["sum_30m", "sum_30m"],
            "VERSION": ["v1", "v1"],
            "TILE_ID": [tile_id, tile_id],
            "TILE_START_DATE": ["2022-06-05 16:03:00", "2022-06-05 15:58:00"],
        }
    )
    result_df = result[:2][
        [
            "NAME",
            "VERSION",
            "TILE_ID",
            "TILE_START_DATE",
        ]
    ]
    result_df["TILE_START_DATE"] = result_df["TILE_START_DATE"].dt.strftime("%Y-%m-%d %H:%M:%S")

    assert_frame_equal(expected_df, result_df)
