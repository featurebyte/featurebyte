"""
This module contains integration tests for FeatureSnowflake
"""
import copy
from datetime import datetime, timedelta
from unittest.mock import PropertyMock, patch

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
from pandas.testing import assert_frame_equal

from featurebyte.common import date_util
from featurebyte.enum import InternalName
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.utils.snowflake.sql import escape_column_names


@pytest.fixture(name="feature_sql")
def feature_sql_fixture():
    """
    Feature sql fixture
    """
    return "SELECT *, 'test_quote' FROM TEMP_TABLE"


@pytest.fixture(name="feature_store_table_name")
def feature_store_table_name_fixture():
    """
    Feature store table name fixture
    """
    feature_store_table_name = "feature_store_table_1"
    return feature_store_table_name


@pytest_asyncio.fixture(name="online_enabled_feature_spec")
async def online_enabled_feature_spec_fixture(
    snowflake_session,
    snowflake_feature,
    feature_manager,
    feature_sql,
    feature_store_table_name,
    snowflake_tile,
):
    """
    Fixture for an OnlineFeatureSpec corresponding to an online enabled feature
    """
    with patch.object(OnlineFeatureSpec, "feature_sql", PropertyMock(return_value=feature_sql)):
        with patch.object(
            OnlineFeatureSpec,
            "feature_store_table_name",
            PropertyMock(return_value=feature_store_table_name),
        ):
            with patch.object(
                ExtendedFeatureModel, "tile_specs", PropertyMock(return_value=[snowflake_tile])
            ):

                online_feature_spec = OnlineFeatureSpec(
                    feature=snowflake_feature,
                )

                schedule_time = datetime.utcnow()

                await feature_manager.online_enable(
                    online_feature_spec, schedule_time=schedule_time
                )

                yield online_feature_spec, schedule_time

                await snowflake_session.execute_query(
                    f"DELETE FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{online_feature_spec.tile_ids[0]}'"
                )


@pytest.mark.asyncio
async def test_online_enabled_feature_spec(
    online_enabled_feature_spec,
    snowflake_session,
    snowflake_feature,
    snowflake_tile,
    feature_manager,
    feature_sql,
    feature_store_table_name,
):
    """
    Test online_enable
    """
    online_feature_spec, schedule_time = online_enabled_feature_spec
    expected_tile_id = snowflake_tile.tile_id

    next_job_time_online = date_util.get_next_job_datetime(
        input_dt=schedule_time,
        frequency_minutes=snowflake_tile.frequency_minute,
        time_modulo_frequency_seconds=snowflake_tile.time_modulo_frequency_second,
    )

    next_job_time_offline = date_util.get_next_job_datetime(
        input_dt=schedule_time,
        frequency_minutes=1440,
        time_modulo_frequency_seconds=snowflake_tile.time_modulo_frequency_second,
    )

    tasks = await snowflake_session.execute_query(
        f"SHOW TASKS LIKE '%{snowflake_feature.tile_specs[0].tile_id}%'"
    )
    assert len(tasks) == 2
    assert tasks["name"].iloc[0] == f"SHELL_TASK_{expected_tile_id}_OFFLINE"
    assert (
        tasks["schedule"].iloc[0]
        == f"USING CRON {next_job_time_offline.minute} {next_job_time_offline.hour} {next_job_time_offline.day} * * UTC"
    )
    assert tasks["state"].iloc[0] == "started"
    assert tasks["name"].iloc[1] == f"SHELL_TASK_{expected_tile_id}_ONLINE"
    assert (
        tasks["schedule"].iloc[1]
        == f"USING CRON {next_job_time_online.minute} {next_job_time_online.hour} {next_job_time_online.day} * * UTC"
    )
    assert tasks["state"].iloc[1] == "started"

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{expected_tile_id}'"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [expected_tile_id],
            "FEATURE_NAME": [online_feature_spec.feature.name],
            "FEATURE_TYPE": ["FLOAT"],
            "FEATURE_VERSION": [online_feature_spec.feature.version.to_str()],
            "FEATURE_READINESS": [str(online_feature_spec.feature.readiness)],
            "FEATURE_EVENT_DATA_IDS": [
                ",".join([str(i) for i in online_feature_spec.event_data_ids])
            ],
            "FEATURE_SQL": [feature_sql],
            "FEATURE_STORE_TABLE_NAME": [feature_store_table_name],
            "FEATURE_ENTITY_COLUMN_NAMES": [
                ",".join(escape_column_names(online_feature_spec.serving_names))
            ],
            "IS_DELETED": [False],
        }
    )
    result = result.drop(columns=["CREATED_AT"])
    assert_frame_equal(result, expected_df)

    # validate as result of calling SP_TILE_GENERATE to generate historical tiles
    sql = f"SELECT * FROM {expected_tile_id}"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 100
    expected_df = pd.DataFrame(
        {
            "INDEX": np.array([5514911, 5514910, 5514909], dtype=np.int32),
            "PRODUCT_ACTION": ["view", "view", "view"],
            "CUST_ID": np.array([1, 1, 1], dtype=np.int8),
            "VALUE": np.array([5, 2, 2], dtype=np.int8),
        }
    )
    result = result[:3].drop(columns=["CREATED_AT"])
    assert_frame_equal(result, expected_df)


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
    online_feature_spec, _ = online_enabled_feature_spec
    tile_id = online_feature_spec.tile_ids[0]

    await feature_manager.online_disable(online_feature_spec)

    result = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{tile_id}%'")
    assert len(result) == 0

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
    online_feature_spec, _ = online_enabled_feature_spec
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
    online_feature_spec, _ = online_enabled_feature_spec
    tile_id = online_feature_spec.tile_ids[0]

    await feature_manager.online_disable(online_feature_spec)

    result = await snowflake_session.execute_query(f"SHOW TASKS LIKE '%{tile_id}%'")
    assert len(result) == 0

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
                ",".join(escape_column_names(online_feature_spec.serving_names))
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

    assert len(last_index_df) == 1
    assert last_index_df.iloc[0]["TILE_ID"] == "TILE_ID1"
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
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = snowflake_feature.tile_specs[0].tile_id
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 95"
    monitor_tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 100"

    await snowflake_session.execute_query(f"DROP TABLE IF EXISTS {tile_id}")

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_LAST_START_DATE}', "
        f"183, 3, 5, '{entity_col_names}', '{value_col_names}', '{value_col_types}', '{tile_id}', 'ONLINE', null)"
    )
    await snowflake_session.execute_query(sql)

    sql = (
        f"call SP_TILE_MONITOR('{monitor_tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, "
        f"'{entity_col_names}', '{value_col_names}', '{value_col_types}', '{tile_id}', 'ONLINE')"
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
