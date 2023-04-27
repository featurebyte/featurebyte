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

from featurebyte.enum import InternalName
from featurebyte.feature_manager.manager import FeatureManager
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.models.periodic_task import Interval
from featurebyte.models.tile import TileType
from featurebyte.query_graph.sql.online_serving import OnlineStorePrecomputeQuery
from featurebyte.service.task_manager import TaskManager
from featurebyte.tile.scheduler import TileScheduler


@pytest.fixture(name="feature_sql")
def feature_sql_fixture():
    """
    Feature sql fixture
    """
    return f"""
    SELECT *, 'test_quote', 1 as "sum_30m", 2 as "sum_30m_2", row_number() over (order by CUST_ID desc) as "cust_id" FROM TEMP_TABLE
"""


@pytest.fixture(name="feature_store_table_name")
def feature_store_table_name_fixture():
    """
    Feature store table name fixture
    """
    feature_store_table_name = "feature_store_table_1"
    return feature_store_table_name


@pytest.fixture(name="feature_manager_no_sf_scheduling")
def snowflake_feature_manager_no_sf_scheduling_fixture(extended_feature_model, persistent, session):
    """
    Feature store table name fixture
    """
    feature_manager = FeatureManager(session=session)
    task_manager = TaskManager(
        user=User(id=extended_feature_model.user_id),
        persistent=persistent,
        catalog_id=DEFAULT_CATALOG_ID,
    )
    feature_manager._tile_manager._task_manager = task_manager

    return feature_manager


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enabled__without_snowflake_scheduling(
    session,
    extended_feature_model,
    tile_spec,
    feature_manager_no_sf_scheduling,
    feature_sql,
    feature_store_table_name,
    persistent,
):
    with patch.object(ExtendedFeatureModel, "tile_specs", PropertyMock(return_value=[tile_spec])):
        mock_precompute_queries = [
            OnlineStorePrecomputeQuery(
                sql=feature_sql,
                tile_id=tile_spec.tile_id,
                aggregation_id=tile_spec.aggregation_id,
                table_name=feature_store_table_name,
                result_name="sum_30m",
                result_type="FLOAT",
                serving_names=["cust_id"],
            )
        ]
        online_feature_spec = OnlineFeatureSpec(
            feature=extended_feature_model, precompute_queries=mock_precompute_queries
        )
        schedule_time = datetime.utcnow()

        try:
            await feature_manager_no_sf_scheduling.online_enable(
                online_feature_spec, schedule_time=schedule_time
            )

            tile_scheduler = TileScheduler(
                task_manager=feature_manager_no_sf_scheduling._tile_manager._task_manager
            )
            # check if the task is scheduled
            job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"
            job_details = await tile_scheduler.get_job_details(job_id=job_id)
            assert job_details is not None
            assert job_details.name == job_id
            assert job_details.interval == Interval(
                every=tile_spec.frequency_minute * 60, period="seconds"
            )

            # check if the task is not scheduled in snowflake
            tasks = await session.execute_query(
                f"SHOW TASKS LIKE '%{extended_feature_model.tile_specs[0].aggregation_id}%'"
            )
            assert len(tasks) == 0

        finally:
            await feature_manager_no_sf_scheduling.online_disable(online_feature_spec)
            await session.execute_query(
                f"DELETE FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{online_feature_spec.tile_ids[0]}'"
            )
            await session.execute_query(
                f"DELETE FROM ONLINE_STORE_MAPPING WHERE TILE_ID = '{online_feature_spec.tile_ids[0]}'"
            )


@pytest_asyncio.fixture(name="online_enabled_feature_spec")
async def online_enabled_feature_spec_fixture(
    session,
    extended_feature_model,
    feature_manager_no_sf_scheduling,
    feature_sql,
    feature_store_table_name,
    tile_spec,
):
    """
    Fixture for an OnlineFeatureSpec corresponding to an online enabled feature
    """
    # this fixture supports only snowflake
    assert session.source_type == "snowflake"

    with patch.object(ExtendedFeatureModel, "tile_specs", PropertyMock(return_value=[tile_spec])):
        mock_precompute_queries = [
            OnlineStorePrecomputeQuery(
                sql=feature_sql,
                tile_id=tile_spec.tile_id,
                aggregation_id=tile_spec.aggregation_id,
                table_name=feature_store_table_name,
                result_name="sum_30m",
                result_type="FLOAT",
                serving_names=["cust_id"],
            )
        ]
        online_feature_spec = OnlineFeatureSpec(
            feature=extended_feature_model, precompute_queries=mock_precompute_queries
        )
        schedule_time = datetime.utcnow()

        await feature_manager_no_sf_scheduling.online_enable(
            online_feature_spec, schedule_time=schedule_time
        )

        yield online_feature_spec, schedule_time

        await feature_manager_no_sf_scheduling.online_disable(online_feature_spec)
        await session.execute_query(
            f"DELETE FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{online_feature_spec.tile_ids[0]}'"
        )
        await session.execute_query(
            f"DELETE FROM ONLINE_STORE_MAPPING WHERE TILE_ID = '{online_feature_spec.tile_ids[0]}'"
        )


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enabled_feature_spec(
    online_enabled_feature_spec,
    session,
    tile_spec,
    feature_sql,
    feature_store_table_name,
):
    """
    Test online_enable
    """
    assert session.source_type == "snowflake"

    online_feature_spec, schedule_time = online_enabled_feature_spec
    expected_tile_id = tile_spec.tile_id
    expected_aggregation_id = tile_spec.aggregation_id

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{expected_tile_id}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [expected_tile_id],
            "AGGREGATION_ID": [expected_aggregation_id],
            "FEATURE_NAME": [online_feature_spec.feature.name],
            "FEATURE_TYPE": ["FLOAT"],
            "FEATURE_VERSION": [online_feature_spec.feature.version.to_str()],
            "FEATURE_READINESS": [str(online_feature_spec.feature.readiness)],
            "FEATURE_EVENT_DATA_IDS": [
                ",".join([str(i) for i in online_feature_spec.event_table_ids])
            ],
            "IS_DELETED": [False],
        }
    )
    result = result.drop(columns=["CREATED_AT"])
    assert_frame_equal(result, expected_df)

    sql = f"SELECT * FROM ONLINE_STORE_MAPPING WHERE TILE_ID = '{expected_tile_id}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [expected_tile_id],
            "AGGREGATION_ID": [expected_aggregation_id],
            "RESULT_ID": ["sum_30m"],
            "RESULT_TYPE": ["FLOAT"],
            "SQL_QUERY": feature_sql,
            "ONLINE_STORE_TABLE_NAME": feature_store_table_name,
            "ENTITY_COLUMN_NAMES": ['"cust_id"'],
            "IS_DELETED": [False],
        }
    )
    result = result.drop(columns=["CREATED_AT"])
    assert_frame_equal(result, expected_df)

    # validate generate historical tiles
    sql = f"SELECT * FROM {expected_tile_id}"
    result = await session.execute_query(sql)
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

    # validate populate Online Store result
    sql = f"SELECT * FROM {feature_store_table_name}"
    result = await session.execute_query(sql)
    assert len(result) == 100
    expect_cols = online_feature_spec.precompute_queries[0].serving_names[:]
    expect_cols.append(online_feature_spec.feature.name)
    assert list(result)[:2] == expect_cols


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_disable(
    session,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager_no_sf_scheduling,
    online_enabled_feature_spec,
):
    """
    Test online_disable
    """
    assert session.source_type == "snowflake"

    online_feature_spec, _ = online_enabled_feature_spec
    tile_id = online_feature_spec.tile_ids[0]

    await feature_manager_no_sf_scheduling.online_disable(online_feature_spec)

    result = await session.execute_query(f"SHOW TASKS LIKE '%{tile_id}%'")
    assert len(result) == 0

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
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


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_disable__tile_in_use(
    session,
    snowflake_feature_expected_tile_spec_dict,
    online_enabled_feature_spec,
    feature_manager_no_sf_scheduling,
):
    """
    Test online_disable
    """
    assert session.source_type == "snowflake"

    online_feature_spec, _ = online_enabled_feature_spec
    tile_id = online_feature_spec.tile_ids[0]
    aggregation_id = online_feature_spec.aggregation_ids[0]

    online_feature_spec_2 = copy.deepcopy(online_feature_spec)
    online_feature_spec_2.feature.name = online_feature_spec_2.feature.name + "_2"
    await feature_manager_no_sf_scheduling.online_enable(online_feature_spec_2)

    await feature_manager_no_sf_scheduling.online_disable(online_feature_spec)

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}' AND FEATURE_NAME = '{online_feature_spec.feature.name}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [tile_id],
            "AGGREGATION_ID": [aggregation_id],
            "FEATURE_NAME": [online_feature_spec.feature.name],
            "FEATURE_VERSION": [online_feature_spec.feature.version.to_str()],
            "IS_DELETED": [True],
        }
    )
    result = result[["TILE_ID", "AGGREGATION_ID", "FEATURE_NAME", "FEATURE_VERSION", "IS_DELETED"]]
    assert_frame_equal(result, expected_df)

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}' AND FEATURE_NAME = '{online_feature_spec_2.feature.name}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [tile_id],
            "AGGREGATION_ID": [aggregation_id],
            "FEATURE_NAME": [online_feature_spec_2.feature.name],
            "FEATURE_VERSION": [
                online_feature_spec_2.feature.version.to_str(),
            ],
            "IS_DELETED": [False],
        }
    )
    result = result[["TILE_ID", "AGGREGATION_ID", "FEATURE_NAME", "FEATURE_VERSION", "IS_DELETED"]]
    assert_frame_equal(result, expected_df)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_disable___re_enable(
    session,
    snowflake_feature_expected_tile_spec_dict,
    feature_manager_no_sf_scheduling,
    online_enabled_feature_spec,
):
    """
    Test online_disable
    """
    assert session.source_type == "snowflake"

    online_feature_spec, _ = online_enabled_feature_spec
    tile_id = online_feature_spec.tile_ids[0]
    aggregation_id = online_feature_spec.aggregation_ids[0]

    await feature_manager_no_sf_scheduling.online_disable(online_feature_spec)

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}' AND IS_DELETED = TRUE"
    result = await session.execute_query(sql)
    assert len(result) == 1

    # re-enable the feature jobs
    await feature_manager_no_sf_scheduling.online_enable(online_feature_spec)

    sql = f"SELECT * FROM TILE_FEATURE_MAPPING WHERE TILE_ID = '{tile_id}' AND IS_DELETED = FALSE"
    result = await session.execute_query(sql)

    assert len(result) == 1
    expected_df = pd.DataFrame(
        {
            "TILE_ID": [tile_id],
            "AGGREGATION_ID": [aggregation_id],
            "FEATURE_NAME": [online_feature_spec.feature.name],
            "FEATURE_VERSION": [online_feature_spec.feature.version.to_str()],
            "IS_DELETED": [False],
        }
    )
    result = result[
        [
            "TILE_ID",
            "AGGREGATION_ID",
            "FEATURE_NAME",
            "FEATURE_VERSION",
            "IS_DELETED",
        ]
    ]
    assert_frame_equal(result, expected_df)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enable___re_deploy_from_latest_tile_start(
    session,
    feature_manager_no_sf_scheduling,
    online_enabled_feature_spec,
):
    """
    Test re-deploy tile generation from the latest tile start date
    """
    assert session.source_type == "snowflake"

    online_feature_spec, _ = online_enabled_feature_spec
    tile_spec = online_feature_spec.feature.tile_specs[0]

    last_tile_start_ts_df = await session.execute_query(
        f"SELECT LAST_TILE_START_DATE_OFFLINE FROM TILE_REGISTRY WHERE TILE_ID = '{tile_spec.tile_id}'"
    )
    assert last_tile_start_ts_df is not None and len(last_tile_start_ts_df) == 1
    last_tile_start_ts = last_tile_start_ts_df.iloc[0]["LAST_TILE_START_DATE_OFFLINE"]

    # disable/un-deploy
    await feature_manager_no_sf_scheduling.online_disable(online_feature_spec)

    # re-deploy and verify that the tile start ts is the same as the last tile start ts
    with patch("featurebyte.tile.manager.TileManager.generate_tiles") as mock_tile_manager:
        await feature_manager_no_sf_scheduling.online_enable(online_feature_spec)
        kwargs = mock_tile_manager.mock_calls[0][2]
        assert kwargs["start_ts_str"] == last_tile_start_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
