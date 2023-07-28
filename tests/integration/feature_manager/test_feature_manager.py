"""
This module contains integration tests for FeatureSnowflake
"""
import contextlib
from datetime import datetime
from unittest.mock import PropertyMock, patch

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.api.feature_list import FeatureList
from featurebyte.enum import InternalName
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.models.periodic_task import Interval
from featurebyte.models.tile import TileType


@contextlib.contextmanager
def create_and_enable_deployment(feature):
    """
    Create a temporary enabled deployment using the given feature
    """
    feature_list = FeatureList([feature], name=str(ObjectId()))
    feature_list.save(conflict_resolution="retrieve")
    deployment = None
    try:
        deployment = feature_list.deploy(make_production_ready=True)
        deployment.enable()
        yield deployment
    finally:
        if deployment:
            deployment.disable()


@pytest.fixture(name="feature_sum_30h")
def feature_sum_30h(event_table):
    """
    Feature feature_sum_30h fixture
    """
    event_view = event_table.get_view()
    feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ÀMOUNT",
        method="sum",
        windows=["30h"],
        feature_names=["sum_30h"],
    )["sum_30h"]
    return feature


@pytest.fixture(name="feature_sum_30h_transformed")
def feature_sum_30h_transformed(feature_sum_30h):
    """
    Feature feature_sum_30h_transformed fixture
    """
    new_feature = feature_sum_30h + 123
    new_feature.name = "feature_sum_30h_transformed"
    return new_feature


@pytest.fixture(name="feature_service")
def feature_service_fixture(app_container):
    """
    Feature service fixture
    """
    return app_container.feature_service


@pytest.fixture(name="periodic_task_service")
def periodic_task_service_fixture(app_container):
    """
    Periodic task service fixture
    """
    return app_container.periodic_task_service


@pytest.fixture(name="online_store_cleanup_scheduler_service")
def online_store_cleanup_scheduler_service_fixture(app_container):
    """
    Online store cleanup scheduler service fixture
    """
    return app_container.online_store_cleanup_scheduler_service


async def list_scheduled_tasks(periodic_task_service, feature_service, saved_feature):
    """
    List scheduled tasks for the given feature
    """
    feature_model = await feature_service.get_document(saved_feature.id)
    periodic_tasks = (await periodic_task_service.list_documents_as_dict())["data"]
    out = []
    for task in periodic_tasks:
        for agg_id in feature_model.aggregation_ids:
            if agg_id in task["name"]:
                out.append(task["name"])
                break
    return out


async def list_online_store_compute_queries(
    online_store_compute_query_service, feature_service, saved_feature
):
    """
    List online store compute queries for the given feature
    """
    feature_model = await feature_service.get_document(saved_feature.id)
    assert len(feature_model.aggregation_ids) == 1
    agg_id = feature_model.aggregation_ids[0]
    out = []
    async for query in online_store_compute_query_service.list_by_aggregation_id(agg_id):
        out.append(query)
    return out


async def list_online_store_cleanup_tasks(
    online_store_cleanup_scheduler_service, feature_service, saved_feature
):
    """
    List online store cleanup tasks for the given feature
    """
    feature_model = await feature_service.get_document(saved_feature.id)
    feature_spec = OnlineFeatureSpec(feature=feature_model.dict(by_alias=True))
    out = []
    for query in feature_spec.precompute_queries:
        task = await online_store_cleanup_scheduler_service.get_periodic_task(query.table_name)
        if task is not None:
            out.append(task)
    return out


async def online_store_table_exists(session, feature_service, saved_feature):
    """
    Check if the online store table exists for the given feature
    """
    feature_model = await feature_service.get_document(saved_feature.id)
    assert len(feature_model.online_store_table_names) == 1
    table_name = feature_model.online_store_table_names[0]
    try:
        await session.execute_query(f"select * from {table_name} limit 1")
    except:
        return False
    return True


@pytest.fixture(name="feature_sql")
def feature_sql_fixture():
    """
    Feature sql fixture
    """
    return f"""
        SELECT
          row_number() over (order by CUST_ID desc) as "cust_id",
          CAST('sum_30h' AS VARCHAR) AS {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          1 AS {InternalName.ONLINE_STORE_VALUE_COLUMN},
          'test_quote'
        FROM TEMP_TABLE
        """


@pytest.fixture(name="feature_store_table_name")
def feature_store_table_name_fixture():
    """
    Feature store table name fixture
    """
    feature_store_table_name = "feature_store_table_1"
    return feature_store_table_name


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enabled__without_snowflake_scheduling(
    session,
    extended_feature_model,
    tile_spec,
    feature_manager_service,
    feature_sql,
    feature_store_table_name,
    persistent,
    task_manager,
    tile_scheduler_service,
):
    with patch.object(ExtendedFeatureModel, "tile_specs", PropertyMock(return_value=[tile_spec])):
        mock_precompute_queries = [
            OnlineStoreComputeQueryModel(
                sql=feature_sql,
                tile_id=tile_spec.tile_id,
                aggregation_id=tile_spec.aggregation_id,
                table_name=feature_store_table_name,
                result_name="sum_30h",
                result_type="FLOAT",
                serving_names=["cust_id"],
            )
        ]
        online_feature_spec = OnlineFeatureSpec(
            feature=extended_feature_model, precompute_queries=mock_precompute_queries
        )
        schedule_time = datetime.utcnow()

        try:
            await feature_manager_service.online_enable(
                session, online_feature_spec, schedule_time=schedule_time
            )

            # check if the task is scheduled
            job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"
            job_details = await tile_scheduler_service.get_job_details(job_id=job_id)
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
            await feature_manager_service.online_disable(None, online_feature_spec)
            await session.execute_query(
                f"DELETE FROM {feature_store_table_name} WHERE {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN} = 'sum_30h'"
            )


@pytest_asyncio.fixture(name="online_enabled_feature_spec")
async def online_enabled_feature_spec_fixture(
    session,
    extended_feature_model,
    feature_manager_service,
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
            OnlineStoreComputeQueryModel(
                sql=feature_sql,
                tile_id=tile_spec.tile_id,
                aggregation_id=tile_spec.aggregation_id,
                table_name=feature_store_table_name,
                result_name="sum_30h",
                result_type="FLOAT",
                serving_names=["cust_id"],
            )
        ]
        online_feature_spec = OnlineFeatureSpec(
            feature=extended_feature_model, precompute_queries=mock_precompute_queries
        )
        schedule_time = datetime.utcnow()

        await feature_manager_service.online_enable(
            session, online_feature_spec, schedule_time=schedule_time
        )

        yield online_feature_spec, schedule_time

        await feature_manager_service.online_disable(session, online_feature_spec)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enabled_feature_spec(
    online_enabled_feature_spec,
    session,
    tile_spec,
    feature_sql,
    feature_store_table_name,
    online_store_compute_query_service,
):
    """
    Test online_enable
    """
    assert session.source_type == "snowflake"

    online_feature_spec, schedule_time = online_enabled_feature_spec
    expected_tile_id = tile_spec.tile_id
    expected_aggregation_id = tile_spec.aggregation_id

    result = [
        doc
        async for doc in online_store_compute_query_service.list_documents_iterator(
            query_filter={"tile_id": expected_tile_id}
        )
    ]
    assert len(result) == 1
    result = result[0]
    assert result.tile_id == expected_tile_id
    assert result.aggregation_id == expected_aggregation_id
    assert result.result_name == "sum_30h"
    assert result.result_type == "FLOAT"
    assert result.sql == feature_sql
    assert result.table_name == feature_store_table_name
    assert result.serving_names == ["cust_id"]

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
    expect_cols.append(InternalName.ONLINE_STORE_RESULT_NAME_COLUMN)
    expect_cols.append(InternalName.ONLINE_STORE_VALUE_COLUMN)
    assert list(result)[:3] == expect_cols


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_disable(
    session,
    feature_sum_30h,
    feature_sum_30h_transformed,
    periodic_task_service,
    feature_service,
    online_store_compute_query_service,
    online_store_cleanup_scheduler_service,
):
    """
    Test online_disable behaves correctly
    """
    with create_and_enable_deployment(feature_sum_30h) as deployment1:
        with create_and_enable_deployment(feature_sum_30h_transformed) as deployment2:
            # 1. Check that both features share the same tile tasks
            tasks1 = await list_scheduled_tasks(
                periodic_task_service, feature_service, feature_sum_30h
            )
            tasks2 = await list_scheduled_tasks(
                periodic_task_service, feature_service, feature_sum_30h_transformed
            )
            assert len(tasks1) > 0
            assert set(tasks1) == set(tasks2)

            # Both features share the same compute query
            online_store_compute_queries = await list_online_store_compute_queries(
                online_store_compute_query_service, feature_service, feature_sum_30h_transformed
            )
            assert len(online_store_compute_queries) == 1

            # Check clean up task
            cleanup_tasks = await list_online_store_cleanup_tasks(
                online_store_cleanup_scheduler_service, feature_service, feature_sum_30h
            )
            assert len(cleanup_tasks) == 1
            assert await online_store_table_exists(session, feature_service, feature_sum_30h)

            # 2. Disable the first feature. Since the tile is still used by the second feature, the
            # tile tasks should not be removed.
            deployment1.disable()
            tasks = await list_scheduled_tasks(
                periodic_task_service, feature_service, feature_sum_30h_transformed
            )
            assert set(tasks) == set(tasks2)

            # The query should still exist because it is still used by the second feature
            online_store_compute_queries = await list_online_store_compute_queries(
                online_store_compute_query_service, feature_service, feature_sum_30h_transformed
            )
            assert len(online_store_compute_queries) == 1

            # Clean up task still required
            cleanup_tasks = await list_online_store_cleanup_tasks(
                online_store_cleanup_scheduler_service, feature_service, feature_sum_30h_transformed
            )
            assert len(cleanup_tasks) == 1
            assert await online_store_table_exists(session, feature_service, feature_sum_30h)

            # 3. Disable the second feature. Since the tile is no longer used by any feature, the
            # tile tasks should be removed.
            deployment2.disable()
            tasks = await list_scheduled_tasks(
                periodic_task_service, feature_service, feature_sum_30h_transformed
            )
            assert len(tasks) == 0

            # The query should be removed now
            online_store_compute_queries = await list_online_store_compute_queries(
                online_store_compute_query_service, feature_service, feature_sum_30h_transformed
            )
            assert len(online_store_compute_queries) == 0

            # Clean up task should be removed
            cleanup_tasks = await list_online_store_cleanup_tasks(
                online_store_cleanup_scheduler_service, feature_service, feature_sum_30h_transformed
            )
            assert len(cleanup_tasks) == 0
            assert not await online_store_table_exists(session, feature_service, feature_sum_30h)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enable__re_deploy_from_latest_tile_start(
    session,
    feature_manager_service,
    online_enabled_feature_spec,
    tile_registry_service,
):
    """
    Test re-deploy tile generation from the latest tile start date
    """
    assert session.source_type == "snowflake"

    online_feature_spec, _ = online_enabled_feature_spec
    tile_spec = online_feature_spec.feature.tile_specs[0]

    tile_model = await tile_registry_service.get_tile_model(
        tile_spec.tile_id, tile_spec.aggregation_id
    )
    assert tile_model is not None
    last_tile_start_ts = tile_model.last_run_metadata_offline.tile_end_date

    # disable/un-deploy
    await feature_manager_service.online_disable(session, online_feature_spec)

    # re-deploy and verify that the tile start ts is the same as the last tile start ts
    with patch(
        "featurebyte.service.tile_manager.TileManagerService.generate_tiles"
    ) as mock_generate_tiles:
        await feature_manager_service.online_enable(session, online_feature_spec)
        _, kwargs = mock_generate_tiles.call_args
        assert kwargs["start_ts_str"] == last_tile_start_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
