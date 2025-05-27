"""
This module contains integration tests for FeatureSnowflake
"""

import contextlib
from datetime import timedelta
from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.api.feature_list import FeatureList
from featurebyte.enum import InternalName
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.periodic_task import Interval
from featurebyte.models.tile import TileType
from featurebyte.query_graph.sql.online_store_compute_query import (
    get_online_store_precompute_queries,
)


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


@pytest.fixture(name="feature_sum_30h", scope="module")
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
    feature.save()
    return feature


@pytest.fixture(name="feature_sum_30h_transformed")
def feature_sum_30h_transformed(feature_sum_30h, scope="module"):
    """
    Feature feature_sum_30h_transformed fixture
    """
    new_feature = feature_sum_30h + 123
    new_feature.name = "feature_sum_30h_transformed"
    new_feature.save()
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


async def list_scheduled_tasks(app_container, saved_feature):
    """
    List scheduled tasks for the given feature
    """
    feature_service = app_container.feature_service
    periodic_task_service = app_container.periodic_task_service
    deployed_tile_table_service = app_container.deployed_tile_table_service

    feature_model = await feature_service.get_document(saved_feature.id)
    deployed_tile_tables = (
        await deployed_tile_table_service.get_deployed_tile_table_info(
            aggregation_ids=feature_model.aggregation_ids
        )
    ).deployed_tile_tables

    if not deployed_tile_tables:
        return []

    assert len(deployed_tile_tables) == 1
    deployed_tile_table = deployed_tile_tables[0]

    periodic_tasks = (await periodic_task_service.list_documents_as_dict())["data"]
    out = []
    for task in periodic_tasks:
        if str(deployed_tile_table.id) in task["name"]:
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
    async for query in online_store_compute_query_service.list_by_aggregation_ids({agg_id}):
        out.append(query)
    return out


async def list_online_store_cleanup_tasks(
    online_store_cleanup_scheduler_service, feature_service, saved_feature
):
    """
    List online store cleanup tasks for the given feature
    """
    feature_model = await feature_service.get_document(saved_feature.id)
    precompute_queries = get_online_store_precompute_queries(
        graph=feature_model.graph,
        node=feature_model.node,
        source_info=feature_model.get_source_info(),
        agg_result_name_include_serving_names=feature_model.agg_result_name_include_serving_names,
    )
    out = []
    for query in precompute_queries:
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


@pytest_asyncio.fixture(name="online_enabled_feature_sum_30h")
async def online_enabled_feature_sum_30h_fixture(feature_sum_30h):
    """
    Fixture for an online enabled feature sum_30h
    """
    with patch("featurebyte.service.feature_manager.datetime") as patched_datetime:
        patched_datetime.utcnow.return_value = pd.Timestamp("2001-01-02 13:15:00").to_pydatetime()
        with create_and_enable_deployment(feature_sum_30h):
            yield feature_sum_30h.cached_model


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_enabled_feature_spec(
    online_enabled_feature_sum_30h,
    session,
    tile_scheduler_service,
    online_store_compute_query_service,
    deployed_tile_table_service,
):
    """
    Test online_enable
    """
    assert session.source_type == "snowflake"

    expected_online_store_table_name = "ONLINE_STORE_43C2B44924994BBE375C05427AC312CB1008A469"
    expected_aggregation_id = online_enabled_feature_sum_30h.aggregation_ids[0]

    # check deployed tile table
    deployed_tile_tables = (
        await deployed_tile_table_service.get_deployed_tile_table_info({expected_aggregation_id})
    ).deployed_tile_tables
    assert len(deployed_tile_tables) == 1

    # check if the tile task is scheduled
    job_id = f"deployed_tile_table_{TileType.ONLINE}_{deployed_tile_tables[0].id}"
    job_details = await tile_scheduler_service.get_job_details(job_id=job_id)
    assert job_details is not None
    assert job_details.name == job_id
    assert job_details.interval == Interval(every=3600, period="seconds")

    # check online store precompute query
    result = [
        doc
        async for doc in online_store_compute_query_service.list_documents_iterator(
            query_filter={"aggregation_id": expected_aggregation_id}
        )
    ]
    assert len(result) == 1
    result = result[0]
    assert result.aggregation_id == expected_aggregation_id
    assert result.result_name.startswith("_fb_internal_üser id_window_w108000_sum")
    assert result.result_type == "FLOAT"
    assert result.table_name == expected_online_store_table_name
    assert result.serving_names == ["üser id"]

    # validate generate historical tiles
    sql = f"SELECT * FROM {deployed_tile_tables[0].table_name}"
    result = await session.execute_query(sql)
    assert len(result) > 0

    # validate populate Online Store result
    sql = f"SELECT * FROM {expected_online_store_table_name}"
    result = await session.execute_query(sql)
    assert len(result) > 0
    expect_cols = [
        "üser id",
        InternalName.ONLINE_STORE_RESULT_NAME_COLUMN,
        InternalName.ONLINE_STORE_VALUE_COLUMN,
    ]
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
    deployed_tile_table_service,
    app_container,
):
    """
    Test online_disable behaves correctly
    """
    with create_and_enable_deployment(feature_sum_30h) as deployment1:
        with create_and_enable_deployment(feature_sum_30h_transformed) as deployment2:
            # 1. Check that both features share the same tile tasks
            tasks1 = await list_scheduled_tasks(app_container, feature_sum_30h)
            tasks2 = await list_scheduled_tasks(app_container, feature_sum_30h_transformed)
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
            tasks = await list_scheduled_tasks(app_container, feature_sum_30h_transformed)
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
            tasks = await list_scheduled_tasks(app_container, feature_sum_30h_transformed)
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
    online_enabled_feature_sum_30h,
    deployed_tile_table_service,
):
    """
    Test re-deploy tile generation from the latest tile start date
    """
    assert session.source_type == "snowflake"

    feature_spec = ExtendedFeatureModel(**online_enabled_feature_sum_30h.model_dump(by_alias=True))
    deployed_tile_table = (
        await deployed_tile_table_service.get_deployed_tile_table_info(
            online_enabled_feature_sum_30h.aggregation_ids
        )
    ).deployed_tile_tables[0]
    assert deployed_tile_table is not None
    last_tile_start_ts = deployed_tile_table.last_run_metadata_offline.tile_end_date

    # disable/un-deploy
    await feature_manager_service.online_disable(session, online_enabled_feature_sum_30h)

    # re-deploy and verify that the tile start ts is the same as the last tile start ts
    with patch(
        "featurebyte.service.tile_manager.TileManagerService.generate_tiles"
    ) as mock_generate_tiles:
        with patch("featurebyte.service.feature_manager.datetime") as mock_datetime:
            # simulate re-deploy at a later date; otherwise this will be a no-op since no new tiles
            # need to be generated and generate_tiles won't be called
            mock_datetime.utcnow.return_value = last_tile_start_ts + timedelta(days=1)
            await feature_manager_service.online_enable(session, feature_spec)
        _, kwargs = mock_generate_tiles.call_args
        assert kwargs["start_ts_str"] == last_tile_start_ts.strftime("%Y-%m-%dT%H:%M:%S")
