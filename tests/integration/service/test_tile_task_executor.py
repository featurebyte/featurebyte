"""
Integration test for scheduled tile tasks
"""

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import (
    FeatureList,
)
from featurebyte.enum import WorkerCommand
from featurebyte.models.tile import TileScheduledJobParameters, TileType
from featurebyte.service.tile.tile_task_executor import TileTaskExecutor


@pytest.fixture(name="tile_task_executor")
def tile_task_executor_fixture(app_container) -> TileTaskExecutor:
    """
    Fixture to create TileTaskExecutor instance
    """
    return app_container.tile_task_executor


@pytest.fixture(name="periodic_task_service")
def periodic_task_service_fixture(app_container):
    """
    Fixture to create PeriodicTaskService instance
    """
    return app_container.periodic_task_service


@pytest.fixture(name="deployed_feature_with_partition_column")
def feature_with_partition_column_fixture(event_table_with_timestamp_schema):
    """
    Fixture to create a deployed feature with partition column
    """
    feature_name = "feature_with_partition_column"
    event_view = event_table_with_timestamp_schema.get_view()
    feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=[feature_name],
    )[feature_name]
    feature_list = FeatureList([feature], name=str(ObjectId()))
    feature_list.save()
    deployment = feature_list.deploy(str(ObjectId()), make_production_ready=True)
    deployment.enable()
    try:
        yield
    finally:
        deployment.disable()


@pytest_asyncio.fixture(name="tile_scheduled_job_parameters_with_partition_column")
async def tile_scheduled_job_parameters_with_partition_column_fixture(
    deployed_feature_with_partition_column,
    periodic_task_service,
):
    """
    Fixture to create scheduled job parameters for a tile task with partition column
    """
    _ = deployed_feature_with_partition_column
    docs = []
    async for doc in periodic_task_service.list_documents_iterator({}):
        kwargs = doc.kwargs
        parameters = kwargs.get("parameters", {})
        if (
            kwargs["command"] == WorkerCommand.TILE_COMPUTE
            and parameters.get("tile_type") == TileType.ONLINE
        ):
            docs.append(doc)
    assert len(docs) == 1
    yield TileScheduledJobParameters(**docs[0].kwargs["parameters"])


@pytest.mark.asyncio
async def test_scheduled_tile_task_with_partition_column(
    tile_task_executor: TileTaskExecutor,
    session,
    tile_scheduled_job_parameters_with_partition_column,
):
    """
    Test scheduled tile task with partition column
    """
    # Check the tile task can run without errors
    await tile_task_executor.execute(session, tile_scheduled_job_parameters_with_partition_column)
