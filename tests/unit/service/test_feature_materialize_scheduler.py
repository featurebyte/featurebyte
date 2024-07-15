"""
Teat FeatureMaterializeSchedulerService
"""

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.enum import WorkerCommand
from featurebyte.models.entity_universe import EntityUniverseModel
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.periodic_task import Interval
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.worker.task.scheduled_feature_materialize import (
    ScheduledFeatureMaterializeTaskPayload,
)


@pytest.fixture(name="feature_job_setting")
def feature_job_setting_fixture():
    """
    Fixture for a feature job setting
    """
    return FeatureJobSetting(period="1h", blind_spot="0s", offset="5m")


@pytest_asyncio.fixture(name="offline_store_feature_table")
async def fixture_offline_store_feature_table(app_container, feature_job_setting):
    """
    Fixture for offline store feature table
    """
    feature_table = OfflineStoreFeatureTableModel(
        name="my_feature_table",
        feature_ids=[ObjectId()],
        primary_entity_ids=[ObjectId()],
        serving_names=["cust_id"],
        feature_job_setting=feature_job_setting,
        has_ttl=True,
        feature_cluster=FeatureCluster(graph=QueryGraph(), node_names=[], feature_store_id=ObjectId()),
        output_column_names=["col1", "col2"],
        output_dtypes=["VARCHAR", "FLOAT"],
        entity_universe=EntityUniverseModel(
            query_template=SqlglotExpressionModel(
                formatted_expression="SELECT DISTINCT cust_id FROM my_table",
            )
        ).dict(by_alias=True),
        catalog_id=app_container.catalog_id,
    )
    await app_container.offline_store_feature_table_service.create_document(feature_table)
    yield feature_table


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    Fixture for FeatureMaterializeSchedulerService
    """
    return app_container.feature_materialize_scheduler_service


@pytest_asyncio.fixture(name="deprecated_scheduled_task")
async def deprecated_scheduled_feature_materialize_task(service, offline_store_feature_table):
    """
    Fixture for a deprecated scheduled job
    """
    payload = ScheduledFeatureMaterializeTaskPayload(
        user_id=service.user.id,
        catalog_id=service.catalog_id,
        offline_store_feature_table_name=offline_store_feature_table.name,
        offline_store_feature_table_id=offline_store_feature_table.id,
    )
    task_name = service._get_deprecated_job_id(offline_store_feature_table.id)
    task_id = await service.task_manager.schedule_interval_task(
        name=task_name,
        payload=payload,
        interval=Interval(
            every=offline_store_feature_table.feature_job_setting.period_seconds,
            period="seconds",
        ),
        time_modulo_frequency_second=offline_store_feature_table.feature_job_setting.offset_seconds,
    )
    yield task_id


@pytest.mark.parametrize(
    "feature_job_setting",
    [
        FeatureJobSetting(period="1h", blind_spot="0s", offset="5m"),
        FeatureJobSetting(period="1d", blind_spot="0s", offset="5m"),
    ],
)
@pytest.mark.asyncio
async def test_start_and_stop_job(service, feature_job_setting, offline_store_feature_table):
    """
    Test start_job with different feature job settings
    """
    # Test starting job
    await service.start_job_if_not_exist(offline_store_feature_table)
    periodic_task = await service.get_periodic_task(offline_store_feature_table.id)
    assert periodic_task is not None
    assert periodic_task.interval == Interval(
        every=offline_store_feature_table.feature_job_setting.period_seconds, period="seconds"
    )
    assert periodic_task.time_modulo_frequency_second == feature_job_setting.offset_seconds
    assert periodic_task.kwargs["command"] == WorkerCommand.FEATURE_MATERIALIZE_SYNC
    assert periodic_task.kwargs["offline_store_feature_table_id"] == str(offline_store_feature_table.id)
    assert periodic_task.kwargs["offline_store_feature_table_name"] == offline_store_feature_table.name

    # Test stopping job
    await service.stop_job(offline_store_feature_table.id)
    periodic_task = await service.get_periodic_task(offline_store_feature_table.id)
    assert periodic_task is None


@pytest.mark.asyncio
async def test_cleanup_deprecated_scheduled_job_on_start(
    service, offline_store_feature_table, deprecated_scheduled_task
):
    """
    Test deprecated scheduled tasks are cleaned up on starting job
    """
    # Check there is a deprecated task
    periodic_task = await service.task_manager.get_periodic_task(deprecated_scheduled_task)
    assert periodic_task is not None
    assert periodic_task.kwargs["command"] == WorkerCommand.SCHEDULED_FEATURE_MATERIALIZE

    # Check new task is scheduled
    await service.start_job_if_not_exist(offline_store_feature_table)
    periodic_task = await service.get_periodic_task(offline_store_feature_table.id)
    assert periodic_task is not None
    assert periodic_task.kwargs["command"] == WorkerCommand.FEATURE_MATERIALIZE_SYNC

    # Check deprecated task is cleaned up
    periodic_task = await service.get_periodic_task(deprecated_scheduled_task)
    assert periodic_task is None


@pytest.mark.asyncio
async def test_cleanup_deprecated_scheduled_job_on_stop(
    service, offline_store_feature_table, deprecated_scheduled_task
):
    """
    Test deprecated scheduled tasks are cleaned up on stopping job
    """
    # Check there is a deprecated task
    periodic_task = await service.task_manager.get_periodic_task(deprecated_scheduled_task)
    assert periodic_task is not None
    assert periodic_task.kwargs["command"] == WorkerCommand.SCHEDULED_FEATURE_MATERIALIZE

    # Check deprecated task is cleaned up on stop_job
    await service.stop_job(offline_store_feature_table)
    periodic_task = await service.get_periodic_task(deprecated_scheduled_task)
    assert periodic_task is None
