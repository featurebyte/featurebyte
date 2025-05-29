"""
Unit tests for TileTaskExecutor
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import FeatureJobSetting, SourceType
from featurebyte.enum import DBVarType
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.tile import TileScheduledJobParameters
from featurebyte.session.snowflake import SnowflakeSession


@pytest.fixture(name="session")
def session_fixture():
    """
    Fixture for the db session object
    """
    return Mock(
        name="mock_snowflake_session",
        spec=SnowflakeSession,
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.fixture(name="aggregation_id")
def aggregation_id_fixture():
    """
    Fixture for an aggregation id
    """
    return "some_agg_id"


@pytest.fixture(name="tile_task_parameters")
def tile_task_parameters_fixture(aggregation_id) -> TileScheduledJobParameters:
    """
    Fixture for TileScheduledJobParameters
    """
    return TileScheduledJobParameters(
        tile_id="some_tile_id",
        aggregation_id=aggregation_id,
        time_modulo_frequency_second=10,
        blind_spot_second=30,
        frequency_minute=5,
        sql="SELECT * FROM some_table",
        entity_column_names=["cust_id"],
        value_column_names=["sum_value"],
        value_column_types=["FLOAT"],
        offline_period_minute=1440,
        tile_type="online",
        monitor_periods=10,
        feature_store_id=ObjectId(),
    )


@pytest_asyncio.fixture(name="offline_store_feature_table")
async def offline_store_feature_table_fixture(app_container, aggregation_id):
    """
    Fixture for a saved offline store feature table
    """
    model = OfflineStoreFeatureTableModel(
        name="my_feature_table",
        feature_ids=[ObjectId()],
        primary_entity_ids=[ObjectId()],
        serving_names=["cust_id"],
        has_ttl=False,
        output_column_names=["x"],
        output_dtypes=[DBVarType.FLOAT],
        catalog_id=app_container.catalog_id,
        feature_job_setting=FeatureJobSetting(period="5m", offset="10s", blind_spot="30s"),
        aggregation_ids=[aggregation_id],
    )
    doc = await app_container.offline_store_feature_table_service.create_document(model)
    yield doc


@pytest.fixture(name="patched_tile_classes")
def patched_tile_classes_fixture():
    """
    Fixture to patch TileMonitor, TileGenerate and TileScheduleOnlineStore used in TileTaskExecutor
    """
    mocks = {}
    patchers = []
    classes_to_patch = ["TileGenerate"]
    for class_name in classes_to_patch:
        patcher = patch(f"featurebyte.service.tile.tile_task_executor.{class_name}", autospec=True)
        mocks[class_name] = patcher.start()
        patchers.append(patcher)
    yield mocks
    for patcher in patchers:
        patcher.stop()


@pytest.mark.usefixtures("patched_tile_classes")
@pytest.mark.asyncio
async def test_update_tile_prerequisite__success(
    app_container,
    tile_task_executor,
    tile_task_parameters,
    session,
    offline_store_feature_table,
):
    """
    Test that tile task status are reflected in feature store table prerequisite documents (success
    case)
    """
    # Simulate a successful tile task
    tile_task_parameters.job_schedule_ts = "2023-01-15 10:00:11"
    await tile_task_executor.execute(session, tile_task_parameters)

    # Check prerequisite documents updated
    docs = await app_container.feature_materialize_prerequisite_service.list_documents_as_dict()
    assert len(docs["data"]) == 1
    doc = docs["data"][0]
    expected = {
        "offline_store_feature_table_id": offline_store_feature_table.id,
        "scheduled_job_ts": datetime(2023, 1, 15, 10, 0, 10),
        "completed": [{"aggregation_id": "some_agg_id", "status": "success"}],
    }
    assert expected.items() < doc.items()


@pytest.mark.asyncio
async def test_update_tile_prerequisite__failure(
    app_container,
    tile_task_executor,
    tile_task_parameters,
    session,
    patched_tile_classes,
    offline_store_feature_table,
):
    """
    Test that tile task status are reflected in feature store table prerequisite documents (failure
    case)
    """
    # Simulate an error during tile task
    patched_tile_classes["TileGenerate"].return_value.execute.side_effect = RuntimeError(
        "Fail on purpose"
    )
    tile_task_parameters.job_schedule_ts = "2023-01-15 10:00:11"
    with pytest.raises(RuntimeError, match="Fail on purpose"):
        await tile_task_executor.execute(session, tile_task_parameters)

    # Check prerequisite documents updated
    docs = await app_container.feature_materialize_prerequisite_service.list_documents_as_dict()
    assert len(docs["data"]) == 1
    doc = docs["data"][0]
    expected = {
        "offline_store_feature_table_id": offline_store_feature_table.id,
        "scheduled_job_ts": datetime(2023, 1, 15, 10, 0, 10),
        "completed": [{"aggregation_id": "some_agg_id", "status": "failure"}],
    }
    assert expected.items() < doc.items()
