"""
Tests for FeatureJobHistoryService
"""

# pylint: disable=wildcard-import,unused-wildcard-import

from featurebyte.schema.deployment import DeploymentJobHistory, FeatureTableJobRun
from featurebyte.service.feature_job_history_service import FeatureJobHistoryService
from tests.unit.service.fixtures_feature_materialize_runs import *


@pytest.fixture
def service(app_container) -> FeatureJobHistoryService:
    """
    Fixture for a FeatureJobHistoryService
    """
    return app_container.feature_job_history_service


@pytest.mark.usefixtures("saved_feature_materialize_run_models")
@pytest.mark.asyncio
async def test_get_deployment_job_history(service, deployment_id, offline_store_feature_table_id):
    """
    Tests for get_deployment_job_history
    """
    job_history = await service.get_deployment_job_history(deployment_id, 3)
    assert job_history == DeploymentJobHistory(
        runs=[
            FeatureTableJobRun(
                feature_table_id=offline_store_feature_table_id,
                feature_table_name=None,
                scheduled_ts=datetime(2024, 7, 15, 9, 0),
                completion_ts=datetime(2024, 7, 15, 9, 0, 10),
                completion_status="failure",
                duration_seconds=10,
                incomplete_tile_tasks_count=0,
            ),
            FeatureTableJobRun(
                feature_table_id=offline_store_feature_table_id,
                feature_table_name=None,
                scheduled_ts=datetime(2024, 7, 15, 8, 0),
                completion_ts=datetime(2024, 7, 15, 8, 0, 10),
                completion_status="success",
                duration_seconds=10,
                incomplete_tile_tasks_count=0,
            ),
            FeatureTableJobRun(
                feature_table_id=offline_store_feature_table_id,
                feature_table_name=None,
                scheduled_ts=datetime(2024, 7, 15, 7, 0),
                completion_ts=datetime(2024, 7, 15, 7, 0, 10),
                completion_status="failure",
                duration_seconds=10,
                incomplete_tile_tasks_count=0,
            ),
        ]
    )
