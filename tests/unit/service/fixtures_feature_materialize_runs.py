"""
Shared fixtures of FeatureMaterializeRun
"""

from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.deployment import DeploymentModel
from featurebyte.models.feature_materialize_run import FeatureMaterializeRun, IncompleteTileTask


@pytest.fixture
def feature_materialize_run_service(app_container):
    """
    Fixture for FeatureMaterializeRunService
    """
    return app_container.feature_materialize_run_service


@pytest.fixture
def offline_store_feature_table_id():
    """
    Fixture for offline store feature table id
    """
    return ObjectId()


@pytest.fixture
def scheduled_job_ts():
    """
    Fixture for a scheduled job timestamp
    """
    return datetime(2024, 7, 15, 0, 0, 0)


@pytest.fixture
def completion_ts(scheduled_job_ts):
    """
    Fixture for a completion timestamp
    """
    return scheduled_job_ts + timedelta(seconds=10)


@pytest.fixture
def feature_materialize_run_model(offline_store_feature_table_id, scheduled_job_ts):
    """
    Fixture for a FeatureMaterializeRun
    """
    return FeatureMaterializeRun(
        offline_store_feature_table_id=offline_store_feature_table_id,
        scheduled_job_ts=scheduled_job_ts,
    )


@pytest.fixture
def deployment_id():
    """
    Fixture for a deployment id
    """
    return ObjectId()


@pytest.fixture
def another_deployment_id():
    """
    Fixture for another deployment id
    """
    return ObjectId()


@pytest.fixture
def feature_aggregation_id(feature):
    """
    Fixture for aggregation ids for a feature (feature is used in feature_list)
    """
    return feature.aggregation_ids[0]


@pytest.fixture
def saved_feature_list(feature_list):
    """
    Fixture for a saved feature list
    """
    return feature_list


@pytest_asyncio.fixture
async def saved_deployment(app_container, deployment_id, saved_feature_list):
    """
    Fixture for a saved deployment
    """
    deployment = DeploymentModel(
        _id=deployment_id,
        name="some_deployment",
        feature_list_id=saved_feature_list.id,
        feature_list_namespace_id=ObjectId(),
        enabled=True,
    )
    return await app_container.deployment_service.create_document(deployment)


@pytest_asyncio.fixture
async def saved_feature_materialize_run_models(
    feature_materialize_run_service,
    offline_store_feature_table_id,
    scheduled_job_ts,
    saved_deployment,
    another_deployment_id,
    feature_aggregation_id,
):
    """
    Fixture for a list of saved FeatureMaterializeRun models
    """
    models = []
    for current_deployment_id in [saved_deployment.id, another_deployment_id]:
        for i in range(10):
            current_scheduled_job_ts = scheduled_job_ts + i * timedelta(hours=1)
            model = FeatureMaterializeRun(
                offline_store_feature_table_id=offline_store_feature_table_id,
                scheduled_job_ts=current_scheduled_job_ts,
                completion_ts=current_scheduled_job_ts + timedelta(seconds=10),
                completion_status="success" if i % 2 == 0 else "failure",
                duration_from_scheduled_seconds=10,
                incomplete_tile_tasks=[
                    IncompleteTileTask(
                        aggregation_id="another_agg_id" if i % 2 == 0 else feature_aggregation_id,
                        reason="failure",
                    )
                ],
                deployment_ids=[current_deployment_id],
            )
            models.append(await feature_materialize_run_service.create_document(model))
    yield models
