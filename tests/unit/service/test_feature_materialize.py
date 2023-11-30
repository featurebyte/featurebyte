"""
Test FeatureMaterializeService
"""
from dataclasses import asdict

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.online_store import OnlineFeatureSpec


async def create_online_store_compute_query(online_store_compute_query_service, feature_model):
    """
    Helper to create online store compute query since that step is skipped because of
    mock_update_data_warehouse
    """
    extended_feature_model = ExtendedFeatureModel(**feature_model.dict(by_alias=True))
    online_feature_spec = OnlineFeatureSpec(feature=extended_feature_model)
    for query in online_feature_spec.precompute_queries:
        await online_store_compute_query_service.create_document(query)


@pytest_asyncio.fixture(name="deployed_feature_list")
async def deployed_feature_list_fixture(
    app_container, production_ready_feature_list, mock_update_data_warehouse
):
    """
    Fixture for FeatureMaterializeService
    """
    _ = mock_update_data_warehouse
    deployment_id = ObjectId()
    await app_container.deploy_service.create_deployment(
        feature_list_id=production_ready_feature_list.id,
        deployment_id=deployment_id,
        deployment_name=None,
        to_enable_deployment=True,
    )
    for feature_id in production_ready_feature_list.feature_ids:
        feature_model = await app_container.feature_service.get_document(
            document_id=feature_id,
        )
        await create_online_store_compute_query(
            app_container.online_store_compute_query_service, feature_model
        )
    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    deployed_feature_list = await app_container.feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    yield deployed_feature_list


@pytest_asyncio.fixture(name="deployed_feature")
async def deployed_feature_fixture(feature_service, deployed_feature_list):
    """
    Fixture for a deployed feature
    """
    return await feature_service.get_document(deployed_feature_list.feature_ids[0])


@pytest.mark.asyncio
async def test_materialize_features(
    feature_materialize_service,
    mock_snowflake_session,
    deployed_feature,
):
    """
    Test materialize_features
    """
    feature_job_setting = deployed_feature.table_id_feature_job_settings[0].feature_job_setting
    materialized_features = await feature_materialize_service.materialize_features(
        session=mock_snowflake_session,
        primary_entity_ids=deployed_feature.primary_entity_ids,
        feature_job_setting=feature_job_setting,
    )
    materialized_features_dict = asdict(materialized_features)
    materialized_features_dict["materialized_table_name"], suffix = materialized_features_dict[
        "materialized_table_name"
    ].rsplit("_", 1)
    assert suffix != ""
    assert materialized_features_dict == {
        "materialized_table_name": "TEMP_FEATURE_TABLE",
        "names": ["sum_30m"],
        "serving_names": ["cust_id"],
    }


@pytest.mark.asyncio
async def test_materialize_features__different_feature_job_setting(
    feature_materialize_service,
    mock_snowflake_session,
    deployed_feature,
):
    """
    Test materialize_features when no matching features are found
    """
    materialized_features = await feature_materialize_service.materialize_features(
        session=mock_snowflake_session,
        primary_entity_ids=deployed_feature.primary_entity_ids,
        feature_job_setting=FeatureJobSetting(
            blind_spot="1h", time_modulo_frequency="2h", frequency="3h"
        ),
    )
    assert materialized_features is None


@pytest.mark.asyncio
async def test_materialize_features__different_primary_entity_ids(
    feature_materialize_service,
    mock_snowflake_session,
    deployed_feature,
):
    """
    Test materialize_features when no matching features are found
    """
    feature_job_setting = deployed_feature.table_id_feature_job_settings[0].feature_job_setting
    materialized_features = await feature_materialize_service.materialize_features(
        session=mock_snowflake_session,
        primary_entity_ids=[ObjectId()],
        feature_job_setting=feature_job_setting,
    )
    assert materialized_features is None
