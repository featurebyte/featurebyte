"""
Test FeatureMaterializeService
"""
from dataclasses import asdict

import pytest
import pytest_asyncio
from bson import ObjectId

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
        await app_container.offline_store_feature_table_manager_service.handle_online_enabled_feature(
            feature_model,
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


@pytest_asyncio.fixture(name="offline_store_feature_table")
async def offline_store_feature_table_fixture(app_container, deployed_feature):
    """
    Fixture for offline store feature table
    """
    async for model in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={"feature_ids": deployed_feature.id}
    ):
        return model


@pytest.mark.asyncio
async def test_materialize_features(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
):
    """
    Test materialize_features
    """
    materialized_features = await feature_materialize_service.materialize_features(
        session=mock_snowflake_session,
        feature_table_model=offline_store_feature_table,
    )
    assert len(mock_snowflake_session.execute_query_long_running.call_args_list) == 2
    materialized_features_dict = asdict(materialized_features)
    materialized_features_dict["materialized_table_name"], suffix = materialized_features_dict[
        "materialized_table_name"
    ].rsplit("_", 1)
    assert suffix != ""
    assert materialized_features_dict == {
        "materialized_table_name": "TEMP_FEATURE_TABLE",
        "names": ["sum_30m"],
        "data_types": ["FLOAT"],
        "serving_names": ["cust_id"],
    }
