"""
Tests for feature materialization service
"""
from unittest.mock import patch

import pandas as pd
import pytest

import featurebyte as fb
from featurebyte.schema.worker.task.scheduled_feature_materialize import (
    ScheduledFeatureMaterializeTaskPayload,
)


@pytest.fixture(name="features", scope="module")
def features_fixture(event_table):
    """
    Fixture for feature
    """
    event_view = event_table.get_view()

    # Feature saved but not deployed
    feature_0 = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["666h"],
        feature_names=["EXTERNAL_FS_FEATURE_NOT_DEPLOYED"],
    )
    feature_0.save()

    # Window aggregate feature
    feature_1 = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["24h"],
        feature_names=["EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h"],
    )["EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h"]

    # Window aggregate feature with more post-processing
    feature_2 = feature_1 * 100
    feature_2.name = feature_1.name + "_TIMES_100"

    # Feature with a different entity
    filtered_event_view = event_view[event_view["PRODUCT_ACTION"].notnull()]
    feature_3 = filtered_event_view.groupby("PRODUCT_ACTION").aggregate_over(
        None,
        method="count",
        windows=["7d"],
        feature_names=["EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d"],
    )["EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d"]

    # Feature with two entities (not supported yet)
    # feature_4 = feature_1 + feature_3
    # feature_4.name = "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE"

    # Save all features to be deployed
    features = [feature_1, feature_2, feature_3]
    for feature in features:
        feature.save()
        feature.update_readiness("PRODUCTION_READY")

    return features


@pytest.fixture(name="deployed_feature_list", scope="module")
def deployed_features_list_fixture(features):
    """
    Fixture for deployed feature list
    """
    feature_list = fb.FeatureList(features, name="EXTERNAL_FS_FEATURE_LIST")
    feature_list.save()
    with patch(
        "featurebyte.service.feature_manager.get_next_job_datetime",
        return_value=pd.Timestamp("2001-01-02 12:00:00").to_pydatetime(),
    ):
        deployment = feature_list.deploy()
        deployment.enable()
    yield deployment
    deployment.disable()


async def register_offline_store_feature_tables(app_container, features):
    """
    Register offline store feature tables
    """
    for feature in features:
        await app_container.offline_store_feature_table_manager_service.handle_online_enabled_features(
            [await app_container.feature_service.get_document(feature.id)],
        )


@pytest.fixture(name="default_feature_job_setting")
def default_feature_job_setting_fixture(event_table):
    """
    Fixture for default feature job setting
    """
    return event_table.default_feature_job_setting


async def check_feature_tables_populated(session, feature_tables):
    """
    Check feature tables are populated correctly
    """
    for feature_table in feature_tables:
        df = await session.execute_query(f'SELECT * FROM "{feature_table.name}"')

        # Should not be empty
        assert df.shape[0] > 0

        # Should have all the serving names and output columns tracked in OfflineStoreFeatureTable
        assert set(df.columns.tolist()) == set(
            ["__feature_timestamp"]
            + feature_table.serving_names
            + feature_table.output_column_names
        )


async def check_feast_registry(app_container):
    """
    Check feast registry is populated correctly
    """
    feast_registry = await app_container.feast_registry_service.get_feast_registry_for_catalog()
    assert feast_registry is not None
    feature_store = await app_container.feast_feature_store_service.get_feast_feature_store(
        feast_registry.id
    )

    # Check feature views and feature services
    assert {fv.name for fv in feature_store.list_feature_views()} == {
        "fb_entity_PRODUCT_ACTION_fjs_3600_1800_1800_ttl",
        "fb_entity_üser id_fjs_3600_1800_1800_ttl",
    }
    assert {fs.name for fs in feature_store.list_feature_services()} == {"EXTERNAL_FS_FEATURE_LIST"}

    # Check feast materialize and get_online_features
    feature_service = feature_store.get_feature_service("EXTERNAL_FS_FEATURE_LIST")
    online_features = feature_store.get_online_features(
        features=feature_service, entity_rows=[{"üser id": 1, "PRODUCT_ACTION": "detail"}]
    ).to_dict()
    assert online_features == {
        "üser id": ["1"],
        "PRODUCT_ACTION": ["detail"],
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": [43.0],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": [475.38],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100": [47538.0],
    }


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_feature_materialize_service(
    app_container,
    session,
    user_entity,
    product_action_entity,
    features,
    deployed_feature_list,
):
    """
    Test FeatureMaterializeService
    """
    _ = deployed_feature_list

    await register_offline_store_feature_tables(app_container, features)

    primary_entity_to_feature_table = {}
    async for feature_table in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={},
    ):
        primary_entity_to_feature_table[
            tuple(sorted(feature_table.primary_entity_ids))
        ] = feature_table

    assert set(primary_entity_to_feature_table.keys()) == {
        (user_entity.id,),
        (product_action_entity.id,),
    }

    await check_feature_tables_populated(session, primary_entity_to_feature_table.values())

    await check_feast_registry(app_container)

    # Check offline store table for user entity
    service = app_container.feature_materialize_service
    feature_table_model = primary_entity_to_feature_table[(user_entity.id,)]
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df = await session.execute_query(f'SELECT * FROM "{feature_table_model.name}"')
    expected = [
        "__feature_timestamp",
        "üser id",
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100",
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h",
    ]
    assert set(df.columns.tolist()) == set(expected)
    assert df.shape[0] == 18
    assert df["__feature_timestamp"].nunique() == 2
    assert df["üser id"].isnull().sum() == 0

    # Materialize one more time
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df = await session.execute_query(f'SELECT * FROM "{feature_table_model.name}"')
    assert df.shape[0] == 27
    assert df["__feature_timestamp"].nunique() == 3
    assert df["üser id"].isnull().sum() == 0

    # Simulate a scheduled task
    task_payload = ScheduledFeatureMaterializeTaskPayload(
        catalog_id=feature_table_model.catalog_id,
        offline_store_feature_table_name=feature_table_model.name,
        offline_store_feature_table_id=feature_table_model.id,
    )
    await app_container.task_manager.submit(task_payload)
    df = await session.execute_query(f'SELECT * FROM "{feature_table_model.name}"')
    assert df.shape[0] == 36
    assert df["__feature_timestamp"].nunique() == 4
    assert df["üser id"].isnull().sum() == 0
