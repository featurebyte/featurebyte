"""
Tests for feature materialization service
"""
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio

import featurebyte as fb
from featurebyte.feast.schema.registry import FeastRegistryCreate


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

    # Feature with two entities
    feature_3 = event_view.groupby("PRODUCT_ACTION").aggregate_over(
        None,
        method="count",
        windows=["7d"],
        feature_names=["temp"],
    )["temp"]
    feature_3 = feature_1 + feature_3
    feature_3.name = "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE"

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


async def register_offline_store_feature_tables(app_container, session, features):
    """
    Register offline store feature tables
    """
    for feature in features:
        await app_container.offline_store_feature_table_manager_service.handle_online_enabled_feature(
            await app_container.feature_service.get_document(feature.id)
        )
        async for feature_table_model in app_container.offline_store_feature_table_service.list_documents_iterator(
            query_filter={"feature_ids": feature.id},
        ):
            await app_container.feature_materialize_service.initialize_new_columns(
                session=session, feature_table_model=feature_table_model
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


@pytest_asyncio.fixture(name="feast_registry")
async def feast_registry_fixture(app_container, features):
    """
    Fixture for feast registry
    """
    selected_features = []
    for feature in features:
        if len(feature.primary_entity_ids) == 1:
            selected_features.append(feature)
    feature_list = fb.FeatureList(selected_features, name="simple_feature_list_for_feast")
    feature_list.save()
    feature_list_models = [
        await app_container.feature_list_service.get_document(document_id=feature_list.id)
    ]
    feast_registry_model = await app_container.feast_registry_service.create_document(
        FeastRegistryCreate(project_name="featurebyte", feature_lists=feature_list_models)
    )
    yield feast_registry_model


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_feature_materialize_service(
    app_container,
    session,
    user_entity,
    product_action_entity,
    features,
    deployed_feature_list,
    feast_registry,
):
    """
    Test FeatureMaterializeService
    """
    _ = deployed_feature_list

    await register_offline_store_feature_tables(app_container, session, features)

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

    # Check offline store table for user entity
    service = app_container.feature_materialize_service
    feature_table_model = primary_entity_to_feature_table[(user_entity.id,)]
    await service.scheduled_materialize_features(
        session=session,
        feature_table_model=feature_table_model,
    )
    df = await session.execute_query(f'SELECT * FROM "{feature_table_model.name}"')
    expected = [
        "__feature_timestamp",
        "üser id",
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100",
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h",
        "__EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE__part0",
    ]
    assert set(df.columns.tolist()) == set(expected)
    assert df.shape[0] == 18
    assert df["__feature_timestamp"].nunique() == 2
    assert df["üser id"].isnull().sum() == 0

    # Materialize one more time
    await service.scheduled_materialize_features(
        session=session,
        feature_table_model=feature_table_model,
    )
    df = await session.execute_query(f'SELECT * FROM "{feature_table_model.name}"')
    assert df.shape[0] == 27
    assert df["__feature_timestamp"].nunique() == 3
    assert df["üser id"].isnull().sum() == 0

    feast_feature_store = await app_container.feast_feature_store_service.get_feast_feature_store(
        feast_registry.id
    )
    feast_feature_store.materialize(datetime(2000, 1, 1), datetime.now())
    feature_service = feast_feature_store.get_feature_service("simple_feature_list_for_feast")
    online_features = feast_feature_store.get_online_features(
        features=feature_service, entity_rows=[{"üser id": 1}]
    ).to_dict()
    assert online_features == {
        "üser id": ["1"],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": [475.38],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100": [47538.0],
    }
