import contextlib
from copy import deepcopy
from unittest.mock import Mock

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import Feature, FeatureJobSetting, FeatureList
from featurebyte.enum import InternalName
from featurebyte.migration.service.data_warehouse import (
    DataWarehouseMigrationServiceV6,
    TileColumnTypeExtractor,
)
from featurebyte.models.base import DEFAULT_CATALOG_ID


@pytest.fixture(scope="session")
def user():
    """
    Mock user
    """
    user = Mock()
    user.id = ObjectId()
    return user


def get_tile_id(feature: Feature):
    """
    Helper function to retrieve the tile id of a feature
    """
    graph, _ = feature.extract_pruned_graph_and_node()
    groupby_node = graph.get_node_by_name("groupby_1")
    return groupby_node.parameters.dict()["tile_id"]


def get_aggregation_id(feature: Feature):
    """
    Helper function to retrieve the aggregation id of a feature
    """
    graph, _ = feature.extract_pruned_graph_and_node()
    groupby_node = graph.get_node_by_name("groupby_1")
    return groupby_node.parameters.dict()["aggregation_id"]


@contextlib.asynccontextmanager
async def revert_when_done(session, table_name):
    """
    Backup a table and revert it at the end of a context

    Mainly used to prevent unintended interference between tests (a failed migration test should not
    cause other tests to fail)
    """
    backup_name = f"{table_name}_BACKUP"
    await session.execute_query(f"CREATE OR REPLACE TABLE {backup_name} CLONE {table_name}")
    try:
        yield
    finally:
        await session.execute_query(f"CREATE OR REPLACE TABLE {table_name} CLONE {backup_name}")


@pytest_asyncio.fixture(name="bad_feature_stores", scope="session")
async def bad_feature_stores_fixture(feature_store, persistent, user, session):
    """
    Invalid FeatureStore documents to test error handling during migration
    """
    feature_store_doc = await persistent.find_one("feature_store", {"_id": feature_store.id})
    del feature_store_doc["_id"]

    bad_feature_store_docs = []

    # FeatureStore without credentials configured
    feature_store = deepcopy(feature_store_doc)
    feature_store["name"] = "snowflake_featurestore_no_creds"
    feature_store["details"]["sf_schema"] += "_1"
    await persistent.insert_one(
        collection_name="feature_store", document=feature_store, user_id=user.id
    )
    bad_feature_store_docs.append(feature_store)

    # FeatureStore with wrong credentials
    feature_store = deepcopy(feature_store_doc)
    feature_store["name"] = "snowflake_featurestore_wrong_creds"
    feature_store["details"]["sf_schema"] += "_2"
    await persistent.insert_one(
        collection_name="feature_store", document=feature_store, user_id=user.id
    )
    bad_feature_store_docs.append(feature_store)

    # FeatureStore that can no longer instantiate a session object because of working schema
    # collision (they used to be allowed and might still exist as old documents)
    feature_store = deepcopy(feature_store_doc)
    feature_store["name"] = "snowflake_featurestore_invalid_because_same_schema_a"
    feature_store["details"]["sf_schema"] += "_3"
    await persistent.insert_one(
        collection_name="feature_store", document=feature_store, user_id=user.id
    )
    feature_store = deepcopy(feature_store_doc)
    feature_store["name"] = "snowflake_featurestore_invalid_because_same_schema_b"
    feature_store["details"]["sf_schema"] += "_3"
    await persistent.insert_one(
        collection_name="feature_store", document=feature_store, user_id=user.id
    )
    bad_feature_store_docs.append(feature_store)

    # FeatureStore with unreachable host
    feature_store = deepcopy(feature_store_doc)
    feature_store["name"] = "snowflake_featurestore_unreachable"
    feature_store["details"]["account"] = "this.snowflake.account.does.not.exist.gcp"
    feature_store["details"]["sf_schema"] += "_4"
    await persistent.insert_one(
        collection_name="feature_store", document=feature_store, user_id=user.id
    )

    yield

    for doc in bad_feature_store_docs:
        drop_schema_query = f'DROP SCHEMA IF EXISTS {doc["details"]["sf_schema"]}'
        await session.execute_query(drop_schema_query)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.skip(reason="irrelevant test to be removed")
async def test_data_warehouse_migration_v6(
    user,
    persistent,
    event_table,
    session,
    bad_feature_stores,
    get_credential,
):
    """
    Test data warehouse migration
    """
    _ = bad_feature_stores
    event_view = event_table.get_view()
    features_1 = event_view.groupby("ÜSER ID").aggregate_over(
        method="count",
        windows=["7d"],
        feature_names=["test_data_warehouse_migration_v6_feature_count"],
        feature_job_setting=FeatureJobSetting(
            frequency="42m",
            blind_spot="5m",
            time_modulo_frequency="10m",
        ),
    )
    features_2 = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ËVENT_TIMESTAMP",
        method="latest",
        windows=["7d"],
        feature_names=["test_data_warehouse_migration_v6_feature_latest_event_time"],
        feature_job_setting=FeatureJobSetting(
            frequency="42m",
            blind_spot="5m",
            time_modulo_frequency="10m",
        ),
    )
    feature_list_1 = FeatureList([features_1], name="test_data_warehouse_migration_v6_list_1")
    feature_list_1.save()
    feature_list_2 = FeatureList([features_2], name="test_data_warehouse_migration_v6_list_2")
    feature_list_2.save()
    preview_param = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "üser id": 1,
    }
    observations_set = pd.DataFrame([preview_param])
    _ = feature_list_1.compute_historical_features(observations_set)
    _ = feature_list_2.compute_historical_features(observations_set)

    # Get tile id to check (both features should have the same tile id)
    expected_tile_id = get_tile_id(features_1["test_data_warehouse_migration_v6_feature_count"])
    assert expected_tile_id == get_tile_id(
        features_2["test_data_warehouse_migration_v6_feature_latest_event_time"]
    )
    latest_feature_agg_id = get_aggregation_id(
        features_2["test_data_warehouse_migration_v6_feature_latest_event_time"]
    )
    latest_feature_tile_column = f"value_{latest_feature_agg_id}"

    async def _retrieve_tile_registry():
        df = await session.execute_query(
            f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{expected_tile_id}' AND AGGREGATION_ID = '{latest_feature_agg_id}'"
        )
        return df.sort_values("TILE_ID")

    async def _retrieve_tile_table():
        df = await session.execute_query(f"SELECT * FROM {expected_tile_id}")
        return df

    async def _get_migration_version():
        df = await session.execute_query(f"SELECT * FROM METADATA_SCHEMA")
        return df["MIGRATION_VERSION"].iloc[0]

    # New TILE_REGISTRY always has VALUE_COLUMN_TYPES column correctly setup
    df_expected = await _retrieve_tile_registry()

    async with revert_when_done(session, "TILE_REGISTRY"):
        # Simulate migration scenario where VALUE_COLUMN_TYPES column is missing
        await session.execute_query("ALTER TABLE TILE_REGISTRY DROP COLUMN VALUE_COLUMN_TYPES")
        assert "VALUE_COLUMN_TYPES" not in (await _retrieve_tile_registry())

        # Simulate the case when the column in the tile table has wrong type to be corrected (FLOAT
        # is the wrong type, should be TIMESTAMP_NTZ)
        await session.execute_query(
            f"ALTER TABLE {expected_tile_id} DROP COLUMN {latest_feature_tile_column}"
        )
        await session.execute_query(
            f"ALTER TABLE {expected_tile_id} ADD COLUMN {latest_feature_tile_column} FLOAT DEFAULT NULL"
        )

        # Simulate missing MIGRATION_VERSION
        await session.execute_query(
            f"ALTER TABLE METADATA_SCHEMA DROP COLUMN {InternalName.MIGRATION_VERSION}"
        )

        # Run migration
        service = DataWarehouseMigrationServiceV6(
            user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        )
        service.set_credential_callback(get_credential)
        await service.add_tile_value_types_column()

        # Check migration correctly adds the VALUE_COLUMN_TYPES column
        df_migrated = await _retrieve_tile_registry()
        assert (
            df_migrated["VALUE_COLUMN_TYPES"].tolist() == df_expected["VALUE_COLUMN_TYPES"].tolist()
        )

        df_migrated_tile_table = await _retrieve_tile_table()
        assert (
            str(df_migrated_tile_table[latest_feature_tile_column.upper()].dtype)
            == "datetime64[ns, UTC]"
        )

        assert await _get_migration_version() == 6


@pytest.mark.asyncio
async def test_tile_column_type_extractor_build_tile_column_name_to_type_mapping(
    user,
    persistent,
):
    """Test build_tile_column_name_to_type_mapping"""
    extractor = TileColumnTypeExtractor(user=user, persistent=persistent)
    await extractor.setup()
    assert extractor.tile_column_name_to_type == {}
