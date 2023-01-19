import contextlib
from copy import deepcopy
from unittest.mock import Mock

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import EventView, Feature, FeatureList
from featurebyte.migration.service.data_warehouse import DataWarehouseMigrationService
from featurebyte.utils.credential import get_credential


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


@pytest_asyncio.fixture(name="bad_feature_stores")
async def bad_feature_stores_fixture(snowflake_feature_store, persistent, user):
    """
    Invalid FeatureStore documents to test error handling during migration
    """
    feature_store_doc = await persistent.find_one(
        "feature_store", {"_id": snowflake_feature_store.id}
    )
    del feature_store_doc["_id"]

    # FeatureStore without credentials configured
    feature_store = deepcopy(feature_store_doc)
    feature_store["name"] = "snowflake_featurestore_no_creds"
    feature_store["details"]["sf_schema"] += "_1"
    await persistent.insert_one(
        collection_name="feature_store", document=feature_store, user_id=user.id
    )

    # FeatureStore with wrong credentials
    feature_store = deepcopy(feature_store_doc)
    feature_store["name"] = "snowflake_featurestore_wrong_creds"
    feature_store["details"]["sf_schema"] += "_2"
    await persistent.insert_one(
        collection_name="feature_store", document=feature_store, user_id=user.id
    )

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


@pytest.mark.asyncio
async def test_data_warehouse_migration_v6(
    user,
    persistent,
    event_data,
    snowflake_session,
    bad_feature_stores,
):
    """
    Test data warehouse migration
    """
    _ = bad_feature_stores
    event_view = EventView.from_event_data(event_data)
    features = event_view.groupby("USER ID").aggregate_over(
        method="count",
        windows=["7d"],
        feature_names=["test_data_warehouse_migration_v6_feature"],
        feature_job_setting={
            "frequency": "42m",
            "blind_spot": "5m",
            "time_modulo_frequency": "10m",
        },
    )
    feature_list = FeatureList([features], name="test_data_warehouse_migration_v6_list")
    feature_list.save()
    preview_param = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "user id": 1,
    }
    observations_set = pd.DataFrame([preview_param])
    _ = feature_list.get_historical_features(observations_set)

    expected_tile_id = get_tile_id(features["test_data_warehouse_migration_v6_feature"])

    async def _retrieve_tile_registry():
        df = await snowflake_session.execute_query(
            f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{expected_tile_id}'"
        )
        return df.sort_values("TILE_ID")

    # New TILE_REGISTRY always has VALUE_COLUMN_TYPES column correctly setup
    df_expected = await _retrieve_tile_registry()

    async with revert_when_done(snowflake_session, "TILE_REGISTRY"):

        # Simulate migration scenario where VALUE_COLUMN_TYPES column is missing
        await snowflake_session.execute_query(
            "ALTER TABLE TILE_REGISTRY DROP COLUMN VALUE_COLUMN_TYPES"
        )
        assert "VALUE_COLUMN_TYPES" not in (await _retrieve_tile_registry())

        # # Run migration
        service = DataWarehouseMigrationService(user=user, persistent=persistent)
        service.set_credential_callback(get_credential)
        await service.add_tile_value_types_column()

        # Check migration correctly adds the VALUE_COLUMN_TYPES column
        df_migrated = await _retrieve_tile_registry()
        assert (
            df_migrated["VALUE_COLUMN_TYPES"].tolist() == df_expected["VALUE_COLUMN_TYPES"].tolist()
        )
