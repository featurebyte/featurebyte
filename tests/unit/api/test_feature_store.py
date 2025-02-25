"""
Unit test for DatabaseSource
"""

import json
from unittest import mock
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
from pandas.testing import assert_frame_equal

from featurebyte import UsernamePasswordCredential
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.source_table import SourceTable
from featurebyte.enum import SourceType
from featurebyte.exception import (
    ObjectHasBeenSavedError,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.query_graph.node.schema import SnowflakeDetails


def test_save__unexpected_creation_exception(snowflake_feature_store_params):
    """
    Test unexpected feature store creation exception
    """
    # check unexpected creation exception
    with mock.patch("featurebyte.api.savable_api_object.Configurations"):
        with pytest.raises(RecordCreationException):
            FeatureStore.create(**snowflake_feature_store_params)


@pytest.mark.asyncio
async def test_get_session(
    snowflake_connector,
    snowflake_execute_query,
    snowflake_feature_store,
    snowflake_credentials,
    session_manager_service,
):
    """
    Test DatabaseSource.get_session return expected session
    """
    _ = snowflake_connector, snowflake_execute_query
    session = await session_manager_service.get_session(
        snowflake_feature_store, snowflake_credentials
    )
    assert session.model_dump() == {
        "source_type": "snowflake",
        "account": "sf_account",
        "warehouse": "sf_warehouse",
        "schema_name": "sf_schema",
        "database_name": "sf_database",
        "role_name": "TESTING",
        "database_credential": {
            "type": "USERNAME_PASSWORD",
            "username": "sf_user",
            "password": "sf_password",
        },
    }


def test_list_databases(snowflake_connector, snowflake_execute_query, snowflake_feature_store):
    """
    Test list_databases return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    data_source = snowflake_feature_store.get_data_source()
    output = data_source.list_databases()
    assert output == ["sf_database"]


def test_list_schema(snowflake_connector, snowflake_execute_query, snowflake_feature_store, config):
    """
    Test test_list_schema return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    data_source = snowflake_feature_store.get_data_source()
    output = data_source.list_schemas(database_name="sf_database")
    assert output == ["sf_schema"]


def test_list_tables(
    snowflake_connector,
    snowflake_execute_query,
    snowflake_feature_store,
    config,
    mock_is_featurebyte_schema,
):
    """
    Test list_tables return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    data_source = snowflake_feature_store.get_data_source()
    output = data_source.list_source_tables(database_name="sf_database", schema_name="sf_schema")
    assert output == [
        "dimension_table",
        "fixed_table",
        "items_table",
        "items_table_same_event_id",
        "non_scalar_table",
        "scd_table",
        "scd_table_state_map",
        "sf_table",
        "sf_table_no_tz",
        "sf_view",
    ]


def test__getitem__retrieve_database_table(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store, config
):
    """
    Test retrieval database table by indexing
    """
    _ = snowflake_connector, snowflake_execute_query
    data_source = snowflake_feature_store.get_data_source()
    database_table = data_source.get_source_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table",
    )
    assert isinstance(database_table, SourceTable)


def test_info(saved_snowflake_feature_store):
    """
    Test info
    """
    info_dict = saved_snowflake_feature_store.info()
    expected_info = {
        "name": "sf_featurestore",
        "updated_at": None,
        "source": "snowflake",
        "database_details": {
            "account": "sf_account",
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "role_name": "TESTING",
            "warehouse": "sf_warehouse",
        },
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert "created_at" in info_dict, info_dict


@pytest_asyncio.fixture(name="saved_snowflake_feature_store")
async def saved_snowflake_feature_store_fixture(
    snowflake_feature_store, mock_get_persistent, snowflake_connector, snowflake_execute_query
):
    """
    Test saving feature store
    """
    _ = snowflake_connector, snowflake_execute_query

    persistent = mock_get_persistent.return_value
    assert snowflake_feature_store.saved is True
    assert snowflake_feature_store.created_at is not None
    docs, cnt = await persistent.find(collection_name="feature_store", query_filter={})
    assert cnt == 1
    assert (
        docs[0].items()
        >= {
            "_id": snowflake_feature_store.id,
            "name": snowflake_feature_store.name,
            "created_at": snowflake_feature_store.created_at,
            "type": "snowflake",
            "details": {
                "account": "sf_account",
                "warehouse": "sf_warehouse",
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "role_name": "TESTING",
            },
            "updated_at": None,
        }.items()
    )

    # test list feature store
    feature_stores = FeatureStore.list()
    assert_frame_equal(
        feature_stores,
        pd.DataFrame({
            "id": [str(snowflake_feature_store.id)],
            "name": [snowflake_feature_store.name],
            "type": ["snowflake"],
            "created_at": [snowflake_feature_store.created_at.isoformat()],
        }),
    )
    yield snowflake_feature_store


def test_save__duplicate_record_exception(saved_snowflake_feature_store):
    """
    Test duplicated record exception
    """
    # check conflict
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_snowflake_feature_store.save()
    expected_msg = f'FeatureStore (id: "{saved_snowflake_feature_store.id}") has been saved before.'
    assert expected_msg in str(exc.value)


def test_get(saved_snowflake_feature_store, user_id):
    """
    Test feature store retrieval
    """
    loaded_feature_store = FeatureStore.get(saved_snowflake_feature_store.name)
    assert loaded_feature_store.saved is True
    assert loaded_feature_store == saved_snowflake_feature_store
    assert FeatureStore.get_by_id(saved_snowflake_feature_store.id) == saved_snowflake_feature_store

    # check audit history
    audit_history = loaded_feature_store.audit()
    expected_audit_history = pd.DataFrame(
        [
            ("block_modification_by", []),
            ("created_at", loaded_feature_store.created_at.isoformat()),
            ("description", None),
            ("details.account", "sf_account"),
            ("details.database_name", "sf_database"),
            ("details.role_name", "TESTING"),
            ("details.schema_name", "sf_schema"),
            ("details.warehouse", "sf_warehouse"),
            ("is_deleted", False),
            ("max_query_concurrency", None),
            ("name", "sf_featurestore"),
            ("type", "snowflake"),
            ("updated_at", None),
            ("user_id", str(user_id)),
        ],
        columns=["field_name", "new_value"],
    )
    expected_audit_history["action_type"] = "INSERT"
    expected_audit_history["name"] = 'insert: "sf_featurestore"'
    expected_audit_history["old_value"] = np.nan
    pd.testing.assert_frame_equal(
        audit_history[expected_audit_history.columns], expected_audit_history
    )


def test_get__unexpected_retrieval_exception():
    """
    Test unexpected feature store retrieval exception
    """
    # check unexpected creation exception
    with pytest.raises(RecordRetrievalException) as exc:
        FeatureStore.get("some random name")

    expected_msg = 'FeatureStore (name: "some random name") not found. Please save the FeatureStore object first.'
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_feature_store_create(
    mock_get_persistent, snowflake_connector, snowflake_execute_query
):
    """
    Test the create feature store static method.
    """
    _ = snowflake_connector, snowflake_execute_query

    feature_store_name_in_test = "sf_featurestore"
    # We expect to get an error here since we have not stored a feature store yet.
    with pytest.raises(RecordRetrievalException):
        FeatureStore.get(feature_store_name_in_test)

    # before save
    persistent = mock_get_persistent.return_value
    docs, cnt = await persistent.find(collection_name="feature_store", query_filter={})
    assert cnt == 0 and docs == []

    # call create - this should save, and return an instance
    snowflake_feature_store = FeatureStore.create(
        name=feature_store_name_in_test,
        source_type=SourceType.SNOWFLAKE,
        details=SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            schema_name="sf_schema",
            database_name="sf_database",
            role_name="TESTING",
        ),
        database_credential=UsernamePasswordCredential(
            username="sf_username", password="sf_password"
        ),
    )
    # assert that we have a correct instance returned
    assert isinstance(snowflake_feature_store, FeatureStore)

    # assert that we have saved the feature store correctly
    assert snowflake_feature_store.saved is True
    assert snowflake_feature_store.created_at is not None
    docs, cnt = await persistent.find(collection_name="feature_store", query_filter={})
    assert cnt == 1
    assert (
        docs[0].items()
        >= {
            "name": snowflake_feature_store.name,
            "created_at": snowflake_feature_store.created_at,
            "type": "snowflake",
            "details": {
                "account": "sf_account",
                "warehouse": "sf_warehouse",
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "role_name": "TESTING",
            },
            "updated_at": None,
        }.items()
    )


def test_update_description(saved_snowflake_feature_store):
    """Test update description"""
    assert saved_snowflake_feature_store.description is None
    saved_snowflake_feature_store.update_description("new description")
    assert saved_snowflake_feature_store.description == "new description"
    assert saved_snowflake_feature_store.info()["description"] == "new description"
    saved_snowflake_feature_store.update_description(None)
    assert saved_snowflake_feature_store.description is None
    assert saved_snowflake_feature_store.info()["description"] is None


def test_delete_feature_store(saved_snowflake_feature_store):
    """Test delete feature store"""
    assert saved_snowflake_feature_store.saved is True
    saved_snowflake_feature_store.delete()
    assert saved_snowflake_feature_store.saved is False


def test_update_details(saved_snowflake_feature_store):
    """
    Test update feature store details
    """
    assert saved_snowflake_feature_store.details.warehouse == "sf_warehouse"
    saved_snowflake_feature_store.update_details(warehouse="new_warehouse")
    assert saved_snowflake_feature_store.details.warehouse == "new_warehouse"


def test_list_feature_store_with_invalid_item(snowflake_feature_store, caplog):
    """Test list feature store with invalid item"""
    fs_dict = json.loads(snowflake_feature_store.cached_model.model_dump_json())
    with patch(
        "featurebyte.api.api_handler.base.iterate_api_object_using_paginated_routes"
    ) as mock_iterate:
        mock_iterate.return_value = [fs_dict, {"name": "invalid"}]
        output = FeatureStore.list()
        assert output.shape[0] == 1
        assert output["name"].iloc[0] == snowflake_feature_store.name

    expected_warning = (
        "Failed to parse list item: {'name': 'invalid'} with schema: "
        "<class 'featurebyte.models.feature_store.FeatureStoreModel'>"
    )
    assert expected_warning in caplog.text
