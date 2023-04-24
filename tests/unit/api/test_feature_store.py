"""
Unit test for DatabaseSource
"""
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
from featurebyte.session.manager import SessionManager


@pytest.mark.asyncio
async def test_get_session(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store, credentials
):
    """
    Test DatabaseSource.get_session return expected session
    """
    _ = snowflake_connector, snowflake_execute_query
    session = await SessionManager(credentials=credentials).get_session(snowflake_feature_store)
    assert session.dict() == {
        "source_type": "snowflake",
        "account": "sf_account",
        "warehouse": "sf_warehouse",
        "sf_schema": "sf_schema",
        "database": "sf_database",
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


def test_list_tables(snowflake_connector, snowflake_execute_query, snowflake_feature_store, config):
    """
    Test list_tables return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    data_source = snowflake_feature_store.get_data_source()
    output = data_source.list_source_tables(database_name="sf_database", schema_name="sf_schema")
    assert output == ["sf_table", "sf_view"]


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
            "database": "sf_database",
            "sf_schema": "sf_schema",
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
                "database": "sf_database",
                "sf_schema": "sf_schema",
            },
            "updated_at": None,
        }.items()
    )

    # test list feature store
    feature_stores = FeatureStore.list()
    assert_frame_equal(
        feature_stores,
        pd.DataFrame(
            {
                "id": [snowflake_feature_store.id],
                "name": [snowflake_feature_store.name],
                "type": ["snowflake"],
                "created_at": [snowflake_feature_store.created_at],
            }
        ),
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


def test_save__unexpected_creation_exception(snowflake_feature_store_params):
    """
    Test unexpected feature store creation exception
    """
    # check unexpected creation exception
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.api_object.Configurations"):
            FeatureStore.create(**snowflake_feature_store_params)


def test_get(saved_snowflake_feature_store):
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
            ("created_at", loaded_feature_store.created_at.isoformat()),
            ("details.account", "sf_account"),
            ("details.database", "sf_database"),
            ("details.sf_schema", "sf_schema"),
            ("details.warehouse", "sf_warehouse"),
            ("name", "sf_featurestore"),
            ("type", "snowflake"),
            ("updated_at", None),
            ("user_id", None),
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
            sf_schema="sf_schema",
            database="sf_database",
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
                "database": "sf_database",
                "sf_schema": "sf_schema",
            },
            "updated_at": None,
        }.items()
    )
