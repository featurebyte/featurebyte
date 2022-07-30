"""
Unit test for DatabaseSource
"""
import asyncio
from unittest.mock import patch

import pytest

from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.feature_store import FeatureStore
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)


def test_get_session(snowflake_connector, snowflake_execute_query, snowflake_feature_store, config):
    """
    Test DatabaseSource.get_session return expected session
    """
    _ = snowflake_connector, snowflake_execute_query
    session = snowflake_feature_store.get_session(credentials=config.credentials)
    assert session.dict() == {
        "source_type": "snowflake",
        "account": "sf_account",
        "warehouse": "sf_warehouse",
        "sf_schema": "sf_schema",
        "database": "sf_database",
        "username": "sf_user",
        "password": "sf_password",
    }


def test_list_databases(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store, config
):
    """
    Test list_databases return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    output = snowflake_feature_store.list_databases(credentials=config.credentials)
    assert output == ["sf_database"]


def test_list_schema(snowflake_connector, snowflake_execute_query, snowflake_feature_store, config):
    """
    Test test_list_schema return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    output = snowflake_feature_store.list_schemas(credentials=config.credentials)
    assert output == ["sf_schema"]


def test_list_tables(snowflake_connector, snowflake_execute_query, snowflake_feature_store, config):
    """
    Test list_tables return expected results
    """
    _ = snowflake_connector, snowflake_execute_query
    output = snowflake_feature_store.list_tables(credentials=config.credentials)
    assert output == ["sf_table", "sf_view"]


def test__getitem__retrieve_database_table(
    snowflake_connector, snowflake_execute_query, snowflake_feature_store, config
):
    """
    Test retrieval database table by indexing
    """
    _ = snowflake_connector, snowflake_execute_query
    database_table = snowflake_feature_store.get_table(
        database_name="sf_database",
        schema_name="sf_schema",
        table_name="sf_table",
        credentials=config.credentials,
    )
    assert isinstance(database_table, DatabaseTable)


@pytest.fixture(name="saved_snowflake_feature_store")
def saved_snowflake_feature_store_fixture(snowflake_feature_store, mock_get_persistent):
    """
    Test saving feature store
    """
    # before save
    assert snowflake_feature_store.created_at is None
    feature_store_id = snowflake_feature_store.id
    persistent = mock_get_persistent.return_value
    docs, cnt = asyncio.run(persistent.find(collection_name="feature_store", query_filter={}))
    assert cnt == 0 and docs == []

    # after save
    snowflake_feature_store.save()
    assert snowflake_feature_store.id == feature_store_id
    assert snowflake_feature_store.created_at is not None
    docs, cnt = asyncio.run(persistent.find(collection_name="feature_store", query_filter={}))
    assert cnt == 1
    assert (
        docs[0].items()
        >= {
            "_id": feature_store_id,
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
    assert FeatureStore.list() == ["sf_featurestore"] == [snowflake_feature_store.name]
    yield snowflake_feature_store


def test_save__duplicate_record_exception(saved_snowflake_feature_store):
    """
    Test duplicated record exception
    """
    # check conflict
    with pytest.raises(DuplicatedRecordException) as exc:
        saved_snowflake_feature_store.save()
    expected_msg = (
        f'FeatureStore (feature_store.id: "{saved_snowflake_feature_store.id}") already exists.'
    )
    assert expected_msg in str(exc.value)


def test_save__unexpected_creation_exception(snowflake_feature_store):
    """
    Test unexpected feature store creation exception
    """
    # check unexpected creation exception
    with pytest.raises(RecordCreationException):
        with patch("featurebyte.api.api_object.Configurations"):
            snowflake_feature_store.save()


def test_get(saved_snowflake_feature_store):
    """
    Test feature store retrieval
    """
    loaded_feature_store = FeatureStore.get(saved_snowflake_feature_store.name)
    assert loaded_feature_store == saved_snowflake_feature_store


def test_get__unexpected_retrieval_exception():
    """
    Test unexpected feature store retrieval exception
    """
    # check unexpected creation exception
    with pytest.raises(RecordRetrievalException) as exc:
        FeatureStore.get("some random name")
    expected_msg = 'FeatureStore (feature_store.name: "some random name") not found!'
    assert expected_msg in str(exc.value)
