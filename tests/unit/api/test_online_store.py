"""
Unit test for OnlineStore API
"""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
from pandas.testing import assert_frame_equal

from featurebyte import MySQLOnlineStoreDetails, UsernamePasswordCredential
from featurebyte.api.online_store import OnlineStore
from featurebyte.exception import ObjectHasBeenSavedError, RecordRetrievalException


def test_info(saved_mysql_online_store, catalog):
    """
    Test info
    """

    catalog.update_online_store(saved_mysql_online_store.name)
    info_dict = saved_mysql_online_store.info()
    expected_info = {
        "name": "mysql_online_store",
        "updated_at": None,
        "details": {
            "type": "mysql",
            "host": "mysql_host",
            "database": "mysql_database",
            "port": 3306,
            "credential": {
                "type": "USERNAME_PASSWORD",
                "username": "********",
                "password": "********",
            },
        },
        "catalogs": [{"name": "catalog"}],
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert "created_at" in info_dict, info_dict


def decrypt_credential(docs):
    """
    Decrypt credential in online store documents
    """
    for doc in docs:
        # decrypt credential
        credential = UsernamePasswordCredential(**doc["details"]["credential"])
        credential.decrypt_values()
        doc["details"]["credential"] = credential.model_dump()


@pytest_asyncio.fixture(name="saved_mysql_online_store")
async def saved_mysql_online_store_fixture(mysql_online_store, mock_get_persistent):
    """
    Test saving online store
    """
    persistent = mock_get_persistent.return_value
    assert mysql_online_store.saved is True
    assert mysql_online_store.created_at is not None
    docs, cnt = await persistent.find(collection_name="online_store", query_filter={})
    # decrypt credential
    decrypt_credential(docs)

    assert cnt == 1
    assert (
        docs[0].items()
        >= {
            "_id": mysql_online_store.id,
            "name": mysql_online_store.name,
            "created_at": mysql_online_store.created_at,
            "details": {
                "type": "mysql",
                "host": "mysql_host",
                "database": "mysql_database",
                "port": 3306,
                "credential": {
                    "type": "USERNAME_PASSWORD",
                    "username": "mysql_user",
                    "password": "mysql_password",
                },
            },
            "updated_at": None,
        }.items()
    )

    # test list online store
    online_stores = OnlineStore.list()
    assert_frame_equal(
        online_stores,
        pd.DataFrame({
            "id": [str(mysql_online_store.id)],
            "name": [mysql_online_store.name],
            "details": [mysql_online_store.details.model_dump()],
            "created_at": [mysql_online_store.created_at.isoformat()],
        }),
    )
    yield mysql_online_store


def test_save__duplicate_record_exception(saved_mysql_online_store):
    """
    Test duplicated record exception
    """
    # check conflict
    with pytest.raises(ObjectHasBeenSavedError) as exc:
        saved_mysql_online_store.save()
    expected_msg = f'OnlineStore (id: "{saved_mysql_online_store.id}") has been saved before.'
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_get(saved_mysql_online_store, persistent):
    """
    Test online store retrieval
    """
    loaded_online_store = OnlineStore.get(saved_mysql_online_store.name)
    assert loaded_online_store.saved is True
    assert loaded_online_store == saved_mysql_online_store
    assert OnlineStore.get_by_id(saved_mysql_online_store.id) == saved_mysql_online_store

    # get from database to access encrypted credential
    doc = await persistent.find_one(
        collection_name="online_store", query_filter={"_id": saved_mysql_online_store.id}
    )

    # check audit history
    audit_history = loaded_online_store.audit()
    expected_audit_history = pd.DataFrame(
        [
            ("block_modification_by", []),
            ("created_at", loaded_online_store.created_at.isoformat()),
            ("description", None),
            ("details.credential.password", doc["details"]["credential"]["password"]),
            ("details.credential.type", "USERNAME_PASSWORD"),
            ("details.credential.username", doc["details"]["credential"]["username"]),
            ("details.database", "mysql_database"),
            ("details.host", "mysql_host"),
            ("details.port", 3306),
            ("details.type", "mysql"),
            ("is_deleted", False),
            ("name", "mysql_online_store"),
            ("updated_at", None),
            ("user_id", None),
        ],
        columns=["field_name", "new_value"],
    )
    expected_audit_history["action_type"] = "INSERT"
    expected_audit_history["name"] = 'insert: "mysql_online_store"'
    expected_audit_history["old_value"] = np.nan
    pd.testing.assert_frame_equal(
        audit_history[expected_audit_history.columns], expected_audit_history
    )


def test_get__unexpected_retrieval_exception():
    """
    Test unexpected online store retrieval exception
    """
    # check unexpected creation exception
    with pytest.raises(RecordRetrievalException) as exc:
        OnlineStore.get("some random name")

    expected_msg = 'OnlineStore (name: "some random name") not found. Please save the OnlineStore object first.'
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_online_store_create(
    mock_get_persistent, snowflake_connector, snowflake_execute_query
):
    """
    Test the create online store static method.
    """
    online_store_name_in_test = "mysql_online_store"
    # We expect to get an error here since we have not stored a online store yet.
    with pytest.raises(RecordRetrievalException):
        OnlineStore.get(online_store_name_in_test)

    # before save
    persistent = mock_get_persistent.return_value
    docs, cnt = await persistent.find(collection_name="online_store", query_filter={})
    assert cnt == 0 and docs == []

    # call create - this should save, and return an instance
    mysql_online_store = OnlineStore.create(
        name=online_store_name_in_test,
        details=MySQLOnlineStoreDetails(
            host="mysql_host",
            database="mysql_database",
            port=3306,
            credential=UsernamePasswordCredential(
                username="mysql_user",
                password="mysql_password",
            ),
        ),
    )
    # assert that we have a correct instance returned
    assert isinstance(mysql_online_store, OnlineStore)

    # assert that we have saved the online store correctly
    assert mysql_online_store.saved is True
    assert mysql_online_store.created_at is not None
    docs, cnt = await persistent.find(collection_name="online_store", query_filter={})
    assert cnt == 1
    # decrypt credential
    decrypt_credential(docs)

    assert (
        docs[0].items()
        >= {
            "name": mysql_online_store.name,
            "created_at": mysql_online_store.created_at,
            "details": {
                "type": "mysql",
                "host": "mysql_host",
                "database": "mysql_database",
                "port": 3306,
                "credential": {
                    "type": "USERNAME_PASSWORD",
                    "username": "mysql_user",
                    "password": "mysql_password",
                },
            },
            "updated_at": None,
        }.items()
    )


def test_update_description(saved_mysql_online_store):
    """Test update description"""
    assert saved_mysql_online_store.description is None
    saved_mysql_online_store.update_description("new description")
    assert saved_mysql_online_store.description == "new description"
    assert saved_mysql_online_store.info()["description"] == "new description"
    saved_mysql_online_store.update_description(None)
    assert saved_mysql_online_store.description is None
    assert saved_mysql_online_store.info()["description"] is None


def test_delete_online_store(saved_mysql_online_store):
    """Test delete online store"""
    assert saved_mysql_online_store.saved is True
    saved_mysql_online_store.delete()
    assert saved_mysql_online_store.saved is False
