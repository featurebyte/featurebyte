"""
Unit test for Credential class
"""

from unittest import mock

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import (
    AccessTokenCredential,
    S3StorageCredential,
    UsernamePasswordCredential,
)
from featurebyte.api.credential import Credential
from featurebyte.exception import RecordDeletionException, RecordRetrievalException
from featurebyte.models.credential import CredentialModel
from tests.util.helper import compare_pydantic_obj


@pytest.fixture(name="credential")
def credential_fixture(snowflake_feature_store):
    """
    Credential fixture
    """
    credential = Credential.get(name=snowflake_feature_store.name)
    yield credential


def test_credential_creation__success(snowflake_feature_store, credential):
    """
    Credential creation success
    """
    with pytest.raises(RecordDeletionException) as exc:
        credential.delete()

    expected = (
        f"Cannot delete the last credential for feature store (ID: {snowflake_feature_store.id}). "
        "Please create a new credential before deleting this one to ensure continued access."
    )
    assert expected in str(exc.value)

    new_credential = Credential.create(
        feature_store_name=snowflake_feature_store.name,
        database_credential=UsernamePasswordCredential(username="username", password="password"),
        storage_credential=S3StorageCredential(
            s3_access_key_id="access_key_id", s3_secret_access_key="s3_secret_access_key"
        ),
    )
    assert new_credential.saved
    assert new_credential.id is not None
    assert new_credential.feature_store_id == snowflake_feature_store.id
    assert new_credential.name == snowflake_feature_store.name
    compare_pydantic_obj(
        new_credential.database_credential,
        expected={
            "type": "USERNAME_PASSWORD",
            "username": "********",
            "password": "********",
        },
    )
    compare_pydantic_obj(
        new_credential.storage_credential,
        expected={
            "type": "S3",
            "s3_access_key_id": "********",
            "s3_secret_access_key": "********",
        },
    )

    # delete the credential should work now
    credential.delete()


def test_credential_delete__success(snowflake_feature_store, credential):
    """
    Credential deletion success after featurestore is deleted
    """
    with pytest.raises(RecordDeletionException) as exc:
        credential.delete()

    expected = (
        f"Cannot delete the last credential for feature store (ID: {snowflake_feature_store.id}). "
        "Please create a new credential before deleting this one to ensure continued access."
    )
    assert expected in str(exc.value)

    snowflake_feature_store.delete()

    # delete the credential should work now
    credential.delete()


@pytest.mark.asyncio
async def test_credential_update(credential, snowflake_feature_store, persistent, user_id):
    """
    Test credential update
    """
    before_record = await persistent.find_one(
        collection_name="credential", query_filter={"feature_store_id": snowflake_feature_store.id}
    )
    credential_info = credential.info()
    assert credential_info["database_credential_type"] == "USERNAME_PASSWORD"
    assert credential_info["storage_credential_type"] is None

    with mock.patch(
        "featurebyte.routes.credential.controller.CredentialController._validate_credentials"
    ):  # Do not validate credentials
        credential.update_credentials(
            database_credential=AccessTokenCredential(access_token="access_token"),
            storage_credential=S3StorageCredential(
                s3_access_key_id="access_key_id", s3_secret_access_key="s3_secret_access_key"
            ),
        )

    after_record = await persistent.find_one(
        collection_name="credential", query_filter={"feature_store_id": snowflake_feature_store.id}
    )
    credential_info = credential.info()
    assert credential_info["database_credential_type"] == "ACCESS_TOKEN"
    assert credential_info["storage_credential_type"] == "S3"

    # check audit history
    audit_history = credential.audit()
    expected_audit_history = pd.DataFrame(
        [
            (
                "UPDATE",
                'update: "sf_featurestore"',
                "database_credential.access_token",
                np.nan,
                after_record["database_credential"]["access_token"],
            ),
            (
                "UPDATE",
                'update: "sf_featurestore"',
                "database_credential.password",
                before_record["database_credential"]["password"],
                np.nan,
            ),
            (
                "UPDATE",
                'update: "sf_featurestore"',
                "database_credential.type",
                "USERNAME_PASSWORD",
                "ACCESS_TOKEN",
            ),
            (
                "UPDATE",
                'update: "sf_featurestore"',
                "database_credential.username",
                before_record["database_credential"]["username"],
                np.nan,
            ),
            ("UPDATE", 'update: "sf_featurestore"', "storage_credential", None, np.nan),
            (
                "UPDATE",
                'update: "sf_featurestore"',
                "storage_credential.s3_access_key_id",
                np.nan,
                after_record["storage_credential"]["s3_access_key_id"],
            ),
            (
                "UPDATE",
                'update: "sf_featurestore"',
                "storage_credential.s3_secret_access_key",
                np.nan,
                after_record["storage_credential"]["s3_secret_access_key"],
            ),
            (
                "UPDATE",
                'update: "sf_featurestore"',
                "storage_credential.type",
                np.nan,
                "S3",
            ),
            (
                "UPDATE",
                'update: "sf_featurestore"',
                "updated_at",
                None,
                after_record["updated_at"].isoformat(),
            ),
            (
                "INSERT",
                'insert: "sf_featurestore"',
                "block_modification_by",
                np.nan,
                [],
            ),
            (
                "INSERT",
                'insert: "sf_featurestore"',
                "created_at",
                np.nan,
                before_record["created_at"].isoformat(),
            ),
            (
                "INSERT",
                'insert: "sf_featurestore"',
                "database_credential.password",
                np.nan,
                before_record["database_credential"]["password"],
            ),
            (
                "INSERT",
                'insert: "sf_featurestore"',
                "database_credential.type",
                np.nan,
                "USERNAME_PASSWORD",
            ),
            (
                "INSERT",
                'insert: "sf_featurestore"',
                "database_credential.username",
                np.nan,
                before_record["database_credential"]["username"],
            ),
            (
                "INSERT",
                'insert: "sf_featurestore"',
                "description",
                np.nan,
                None,
            ),
            (
                "INSERT",
                'insert: "sf_featurestore"',
                "feature_store_id",
                np.nan,
                str(snowflake_feature_store.id),
            ),
            ("INSERT", 'insert: "sf_featurestore"', "group_ids", np.nan, []),
            ("INSERT", 'insert: "sf_featurestore"', "is_deleted", np.nan, False),
            ("INSERT", 'insert: "sf_featurestore"', "name", np.nan, "sf_featurestore"),
            ("INSERT", 'insert: "sf_featurestore"', "storage_credential", np.nan, None),
            ("INSERT", 'insert: "sf_featurestore"', "updated_at", np.nan, None),
            ("INSERT", 'insert: "sf_featurestore"', "user_id", np.nan, str(user_id)),
        ],
        columns=["action_type", "name", "field_name", "old_value", "new_value"],
    )
    pd.testing.assert_frame_equal(
        audit_history[expected_audit_history.columns],
        expected_audit_history,
    )


def test_info(credential):
    """
    Test info
    """
    info_dict = credential.info(verbose=True)
    expected_info = {
        "name": "sf_featurestore",
        "updated_at": None,
        "database_credential_type": "USERNAME_PASSWORD",
        "storage_credential_type": None,
    }
    assert info_dict.items() > expected_info.items(), info_dict
    assert "created_at" in info_dict, info_dict
    expected_feature_store_info = {
        "name": "sf_featurestore",
        "updated_at": None,
        "source": "snowflake",
        "database_details": {
            "account": "sf_account",
            "warehouse": "sf_warehouse",
            "database_name": "sf_database",
            "schema_name": "sf_schema",
            "role_name": "TESTING",
        },
    }
    assert info_dict["feature_store_info"].items() > expected_feature_store_info.items()


def test_list_credentials(credential):
    """
    Test list credentials
    """
    credentials = Credential.list()
    assert credentials.to_dict("records") == [
        {
            "id": str(credential.id),
            "created_at": credential.created_at.isoformat(),
            "updated_at": None,
            "feature_store": "sf_featurestore",
            "database_credential": {
                "type": "USERNAME_PASSWORD",
                "username": "********",
                "password": "********",
            },
            "storage_credential": None,
        },
    ]


def test_get_credentials(credential):
    """
    Test get credentials
    """
    retrieved_credential = Credential.get(credential.name)
    assert retrieved_credential.json_dict() == credential.json_dict()
    assert set(retrieved_credential.json_dict().keys()) == {
        "_id",
        "name",
        "user_id",
        "created_at",
        "updated_at",
        "feature_store_id",
        "database_credential",
        "storage_credential",
        "block_modification_by",
        "description",
        "is_deleted",
    }


@pytest.mark.asyncio
async def test_get_credential_user_access(credential, persistent):
    """
    Get and list credentials should be accessible only to the owner
    """
    await persistent.update_one(
        collection_name=CredentialModel.collection_name(),
        query_filter={"_id": credential.id},
        update={"$set": {"user_id": ObjectId()}},
        user_id=None,
    )

    with pytest.raises(RecordRetrievalException) as exc:
        Credential.get_by_id(credential.id)
    assert (
        f'Credential (id: "{credential.id}") not found. ' "Please save the Credential object first."
    ) in str(exc.value)

    credentials = Credential.list()
    assert credentials.shape[0] == 0


def test_update_description(credential):
    """Test update description"""
    assert credential.description is None
    credential.update_description("new description")
    assert credential.description == "new description"
    assert credential.info()["description"] == "new description"
    credential.update_description(None)
    assert credential.description is None
    assert credential.info()["description"] is None
