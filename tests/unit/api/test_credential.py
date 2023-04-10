"""
Unit test for Credential class
"""
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from featurebyte import AccessTokenCredential, S3StorageCredential, UsernamePasswordCredential
from featurebyte.api.credential import Credential
from featurebyte.exception import DuplicatedRecordException


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
    Credential.delete(credential.id)
    with mock.patch("featurebyte.service.credential.CredentialService._validate_credential"):
        new_credential = Credential.create(
            feature_store_name=snowflake_feature_store.name,
            database_credential=UsernamePasswordCredential(
                username="username", password="password"
            ),
            storage_credential=S3StorageCredential(
                s3_access_key_id="access_key_id", s3_secret_access_key="s3_secret_access_key"
            ),
        )
    assert new_credential.saved
    assert new_credential.id is not None
    assert new_credential.feature_store_id == snowflake_feature_store.id
    assert new_credential.name == snowflake_feature_store.name
    assert new_credential.database_credential_type == "USERNAME_PASSWORD"
    assert new_credential.storage_credential_type == "S3"


def test_credential_creation__conflict(snowflake_feature_store):
    """
    Credential creation conflict
    """
    with mock.patch("featurebyte.service.credential.CredentialService._validate_credential"):
        with pytest.raises(DuplicatedRecordException) as exc:
            Credential.create(feature_store_name=snowflake_feature_store.name)
    assert f'Credential (feature_store_id: "{snowflake_feature_store.id}") already exists' in str(
        exc.value
    )


@pytest.mark.asyncio
async def test_credential_update(credential, snowflake_feature_store, persistent):
    """
    Test credential update
    """
    before_record = await persistent.find_one(
        collection_name="credential", query_filter={"feature_store_id": snowflake_feature_store.id}
    )
    credential_info = credential.info()
    assert credential_info["database_credential_type"] == "USERNAME_PASSWORD"
    assert credential_info["storage_credential_type"] is None

    with mock.patch("featurebyte.service.credential.CredentialService._validate_credential"):
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
                "feature_store_id",
                np.nan,
                str(snowflake_feature_store.id),
            ),
            ("INSERT", 'insert: "sf_featurestore"', "name", np.nan, "sf_featurestore"),
            ("INSERT", 'insert: "sf_featurestore"', "storage_credential", np.nan, None),
            ("INSERT", 'insert: "sf_featurestore"', "updated_at", np.nan, None),
            ("INSERT", 'insert: "sf_featurestore"', "user_id", np.nan, None),
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
            "database": "sf_database",
            "sf_schema": "sf_schema",
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
            "created_at": pd.to_datetime(credential.created_at),
            "database_credential_type": "USERNAME_PASSWORD",
            "feature_store": "sf_featurestore",
            "storage_credential_type": None,
            "updated_at": None,
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
    }
