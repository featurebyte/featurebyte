"""
Test document model for stored credentials
"""
import json

import pytest

from featurebyte import SnowflakeDetails
from featurebyte.enum import SourceType
from featurebyte.models.credential import (
    AccessTokenCredential,
    CredentialModel,
    CredentialType,
    S3StorageCredential,
    StorageCredentialType,
    UsernamePasswordCredential,
)
from featurebyte.models.feature_store import FeatureStoreModel


@pytest.fixture(name="storage_credential", params=[None, StorageCredentialType.S3])
def storage_credential_fixture(request):
    """
    Fixture for a StorageCredential object
    """
    if not request.param:
        return None
    if request.param == StorageCredentialType.S3:
        credential = S3StorageCredential(
            s3_access_key_id="access_key_id",
            s3_secret_access_key="secret_access_key",
        )
    else:
        raise ValueError("Invalid storage credential type")
    return credential.json_dict()


@pytest.fixture(
    name="credential",
    params=[None, CredentialType.USERNAME_PASSWORD, CredentialType.ACCESS_TOKEN],
)
def credential_fixture(request):
    """
    Fixture for a Credential object
    """
    if not request.param:
        return None
    if request.param == CredentialType.ACCESS_TOKEN:
        credential = AccessTokenCredential(access_token="access_token")
    elif request.param == CredentialType.USERNAME_PASSWORD:
        credential = UsernamePasswordCredential(username="test", password="password")
    else:
        raise ValueError("Invalid credential type")
    return credential.json_dict()


@pytest.fixture(name="feature_store")
def feature_store_fixture():
    """
    Fixture for a FeatureStoreModel object
    Returns
    -------
    FeatureStoreModel
        FeatureStoreModel object
    """
    return FeatureStoreModel(
        name="sf_featurestore",
        type=SourceType.SNOWFLAKE,
        details=SnowflakeDetails(
            account="account",
            warehouse="COMPUTE_WH",
            database="DATABASE",
            sf_schema="PUBLIC",
        ),
    )


def test_credentials_serialize_json(feature_store, credential, storage_credential):
    """
    Test serialization to json
    """
    credential = CredentialModel(
        name="SF Credentials",
        feature_store_id=feature_store.id,
        database_credential=credential,
        storage_credential=storage_credential,
    )
    credential_json = credential.json(by_alias=True)
    credential_dict = CredentialModel(**json.loads(credential_json)).json_dict()
    assert credential_dict == credential.json_dict()
