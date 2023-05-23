"""
Test document model for stored credentials
"""
import copy
import json

import pytest

from featurebyte import SnowflakeDetails
from featurebyte.enum import SourceType
from featurebyte.models.credential import (
    AccessTokenCredential,
    AzureBlobStorageCredential,
    CredentialModel,
    DatabaseCredentialType,
    GCSStorageCredential,
    S3StorageCredential,
    StorageCredentialType,
    UsernamePasswordCredential,
)
from featurebyte.models.feature_store import FeatureStoreModel


@pytest.fixture(
    name="storage_credential", params=[None] + list(StorageCredentialType.__members__.values())
)
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
    elif request.param == StorageCredentialType.GCS:
        credential = GCSStorageCredential(
            service_account_info={
                "type": "service_account",
                "private_key": "private_key",
            }
        )
    elif request.param == StorageCredentialType.AZURE:
        credential = AzureBlobStorageCredential(
            account_name="account_name",
            account_key="account_key",
        )
    else:
        raise ValueError("Invalid storage credential type")
    return credential.json_dict()


@pytest.fixture(
    name="database_credential",
    params=[None] + list(DatabaseCredentialType.__members__.values()),
)
def database_credential_fixture(request):
    """
    Fixture for a DatabaseCredential object
    """
    if not request.param:
        return None
    if request.param == DatabaseCredentialType.ACCESS_TOKEN:
        credential = AccessTokenCredential(access_token="access_token")
    elif request.param == DatabaseCredentialType.USERNAME_PASSWORD:
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


def test_credentials_serialize_json(feature_store, database_credential, storage_credential):
    """
    Test serialization to json
    """
    credential = CredentialModel(
        name="SF Credentials",
        feature_store_id=feature_store.id,
        database_credential=database_credential,
        storage_credential=storage_credential,
    )
    credential_to_serialize = copy.deepcopy(credential)
    credential_to_serialize.encrypt()
    credential_json = credential_to_serialize.json(by_alias=True)
    deserialized_credential = CredentialModel(**json.loads(credential_json))

    # check that the credential is encrypted
    if database_credential or storage_credential:
        assert deserialized_credential.json_dict() != credential.json_dict()

    # check that the credential is decrypted correctly
    deserialized_credential.decrypt()
    assert deserialized_credential.json_dict() == credential.json_dict()
