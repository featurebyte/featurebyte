"""
Test document model for stored credentials
"""
import json

import pytest

from featurebyte.models.credential import Credential, CredentialType, UsernamePasswordCredential
from featurebyte.models.feature_store import FeatureStoreModel, SnowflakeDetails, SourceType


@pytest.fixture(name="username_password_credential")
def username_password_credential_fixture():
    """
    Fixture for a UsernamePasswordCredential object
    Returns
    -------
    UsernamePasswordCredential
        UsernamePasswordCredential object
    """
    return UsernamePasswordCredential(username="test", password="password")


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


@pytest.fixture(name="credential")
def credential_fixture(feature_store, username_password_credential):
    """
    Fixture for a Credential object
    Returns
    -------
    Credential
        Credential object
    """
    return Credential(
        name="SF Credentials",
        feature_store=feature_store,
        credential_type=CredentialType.USERNAME_PASSWORD,
        credential=username_password_credential,
    )


def test_credential_serialize_json(credential):
    """
    Test serialization to json
    """
    credential_json = credential.json()
    credential_dict = Credential(**json.loads(credential_json))
    assert credential_dict == credential.dict()
