"""
Test document model for stored credentials
"""
import json
from datetime import datetime

import pytest
from bson import ObjectId

from featurebyte.models.credential import Credential, CredentialType, UsernamePasswordCredential
from featurebyte.models.event_data import DatabaseSourceModel, SnowflakeDetails, SourceType


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


@pytest.fixture(name="source")
def source_fixture():
    """
    Fixture for a Source object
    Returns
    -------
    Source
        Source object
    """
    return DatabaseSourceModel(
        type=SourceType.SNOWFLAKE,
        details=SnowflakeDetails(
            account="account",
            warehouse="COMPUTE_WH",
            database="DATABASE",
            sf_schema="PUBLIC",
        ),
    )


@pytest.fixture(name="credential")
def credential_fixture(source, username_password_credential):
    """
    Fixture for a Credential object
    Returns
    -------
    Credential
        Credential object
    """
    return Credential(
        user_id=ObjectId(),
        name="SF Credentials",
        created_at=datetime.utcnow(),
        source=source,
        credential_type=CredentialType.USERNAME_PASSWORD,
        credential=username_password_credential,
    )


def test_credential_serialize_json(credential):
    """
    Test serialization to json
    """
    credential_json = credential.json()
    credential_dict = Credential(**json.loads(credential_json))
    assert credential_dict == {
        "name": credential.name,
        "source": credential.source,
        "credential_type": credential.credential_type,
        "credential": credential.credential,
    }
