"""
Document model for stored credentials
"""
from typing import Union

from pydantic import StrictStr

from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel


class CredentialType(StrEnum):
    """
    Credential Type
    """

    USERNAME_PASSWORD = "USERNAME_PASSWORD"
    ACCESS_TOKEN = "ACCESS_TOKEN"
    S3 = "S3"


class UsernamePasswordCredential(FeatureByteBaseModel):
    """
    Username / Password credential
    """

    username: StrictStr
    password: StrictStr


class AccessTokenCredential(FeatureByteBaseModel):
    """
    Access token credential
    """

    access_token: StrictStr


class S3Credential(FeatureByteBaseModel):
    """
    S3 storage credential
    """

    s3_access_key_id: StrictStr
    s3_secret_access_key: StrictStr


RemoteStorageCredentialType = Union[S3Credential]


class RemoteStorageCredential(FeatureByteBaseModel):
    """
    Include remote storage credential
    """

    storage_credential: RemoteStorageCredentialType


class UsernamePasswordWithRemoteStorageCredential(
    UsernamePasswordCredential, RemoteStorageCredential
):
    """
    Username and Password with staging storage credential
    """


class AccessTokenWithRemoteStorageCredential(AccessTokenCredential, RemoteStorageCredential):
    """
    Access token with staging storage credential
    """


class Credential(FeatureByteBaseModel):
    """
    Credential model
    """

    name: StrictStr
    credential_type: CredentialType
    credential: Union[
        UsernamePasswordCredential,
        AccessTokenCredential,
        S3Credential,
        RemoteStorageCredential,
        UsernamePasswordWithRemoteStorageCredential,
        AccessTokenWithRemoteStorageCredential,
    ]
