"""
Document model for stored credentials
"""
# pylint: disable=too-few-public-methods
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


class UsernamePasswordCredential(FeatureByteBaseModel):
    """
    Username / Password credential
    """

    username: StrictStr
    password: StrictStr


class AccessTokenCredential(FeatureByteBaseModel):
    """
    Access token based credential
    """

    access_token: StrictStr


class Credential(FeatureByteBaseModel):
    """
    Credential model
    """

    name: StrictStr
    credential_type: CredentialType
    credential: Union[UsernamePasswordCredential, AccessTokenCredential]
