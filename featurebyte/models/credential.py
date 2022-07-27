"""
Document model for stored credentials
"""
# pylint: disable=too-few-public-methods
from typing import Union

from enum import Enum

import pymongo
from pydantic import StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import FeatureStoreModel


class CredentialType(str, Enum):
    """
    Credential Type
    """

    USERNAME_PASSWORD = "USERNAME_PASSWORD"


class UsernamePasswordCredential(FeatureByteBaseModel):
    """
    Username / Password credential
    """

    username: StrictStr
    password: StrictStr


class Credential(FeatureByteBaseModel):
    """
    Credential model
    """

    name: StrictStr
    credential_type: CredentialType
    credential: Union[UsernamePasswordCredential]

    class Settings:
        """
        Collection settings for Credential document
        """

        name = "credential"
        indexes = [
            pymongo.operations.IndexModel("_id"),
            pymongo.operations.IndexModel("user_id"),
            pymongo.operations.IndexModel("source"),
        ]
