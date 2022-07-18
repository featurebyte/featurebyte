"""
Document model for stored credentials
"""
# pylint: disable=too-few-public-methods
from typing import Union

from enum import Enum

import pymongo
from pydantic import BaseModel, StrictStr

from featurebyte.models.feature_store import FeatureStoreModel


class CredentialType(str, Enum):
    """
    Credential Type
    """

    USERNAME_PASSWORD = "USERNAME_PASSWORD"


class UsernamePasswordCredential(BaseModel):
    """
    Username / Password credential
    """

    username: StrictStr
    password: StrictStr


class Credential(BaseModel):
    """
    Credential model
    """

    name: StrictStr
    feature_store: FeatureStoreModel
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

    class Config:
        """
        Configuration for Credential
        """

        use_enum_values = True
