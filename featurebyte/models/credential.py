"""
Document model for stored credentials
"""
# pylint: disable=too-few-public-methods
from typing import Union

from enum import Enum

import pymongo
from pydantic import BaseModel

from featurebyte.models.event_data import DatabaseSourceModel


class CredentialType(str, Enum):
    """
    Credential Type
    """

    USERNAME_PASSWORD = "USERNAME_PASSWORD"


class UsernamePasswordCredential(BaseModel):
    """
    Username / Password credential
    """

    username: str
    password: str


CREDENTIAL_CLASS = {CredentialType.USERNAME_PASSWORD: UsernamePasswordCredential}


class Credential(BaseModel):
    """
    Credential model
    """

    name: str
    source: DatabaseSourceModel
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
