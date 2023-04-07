"""
Document model for stored credentials
"""
from typing import List, Literal, Optional, Union
from typing_extensions import Annotated

import base64
import os

from cryptography.fernet import Fernet
from pydantic import BaseModel, Field, StrictStr

from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)

PASSWORD_SECRET = os.environ.get("CONFIG_PASSWORD_SECRET", "T3gUGT(Q)eSSsX+@xZQf&r5vwJ%zQsfr")


def encrypt_value(value: str) -> str:
    """
    Encrypt value
    """
    cipher_suite = Fernet(base64.b64encode(PASSWORD_SECRET.encode("utf-8")))
    return cipher_suite.encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt value
    """
    cipher_suite = Fernet(base64.b64encode(PASSWORD_SECRET.encode("utf-8")))
    return cipher_suite.decrypt(encrypted_value.encode("utf-8")).decode("utf-8")


class BaseCredential(FeatureByteBaseModel):
    """
    Base Credential class
    """

    def encrypt(self) -> None:
        """
        Encrypt credentials
        """
        for field in self.__fields__.values():
            if field.type_ == StrictStr:
                setattr(self, field.name, encrypt_value(getattr(self, field.name)))

    def decrypt(self) -> None:
        """
        Decrypt credentials
        """
        for field in self.__fields__.values():
            if field.type_ == StrictStr:
                setattr(self, field.name, decrypt_value(getattr(self, field.name)))


# Database Credentials
class DatabaseCredentialType(StrEnum):
    """
    Credential Type
    """

    USERNAME_PASSWORD = "USERNAME_PASSWORD"
    ACCESS_TOKEN = "ACCESS_TOKEN"


class BaseDatabaseCredential(BaseCredential):
    """
    Storage credential only
    """

    credential_type: DatabaseCredentialType


class UsernamePasswordCredential(BaseDatabaseCredential):
    """
    Username / Password credential
    """

    credential_type: Literal[DatabaseCredentialType.USERNAME_PASSWORD] = Field(
        DatabaseCredentialType.USERNAME_PASSWORD, const=True
    )
    username: StrictStr
    password: StrictStr


class AccessTokenCredential(BaseDatabaseCredential):
    """
    Access token credential
    """

    credential_type: Literal[DatabaseCredentialType.ACCESS_TOKEN] = Field(
        DatabaseCredentialType.ACCESS_TOKEN, const=True
    )
    access_token: StrictStr


DatabaseCredential = Annotated[
    Union[UsernamePasswordCredential, AccessTokenCredential],
    Field(discriminator="credential_type"),
]


# Storage Credentials
class StorageCredentialType(StrEnum):
    """
    Storage Credential Type
    """

    S3 = "S3"


class BaseStorageCredential(BaseCredential):
    """
    Base storage credential
    """

    credential_type: StorageCredentialType


class S3StorageCredential(BaseStorageCredential):
    """
    S3 storage credential
    """

    credential_type: StorageCredentialType = Field(StorageCredentialType.S3, const=True)
    s3_access_key_id: StrictStr
    s3_secret_access_key: StrictStr


StorageCredential = Annotated[
    Union[S3StorageCredential],
    Field(discriminator="credential_type"),
]


class CredentialModel(FeatureByteBaseDocumentModel):
    """
    Credential model
    """

    feature_store_id: PydanticObjectId
    database_credential: Optional[DatabaseCredential]
    storage_credential: Optional[StorageCredential]

    def encrypt(self) -> None:
        """
        Encrypt credentials
        """
        if self.database_credential:
            self.database_credential.encrypt()
        if self.storage_credential:
            self.storage_credential.encrypt()

    def decrypt(self) -> None:
        """
        Decrypt credentials
        """
        if self.database_credential:
            self.database_credential.decrypt()
        if self.storage_credential:
            self.storage_credential.decrypt()

    class Settings:
        """
        Collection settings for Credential document
        """

        collection_name = "credential"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("user_id", "feature_store_id"),
                conflict_fields_signature={"feature_store_id": ["feature_store_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]
