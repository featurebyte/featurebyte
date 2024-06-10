"""
Document model for stored credentials
"""

from typing import Callable, ClassVar, Dict, List, Literal, Optional, Union
from typing_extensions import Annotated

import base64  # pylint: disable=wrong-import-order
import os  # pylint: disable=wrong-import-order

import pymongo
from cryptography.fernet import Fernet
from pydantic import Field, StrictStr

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)

PASSWORD_SECRET = os.environ.get(
    "CONFIG_PASSWORD_SECRET", "VDNnVUdUKFEpZVNTc1grQHhaUWYmcjV2d0olelFzZnI="
)
HIDDEN_VALUE = "********"


def encrypt_value(value: str) -> str:
    """
    Encrypt value

    Parameters
    ----------
    value: str
        Value to encrypt

    Returns
    -------
    str
        Encrypted value
    """
    cipher_suite = Fernet(PASSWORD_SECRET.encode("utf-8"))
    return cipher_suite.encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt value

    Parameters
    ----------
    encrypted_value: str
        Encrypted value

    Returns
    -------
    str
        Decrypted value
    """
    cipher_suite = Fernet(PASSWORD_SECRET.encode("utf-8"))
    return cipher_suite.decrypt(encrypted_value.encode("utf-8")).decode("utf-8")


class BaseCredential(FeatureByteBaseModel):
    """
    Base Credential class
    """

    def _apply_to_values(self, func: Callable[[str], str]) -> None:
        """
        Apply function to all fields

        Parameters
        ----------
        func: Callable[[str], str]
            Function to apply
        """
        for field in self.__fields__.values():
            if field.type_ == StrictStr:
                setattr(self, field.name, func(getattr(self, field.name)))
            elif field.type_ == str:
                # pydantic captures dict field type as str
                field_value = getattr(self, field.name)
                if isinstance(field_value, dict):
                    # Encrypt each value in the dict
                    setattr(
                        self,
                        field.name,
                        {key: func(value) for key, value in field_value.items()},
                    )

    def encrypt_values(self) -> None:
        """
        Encrypt credentials
        """
        self._apply_to_values(encrypt_value)

    def decrypt_values(self) -> None:
        """
        Decrypt credentials
        """
        self._apply_to_values(decrypt_value)

    def hide_values(self) -> None:
        """
        Hide values in the credential
        """
        self._apply_to_values(lambda _: HIDDEN_VALUE)


# Database Credentials
class DatabaseCredentialType(StrEnum):
    """
    Credential Type
    """

    USERNAME_PASSWORD = "USERNAME_PASSWORD"
    ACCESS_TOKEN = "ACCESS_TOKEN"
    KERBEROS_KEYTAB = "KERBEROS_KEYTAB"


class BaseDatabaseCredential(BaseCredential):
    """
    Storage credential only
    """

    type: DatabaseCredentialType


class UsernamePasswordCredential(BaseDatabaseCredential):
    """
    Data class for a username and password credential.

    Examples
    --------
    >>> username_password_credential = UsernamePasswordCredential(
    ...   username="username",
    ...   password="password",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.UsernamePasswordCredential"
    )

    # instance variables
    type: Literal[DatabaseCredentialType.USERNAME_PASSWORD] = Field(
        DatabaseCredentialType.USERNAME_PASSWORD, const=True
    )
    username: StrictStr = Field(description="Username of your account.")
    password: StrictStr = Field(description="Password of your account.")


class AccessTokenCredential(BaseDatabaseCredential):
    """
    Data class for an access token credential.

    Examples
    --------
    >>> access_token_credential = AccessTokenCredential(access_token="access_token")
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.AccessTokenCredential")

    # instance variables
    type: Literal[DatabaseCredentialType.ACCESS_TOKEN] = Field(
        DatabaseCredentialType.ACCESS_TOKEN, const=True
    )
    access_token: StrictStr = Field(description="The access token used to connect.")


class KerberosKeytabCredential(BaseDatabaseCredential):
    """
    Data class for a kerberos key tab credential.

    Examples
    --------
    >>> kerberos_key_tab_credential = KerberosKeytabCredential.from_file(
    ... keytab="/path/to/keytab", principal="user@FEATUREBYTE.COM"
    ... )  # doctest: +SKIP
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.KerberosKeytabCredential"
    )

    # instance variables
    type: Literal[DatabaseCredentialType.KERBEROS_KEYTAB] = Field(
        DatabaseCredentialType.KERBEROS_KEYTAB, const=True
    )
    principal: StrictStr = Field(description="The principal used to connect.")
    encoded_key_tab: StrictStr = Field(description="The key tab used to connect.")

    @property
    def keytab(self) -> bytes:
        """
        Returns the keytab

        Returns
        -------
        bytes
            The key tab
        """
        return base64.b64decode(self.encoded_key_tab)

    @classmethod
    def from_file(cls, keytab_filepath: str, principal: str) -> "KerberosKeytabCredential":
        """
        Create a KerberosKeytabCredential from a keytab file.

        Parameters
        ----------
        keytab_filepath: str
            Path to the keytab file.
        principal: str
            Principal to use with the keytab.

        Returns
        -------
        KerberosKeytabCredential
        """
        return KerberosKeytabCredential(
            principal=principal,
            encoded_key_tab=base64.b64encode(open(keytab_filepath, "rb").read()).decode("utf-8"),
        )


DatabaseCredential = Annotated[
    Union[UsernamePasswordCredential, AccessTokenCredential, KerberosKeytabCredential],
    Field(discriminator="type"),
]


# Storage Credentials
class StorageCredentialType(StrEnum):
    """
    Storage Credential Type
    """

    S3 = "S3"
    GCS = "GCS"
    AZURE = "AZURE"


class BaseStorageCredential(BaseCredential):
    """
    Base storage credential
    """

    type: StorageCredentialType


class S3StorageCredential(BaseStorageCredential):
    """
    Data class for a S3 storage credential.

    Examples
    --------
    >>> s3_storage_credential = S3StorageCredential(
    ...   s3_access_key_id="access_key_id",
    ...   s3_secret_access_key="s3_secret_access_key"
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.S3StorageCredential")

    # instance variables
    type: StorageCredentialType = Field(StorageCredentialType.S3, const=True)
    s3_access_key_id: StrictStr = Field(
        description="S3 access key ID used for connecting to your S3 store."
    )
    s3_secret_access_key: StrictStr = Field(
        description="S3 secret access key used for connecting to your S3 store."
        "Avoid storing this in plain text, or in a public repository."
    )


class GCSStorageCredential(BaseStorageCredential):
    """
    Data class for a GCS storage credential.

    Examples
    --------
    >>> gcs_storage_credential = GCSStorageCredential(
    ...   service_account_info={"type": "service_account", "private_key": "private_key"}
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.GCSStorageCredential")

    # instance variables
    type: StorageCredentialType = Field(StorageCredentialType.GCS, const=True)
    service_account_info: Dict[str, str] = Field(
        description="Service account information used for connecting to your GCS store."
    )


class AzureBlobStorageCredential(BaseStorageCredential):
    """
    Data class for a Azure Blob storage credential.

    Examples
    --------
    >>> azure_blob_storage_credential = AzureBlobStorageCredential(
    ...   account_name="my_azure_storage",
    ...   account_key="my_azure_storage_key",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.AzureBlobStorageCredential"
    )

    # instance variables
    type: StorageCredentialType = Field(StorageCredentialType.AZURE, const=True)
    account_name: StrictStr
    account_key: StrictStr


StorageCredential = Annotated[
    Union[S3StorageCredential, GCSStorageCredential, AzureBlobStorageCredential],
    Field(discriminator="type"),
]


class CredentialModel(FeatureByteBaseDocumentModel):
    """
    Credential model
    """

    feature_store_id: PydanticObjectId
    database_credential: Optional[DatabaseCredential]
    storage_credential: Optional[StorageCredential]

    def encrypt_credentials(self) -> None:
        """
        Encrypt credentials
        """
        if self.database_credential:
            self.database_credential.encrypt_values()
        if self.storage_credential:
            self.storage_credential.encrypt_values()

    def decrypt_credentials(self) -> None:
        """
        Decrypt credentials
        """
        if self.database_credential:
            self.database_credential.decrypt_values()
        if self.storage_credential:
            self.storage_credential.decrypt_values()

    def hide_credentials(self) -> None:
        """
        Hide credentials
        """
        if self.database_credential:
            self.database_credential.hide_values()
        if self.storage_credential:
            self.storage_credential.hide_values()

    class Settings(FeatureByteBaseDocumentModel.Settings):
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

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
