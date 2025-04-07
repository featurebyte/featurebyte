"""
Document model for stored credentials
"""

import base64
import json
import os
from typing import Any, Callable, ClassVar, Dict, List, Literal, Optional, Union

import pymongo
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pydantic import BaseModel, Field, Strict, StrictStr, model_validator
from typing_extensions import Annotated

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
        for field_name, field in self.model_fields.items():
            if field.annotation is str and field.metadata == [Strict(strict=True)]:
                setattr(self, field_name, func(getattr(self, field_name)))
            else:
                # pydantic captures dict field type as str
                field_value = getattr(self, field_name)
                if isinstance(field_value, dict):
                    # Encrypt each value in the dict
                    setattr(
                        self,
                        field_name,
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
    OAUTH = "OAUTH"
    PRIVATE_KEY = "PRIVATE_KEY"
    KERBEROS_KEYTAB = "KERBEROS_KEYTAB"
    GOOGLE = "GOOGLE"


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
    ...     username="username",
    ...     password="password",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.UsernamePasswordCredential"
    )

    # instance variables
    type: Literal[DatabaseCredentialType.USERNAME_PASSWORD] = (
        DatabaseCredentialType.USERNAME_PASSWORD
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
    type: Literal[DatabaseCredentialType.ACCESS_TOKEN] = DatabaseCredentialType.ACCESS_TOKEN
    access_token: StrictStr = Field(description="The access token used to connect.")


class OAuthCredential(BaseDatabaseCredential):
    """
    Data class for an OAuth credential.

    Examples
    --------
    >>> oauth_token_credential = OAuthCredential(client_id="client_id", client_secret="client_id")
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.OAuthCredential")

    # instance variables
    type: Literal[DatabaseCredentialType.OAUTH] = DatabaseCredentialType.OAUTH
    client_id: StrictStr = Field(description="The client ID used to connect.")
    client_secret: StrictStr = Field(description="The client secret used to connect.")


class PrivateKeyCredential(BaseDatabaseCredential):
    """
    Data class for a private key credential.

    Examples
    --------
    >>> private_key_credential = PrivateKeyCredential.from_file(
    ...     username="user", key_filepath="rsa_key.p8", passphrase="password"
    ... )  # doctest: +SKIP
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.PrivateKeyCredential")

    # instance variables
    type: Literal[DatabaseCredentialType.PRIVATE_KEY] = DatabaseCredentialType.PRIVATE_KEY

    username: StrictStr = Field(description="Username to use with the private key.")
    private_key: StrictStr = Field(description="The private key used to connect in PEM format.")
    passphrase: Optional[StrictStr] = Field(
        default=None,
        description="The passphrase used to encrypt the private key. If the private key is not encrypted, this field should be None.",
    )

    @property
    def pem_private_key(self) -> Any:
        return serialization.load_pem_private_key(
            self.private_key.encode("ascii"),
            password=self.passphrase.encode("ascii") if self.passphrase else None,
            backend=default_backend(),
        )

    @model_validator(mode="before")
    @classmethod
    def _format_private_key(self, values: Any) -> Any:
        # replace newline ascii characters with actual newline characters
        values["private_key"] = values["private_key"].replace("\\n", "\n")
        # replace space before / after header / footer with newline characters
        values["private_key"] = (
            values["private_key"].replace(" -----", "\n-----").replace("----- ", "-----\n")
        )
        return values

    @model_validator(mode="after")
    def _validate_key(self) -> "PrivateKeyCredential":
        # check passphrase if private key is in decrypted PEM format
        if self.private_key.startswith("-----BEGIN"):
            try:
                _ = self.pem_private_key
            except TypeError:
                raise ValueError("Password was not given but private key is encrypted.")
            except ValueError:
                raise ValueError("Passphrase provided is incorrect for the private key.")

        return self

    @classmethod
    def from_file(
        cls, username: str, key_filepath: str, passphrase: Optional[str] = None
    ) -> "PrivateKeyCredential":
        """
        Create a PrivateKeyCredential from a private key file.

        Parameters
        ----------
        username: str
            Username to use with the private key.
        key_filepath: str
            Path to the private key file.
        passphrase: Optional[str]
            Passphrase to use with the private key. If the private key is not encrypted, this should be None.

        Returns
        -------
        PrivateKeyCredential
        """
        with open(key_filepath, "r") as key_file:
            private_key = key_file.read()

        return PrivateKeyCredential(
            username=username, private_key=private_key, passphrase=passphrase
        )


class KerberosKeytabCredential(BaseDatabaseCredential):
    """
    Data class for a kerberos key tab credential.

    Examples
    --------
    >>> kerberos_key_tab_credential = KerberosKeytabCredential.from_file(
    ...     keytab="/path/to/keytab", principal="user@FEATUREBYTE.COM"
    ... )  # doctest: +SKIP
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.KerberosKeytabCredential"
    )

    # instance variables
    type: Literal[DatabaseCredentialType.KERBEROS_KEYTAB] = DatabaseCredentialType.KERBEROS_KEYTAB
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


class ServiceAccountCredentialMixin(FeatureByteBaseModel):
    """
    Mixin for google credential.
    """

    service_account_info: Union[Dict[str, str], StrictStr] = Field(
        description="Service account information used for connecting to your GCS store."
    )

    @model_validator(mode="before")
    @classmethod
    def _validate_service_account_info(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        service_account_info = values.get("service_account_info")
        if isinstance(service_account_info, str):
            values["service_account_info"] = json.loads(service_account_info)
        return values


class GoogleCredential(BaseDatabaseCredential, ServiceAccountCredentialMixin):
    """
    Data class for a Google service account credential.

    Examples
    --------
    >>> google_credential = GoogleCredential(
    ...     service_account_info={"type": "service_account", "private_key": "private_key"}
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.GoogleCredential")

    # instance variables
    type: Literal[DatabaseCredentialType.GOOGLE] = DatabaseCredentialType.GOOGLE


DatabaseCredential = Annotated[
    Union[
        UsernamePasswordCredential,
        AccessTokenCredential,
        OAuthCredential,
        PrivateKeyCredential,
        KerberosKeytabCredential,
        GoogleCredential,
    ],
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
    ...     s3_access_key_id="access_key_id", s3_secret_access_key="s3_secret_access_key"
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.S3StorageCredential")

    # instance variables
    type: Literal[StorageCredentialType.S3] = StorageCredentialType.S3
    s3_access_key_id: StrictStr = Field(
        description="S3 access key ID used for connecting to your S3 store."
    )
    s3_secret_access_key: StrictStr = Field(
        description="S3 secret access key used for connecting to your S3 store."
        "Avoid storing this in plain text, or in a public repository."
    )


class GCSStorageCredential(BaseStorageCredential, ServiceAccountCredentialMixin):
    """
    Data class for a GCS storage credential.

    Examples
    --------
    >>> gcs_storage_credential = GCSStorageCredential(
    ...     service_account_info={"type": "service_account", "private_key": "private_key"}
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.GCSStorageCredential")

    # instance variables
    type: Literal[StorageCredentialType.GCS] = StorageCredentialType.GCS
    service_account_info: Union[Dict[str, str], StrictStr] = Field(
        description="Service account information used for connecting to your GCS store."
    )


class AzureBlobStorageCredential(BaseStorageCredential):
    """
    Data class for a Azure Blob storage credential.

    Examples
    --------
    >>> azure_blob_storage_credential = AzureBlobStorageCredential(
    ...     account_name="my_azure_storage",
    ...     account_key="my_azure_storage_key",
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.AzureBlobStorageCredential"
    )

    # instance variables
    type: Literal[StorageCredentialType.AZURE] = StorageCredentialType.AZURE
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
    group_ids: List[PydanticObjectId] = Field(default=[])
    database_credential: Optional[DatabaseCredential] = Field(default=None)
    storage_credential: Optional[StorageCredential] = Field(default=None)

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
        ]

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
