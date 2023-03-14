"""
Document model for stored credentials
"""
from typing import Any, Dict, Optional

from pydantic import Field, StrictStr, root_validator

from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel


class CredentialType(StrEnum):
    """
    Credential Type
    """

    NONE = "NONE"
    USERNAME_PASSWORD = "USERNAME_PASSWORD"
    ACCESS_TOKEN = "ACCESS_TOKEN"


class StorageCredentialType(StrEnum):
    """
    Storage Credential Type
    """

    NONE = "NONE"
    S3 = "S3"


class BaseStorageCredential(FeatureByteBaseModel):
    """
    Base storage credential
    """


class S3Credential(BaseStorageCredential):
    """
    S3 storage credential
    """

    s3_access_key_id: StrictStr
    s3_secret_access_key: StrictStr


class BaseCredential(FeatureByteBaseModel):
    """
    Storage credential only
    """

    storage_credential_type: Optional[StorageCredentialType] = Field(
        default=StorageCredentialType.NONE
    )
    storage_credential: Optional[BaseStorageCredential]

    @root_validator(pre=True)
    @classmethod
    def _populate_credential(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Populate credential with right class

        Parameters
        ----------
        values: Dict[str, Any]
            values to validate

        Returns
        -------
        Dict[str, Any]
        """
        type_class_mapping = {
            StorageCredentialType.NONE: BaseStorageCredential,
            StorageCredentialType.S3: S3Credential,
        }
        storage_credential = values.get("storage_credential", {})
        if isinstance(storage_credential, FeatureByteBaseModel):
            storage_credential = storage_credential.dict()
        values["storage_credential_type"] = values.get(
            "storage_credential_type", StorageCredentialType.NONE
        )
        values["storage_credential"] = type_class_mapping[values["storage_credential_type"]](
            **storage_credential
        )
        return values


class UsernamePasswordCredential(BaseCredential):
    """
    Username / Password credential
    """

    username: StrictStr
    password: StrictStr


class AccessTokenCredential(BaseCredential):
    """
    Access token credential
    """

    access_token: StrictStr


class Credential(FeatureByteBaseModel):
    """
    Credential model
    """

    name: StrictStr
    credential_type: CredentialType = Field(default=CredentialType.NONE)
    credential: BaseCredential

    @root_validator(pre=True)
    @classmethod
    def _populate_credential(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Populate credential with right class

        Parameters
        ----------
        values: Dict[str, Any]
            values to validate

        Returns
        -------
        Dict[str, Any]
        """
        type_class_mapping = {
            CredentialType.NONE: BaseCredential,
            CredentialType.ACCESS_TOKEN: AccessTokenCredential,
            CredentialType.USERNAME_PASSWORD: UsernamePasswordCredential,
        }
        credential = values.get("credential", {})
        if isinstance(credential, FeatureByteBaseModel):
            credential = credential.dict()
        values["credential"] = type_class_mapping[
            values.get("credential_type", CredentialType.NONE)
        ](**credential)
        return values
