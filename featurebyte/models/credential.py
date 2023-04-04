"""
Document model for stored credentials
"""
from typing import List, Literal, Optional, Union
from typing_extensions import Annotated

from pydantic import Field, StrictStr

from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class CredentialType(StrEnum):
    """
    Credential Type
    """

    USERNAME_PASSWORD = "USERNAME_PASSWORD"
    ACCESS_TOKEN = "ACCESS_TOKEN"


class StorageCredentialType(StrEnum):
    """
    Storage Credential Type
    """

    S3 = "S3"


class BaseStorageCredential(FeatureByteBaseModel):
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


class BaseCredential(FeatureByteBaseModel):
    """
    Storage credential only
    """

    credential_type: CredentialType


class UsernamePasswordCredential(BaseCredential):
    """
    Username / Password credential
    """

    credential_type: Literal[CredentialType.USERNAME_PASSWORD] = Field(
        CredentialType.USERNAME_PASSWORD, const=True
    )
    username: StrictStr
    password: StrictStr


class AccessTokenCredential(BaseCredential):
    """
    Access token credential
    """

    credential_type: Literal[CredentialType.ACCESS_TOKEN] = Field(
        CredentialType.ACCESS_TOKEN, const=True
    )
    access_token: StrictStr


SessionCredential = Annotated[
    Union[UsernamePasswordCredential, AccessTokenCredential],
    Field(discriminator="credential_type"),
]


class CredentialModel(FeatureByteBaseDocumentModel):
    """
    Credential model
    """

    feature_store_id: PydanticObjectId
    database_credential: Optional[SessionCredential]
    storage_credential: Optional[StorageCredential]

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
