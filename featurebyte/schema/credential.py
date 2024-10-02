"""
Pydantic schemas for handling API payloads for credential routes
"""

from typing import List, Optional

from bson import ObjectId
from pydantic import Field, field_validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    NameStr,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.credential import (
    BaseCredential,
    CredentialModel,
    DatabaseCredential,
    StorageCredential,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class CredentialCreate(FeatureByteBaseModel):
    """
    Schema for credential creation
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    group_ids: List[PydanticObjectId] = Field(default_factory=list)
    name: Optional[NameStr] = Field(default=None)
    feature_store_id: PydanticObjectId
    database_credential: Optional[DatabaseCredential] = Field(default=None)
    storage_credential: Optional[StorageCredential] = Field(default=None)


class CredentialRead(FeatureByteBaseDocumentModel):
    """
    Schema for credential read
    """

    feature_store_id: PydanticObjectId
    group_ids: List[PydanticObjectId] = Field(default_factory=list)
    database_credential: Optional[DatabaseCredential] = Field(default=None)
    storage_credential: Optional[StorageCredential] = Field(default=None)

    @field_validator("database_credential", "storage_credential")
    @classmethod
    def hide_credentials(cls, credential: Optional[BaseCredential]) -> Optional[BaseCredential]:
        """
        Hide credentials in the details field

        Parameters
        ----------
        credential: Optional[BaseCredential]
            credential details

        Returns
        -------
        BaseCredential
        """
        if credential:
            credential.hide_values()
        return credential

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = CredentialModel.collection_name()


class CredentialList(PaginationMixin):
    """
    Schema for credentials listing
    """

    data: List[CredentialRead]


class CredentialUpdate(FeatureByteBaseModel):
    """
    Schema for credential update
    """

    group_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    database_credential: Optional[DatabaseCredential] = Field(default=None)
    storage_credential: Optional[StorageCredential] = Field(default=None)


class CredentialServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Credential service update schema
    """

    group_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    database_credential: Optional[DatabaseCredential] = Field(default=None)
    storage_credential: Optional[StorageCredential] = Field(default=None)

    def encrypt(self) -> None:
        """
        Encrypt credentials
        """
        if self.database_credential:
            self.database_credential.encrypt_values()
        if self.storage_credential:
            self.storage_credential.encrypt_values()

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique constraints checking
        """

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
