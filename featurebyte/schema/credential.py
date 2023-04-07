"""
Pydantic schemas for handling API payloads for credential routes
"""
from typing import Any, Dict, List, Optional

from pydantic import root_validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)

# pylint: disable=too-many-ancestors
from featurebyte.models.credential import (
    DatabaseCredential,
    DatabaseCredentialType,
    StorageCredential,
    StorageCredentialType,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class CredentialRead(FeatureByteBaseDocumentModel):
    """
    Schema for credential read
    """

    feature_store_id: PydanticObjectId
    database_credential_type: Optional[DatabaseCredentialType]
    storage_credential_type: Optional[StorageCredentialType]

    @root_validator(pre=True)
    @classmethod
    def convert_credentials(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert credentials to the correct type
        """
        if values.get("database_credential"):
            values["database_credential_type"] = values["database_credential"]["credential_type"]
        if values.get("storage_credential"):
            values["storage_credential_type"] = values["storage_credential"]["credential_type"]
        return values


class CredentialList(PaginationMixin):
    """
    Schema for credentials listing
    """

    data: List[CredentialRead]


class CredentialUpdate(FeatureByteBaseModel):
    """
    Schema for credential update
    """

    database_credential: Optional[DatabaseCredential]
    storage_credential: Optional[StorageCredential]


class CredentialServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Credential service update schema
    """

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

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique contraints checking
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
