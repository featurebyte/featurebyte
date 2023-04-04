"""
Pydantic schemas for handling API payloads for credential routes
"""
from typing import List, Optional

from featurebyte.models.base import (
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)

# pylint: disable=too-many-ancestors
from featurebyte.models.credential import CredentialModel, SessionCredential, StorageCredential
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class CredentialList(PaginationMixin):
    """
    Schema for credentials listing
    """

    data: List[CredentialModel]


class CredentialUpdate(FeatureByteBaseModel):
    """
    Schema for credential update
    """

    database_credential: Optional[SessionCredential]
    storage_credential: Optional[StorageCredential]


class CredentialServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Credential service update schema
    """

    database_credential: Optional[SessionCredential]
    storage_credential: Optional[StorageCredential]

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
