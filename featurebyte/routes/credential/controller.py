"""
Credential API routes
"""

from __future__ import annotations

from bson import ObjectId
from fastapi import Request

from featurebyte.models import FeatureStoreModel
from featurebyte.models.credential import CredentialModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.common.schema import PyObjectId
from featurebyte.schema.credential import (
    CredentialCreate,
    CredentialList,
    CredentialRead,
    CredentialServiceUpdate,
    CredentialUpdate,
)
from featurebyte.schema.info import CredentialInfo
from featurebyte.service.credential import CredentialService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService


class CredentialController(
    BaseDocumentController[CredentialModel, CredentialService, CredentialList],
):
    """
    Credential controller
    """

    paginated_document_class = CredentialList

    def __init__(
        self,
        credential_service: CredentialService,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
    ):
        super().__init__(credential_service)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service

    async def _validate_credentials(
        self,
        feature_store: FeatureStoreModel,
        credentials: CredentialModel,
    ) -> None:
        """
        Validate credentials

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        credentials: CredentialModel
            CredentialModel object
        """
        session = await self.session_manager_service.get_feature_store_session(
            feature_store, credentials_override=credentials
        )
        # Raise Exception if the session is not valid
        await session.list_databases()

    async def create_credential(self, _: Request, data: CredentialCreate) -> CredentialRead:
        """
        Create credential

        Parameters
        ----------
        _: Request
            Request object
        data: CredentialCreate
            CredentialCreate object

        Returns
        -------
        CredentialRead
            CredentialRead object or None if failed to validate credentials
        """
        # Test Credential Validity
        feature_store = await self.feature_store_service.get_document(
            document_id=ObjectId(data.feature_store_id)
        )
        credentials = CredentialModel.model_validate(data.model_dump(by_alias=True))

        await self._validate_credentials(
            feature_store, credentials
        )  # Will raise exception if invalid

        # Create Credential
        saved_credentials = await self.service.create_document(data=data)
        return CredentialRead(**saved_credentials.model_dump(by_alias=True))

    async def update_credential(
        self, credential_id: PyObjectId, data: CredentialUpdate
    ) -> CredentialRead:
        """
        Update credential

        Parameters
        ----------
        credential_id: PyObjectId
            credential id
        data: CredentialUpdate
            CredentialUpdate object

        Returns
        -------
        CredentialRead
            CredentialRead object
        """
        # Temporarily update the credentials
        credentials = await self.service.get_document(document_id=ObjectId(credential_id))
        for key, value in data.model_dump().items():
            if value is not None:
                setattr(credentials, key, value)

        # Validate that the credentials are valid
        feature_store = await self.feature_store_service.get_document(
            document_id=ObjectId(credentials.feature_store_id)
        )
        await self._validate_credentials(feature_store, credentials)

        # Update the credentials
        document = await self.service.update_document(
            document_id=ObjectId(credential_id),
            data=CredentialServiceUpdate(**data.model_dump(by_alias=True)),
        )
        assert document is not None
        return CredentialRead(**document.model_dump(by_alias=True))

    async def get_info(
        self,
        credential_id: ObjectId,
        verbose: bool,
    ) -> CredentialInfo:
        """
        Get credential info given credential ID

        Parameters
        ----------
        credential_id: ObjectId
            Credential ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        CredentialInfo
        """
        _ = verbose
        credential = await self.service.get_document(document_id=ObjectId(credential_id))
        return CredentialInfo(
            name=credential.name,
            feature_store_info=await self.feature_store_service.get_feature_store_info(
                document_id=credential.feature_store_id, verbose=verbose
            ),
            database_credential_type=(
                credential.database_credential.type if credential.database_credential else None
            ),
            storage_credential_type=(
                credential.storage_credential.type if credential.storage_credential else None
            ),
            created_at=credential.created_at,
            updated_at=credential.updated_at,
            description=credential.description,
        )
