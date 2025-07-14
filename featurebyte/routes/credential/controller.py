"""
Credential API routes
"""

from __future__ import annotations

from bson import ObjectId
from fastapi import Request

from featurebyte.exception import DocumentDeletionError
from featurebyte.models import FeatureStoreModel
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.credential import CredentialModel
from featurebyte.routes.common.base import BaseDocumentController
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
        self, credential_id: PydanticObjectId, data: CredentialUpdate
    ) -> CredentialRead:
        """
        Update credential

        Parameters
        ----------
        credential_id: PydanticObjectId
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

    async def delete_credential(self, credential_id: PydanticObjectId) -> None:
        """
        Delete credential

        Parameters
        ----------
        credential_id: PydanticObjectId
            Credential ID

        Raises
        ------
        DocumentDeletionError
            If the last credential for a feature store is being deleted
        """
        document_id = ObjectId(credential_id)
        credential = await self.service.get_document(document_id=document_id)
        feature_store_id = credential.feature_store_id
        other_credentials = await self.service.list_documents_as_dict(
            query_filter={
                "feature_store_id": feature_store_id,
                "_id": {"$ne": credential.id},
            },
            page_size=1,
        )
        if other_credentials["data"]:
            await self.delete(document_id=document_id)
            return

        # Allow deletion if the featurestore is currently deleted
        # NOTE: This will be converted to a is_deleted flag in the future
        #     : This is to ensure that a proper cleanup of featurestore resources is completed
        #     : before allowing deletion of the last credential
        # We are looking at calling all underlying catalogs, tagged to the featurestore to be marked as is_deleted
        # Cleaning them up before allowing the deletion of the last credential & last feature_store
        feature_stores = await self.feature_store_service.list_documents(
            query_filter={"_id": feature_store_id}
        )
        if len(feature_stores) == 0:
            await self.delete(document_id=document_id)
            return

        # Allow deletion of credential if the user is not the feature store owner
        # User's credentials are not used to perform background/cleaning ops
        feature_store = feature_stores[0]
        if feature_store.user_id != credential.user_id:
            await self.delete(document_id=document_id)
            return

        # This is the last credential for the feature store
        raise DocumentDeletionError(
            f"Cannot delete the last credential for feature store (ID: {feature_store_id}). "
            "Please create a new credential before deleting this one to ensure continued access."
        )

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
