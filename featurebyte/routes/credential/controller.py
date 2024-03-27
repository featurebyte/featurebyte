"""
Credential API routes
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.credential import CredentialModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.credential import (
    CredentialList,
    CredentialRead,
    CredentialServiceUpdate,
    CredentialUpdate,
)
from featurebyte.schema.info import CredentialInfo
from featurebyte.service.credential import CredentialService
from featurebyte.service.feature_store import FeatureStoreService


class CredentialController(
    BaseDocumentController[CredentialModel, CredentialService, CredentialList],
):
    """
    Credential controller
    """

    paginated_document_class = CredentialList

    def __init__(
        self, credential_service: CredentialService, feature_store_service: FeatureStoreService
    ):
        super().__init__(credential_service)
        self.feature_store_service = feature_store_service

    async def update_credential(
        self,
        credential_id: PydanticObjectId,
        data: CredentialUpdate,
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
        document = await self.service.update_document(
            document_id=credential_id,
            data=CredentialServiceUpdate(**data.dict(by_alias=True)),
        )
        assert document is not None
        return CredentialRead(**document.dict(by_alias=True))

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
        credential = await self.service.get_document(document_id=credential_id)
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
