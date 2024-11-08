"""
CredentialService
"""

from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId
from cryptography.fernet import InvalidToken
from redis import Redis

from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.credential import CredentialModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.persistent.base import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.credential import CredentialCreate, CredentialServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.storage import Storage

logger = get_logger(__name__)


class CredentialService(
    BaseDocumentService[CredentialModel, CredentialCreate, CredentialServiceUpdate]
):
    """
    CredentialService class
    """

    document_class = CredentialModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        feature_store_service: FeatureStoreService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.feature_store_service = feature_store_service

    async def construct_get_query_filter(
        self, document_id: ObjectId, use_raw_query_filter: bool = False, **kwargs: Any
    ) -> QueryFilter:
        query_filter = await super().construct_get_query_filter(
            document_id=document_id, use_raw_query_filter=use_raw_query_filter, **kwargs
        )
        # credentials are personal to the user
        if not use_raw_query_filter:
            query_filter["user_id"] = self.user.id
        return query_filter

    async def construct_list_query_filter(
        self,
        query_filter: Optional[QueryFilter] = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> QueryFilter:
        output = await super().construct_list_query_filter(
            query_filter=query_filter,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )
        # credentials are personal to the user
        if not use_raw_query_filter:
            output["user_id"] = self.user.id
        return output

    async def get_credentials(
        self, user_id: Optional[ObjectId], feature_store: FeatureStoreModel
    ) -> Optional[CredentialModel]:
        """
        Find credential

        Parameters
        ----------
        user_id: ObjectId | None
            User ID
        feature_store: FeatureStoreModel
            Feature store

        Returns
        -------
        CredentialModel | None
        """
        credentials = await self.list_documents(
            query_filter={"user_id": user_id, "feature_store_id": feature_store.id}
        )
        if len(credentials) == 0:
            return None

        # Choose the credentials
        # TODO: Implement a better way to choose the credentials
        #       Currently choosing the first one
        #       Which is sorted by created_at, DESC
        chosen_credentials = credentials[0]
        chosen_credentials.decrypt_credentials()
        return chosen_credentials

    async def create_document(self, data: CredentialCreate) -> CredentialModel:
        """
        Create document at persistent

        Parameters
        ----------
        data: CredentialCreate
            Credential creation payload object

        Returns
        -------
        CredentialModel
        """
        credential = self.document_class(**data.model_dump(by_alias=True))
        credential.encrypt_credentials()
        return await super().create_document(
            data=CredentialCreate(**credential.model_dump(by_alias=True))
        )

    async def update_document(
        self,
        document_id: ObjectId,
        data: CredentialServiceUpdate,
        exclude_none: bool = True,
        document: Optional[CredentialModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
        populate_remote_attributes: bool = True,
    ) -> Optional[CredentialModel]:
        """
        Update document at persistent

        Parameters
        ----------
        document_id: ObjectId
            Credential ID
        data: CredentialServiceUpdate
            Credential update payload object
        exclude_none: bool
            Whether to exclude None value(s) from the table
        document: Optional[CredentialModel]
            Credential to be updated (when provided, this method won't query persistent for retrieval)
        return_document: bool
            Whether to make additional query to retrieval updated document & return
        skip_block_modification_check: bool
            Whether to skip block modification check (used only when updating description)
        populate_remote_attributes: bool
            Whether to populate remote attributes

        Returns
        -------
        Optional[Document]
        """
        if document is None:
            document = await self.get_document(
                document_id=document_id, populate_remote_attributes=False
            )

        # ensure document is decrypted
        try:
            document.decrypt_credentials()
        except InvalidToken:
            logger.warning("Credential is already decrypted")

        data.encrypt()
        return await super().update_document(
            document_id=document_id,
            data=data,
            exclude_none=exclude_none,
            document=document,
            return_document=return_document,
            skip_block_modification_check=skip_block_modification_check,
            populate_remote_attributes=populate_remote_attributes,
        )
