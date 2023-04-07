"""
CredentialService
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.models.credential import CredentialModel
from featurebyte.persistent.base import Persistent
from featurebyte.schema.credential import CredentialServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService


class CredentialService(
    BaseDocumentService[CredentialModel, CredentialModel, CredentialServiceUpdate]
):
    """
    CredentialService class
    """

    document_class = CredentialModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
    ):
        super().__init__(user=user, persistent=persistent, catalog_id=catalog_id)
        self.feature_store_service = feature_store_service
        self.feature_store_warehouse_service = feature_store_warehouse_service

    async def _validate_credential(self, credential: CredentialModel) -> None:
        """
        Validate credential is valid

        Parameters
        ----------
        credential: CredentialModel
            CredentialModel to validate
        """
        # test credential works
        feature_store_service = FeatureStoreService(
            user=self.user, persistent=self.persistent, catalog_id=self.catalog_id
        )
        feature_store = await feature_store_service.get_document(
            document_id=credential.feature_store_id
        )

        async def get_credential(**kwargs: Any) -> CredentialModel:
            """
            Get credential to test with feature store

            Parameters
            ----------
            kwargs: Any
                Keyword arguments

            Returns
            -------
            CredentialModel
            """
            _ = kwargs
            return credential

        await self.feature_store_warehouse_service.list_databases(
            feature_store=feature_store,
            get_credential=get_credential,
        )

    async def create_document(self, data: CredentialModel) -> CredentialModel:
        """
        Create document at persistent

        Parameters
        ----------
        data: CredentialModel
            Credential creation payload object

        Returns
        -------
        CredentialModel
        """
        credential = self.document_class(**data.json_dict())
        await self._validate_credential(credential=credential)
        credential.encrypt()
        return await super().create_document(data=credential)

    async def update_document(
        self,
        document_id: ObjectId,
        data: CredentialServiceUpdate,
        exclude_none: bool = True,
        document: Optional[CredentialModel] = None,
        return_document: bool = True,
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

        Returns
        -------
        Optional[Document]
        """
        if document is None:
            document = await self.get_document(document_id=document_id)
            document.decrypt()

        # verify credential is valid
        update_dict = data.dict(exclude_none=exclude_none)
        updated_document = self.document_class(**{**document.json_dict(), **update_dict})
        await self._validate_credential(credential=updated_document)
        data.encrypt()

        return await super().update_document(
            document_id=document_id,
            data=data,
            exclude_none=exclude_none,
            document=document,
            return_document=return_document,
        )
