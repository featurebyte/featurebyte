"""
Credential API routes
"""
from __future__ import annotations

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.credential import CredentialModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.credential import CredentialList, CredentialServiceUpdate, CredentialUpdate
from featurebyte.service.credential import CredentialService


class CredentialController(
    BaseDocumentController[CredentialModel, CredentialService, CredentialList],
):
    """
    Credential controller
    """

    paginated_document_class = CredentialList

    def __init__(
        self,
        service: CredentialService,
    ):
        super().__init__(service)

    async def create_credential(
        self,
        data: CredentialModel,
    ) -> CredentialModel:
        """
        Create credential

        Parameters
        ----------
        data: CredentialModel
            CredentialModel object

        Returns
        -------
        CredentialModel
            Created CredentialModel object
        """
        return await self.service.create_document(data=data)

    async def update_credential(
        self,
        credential_id: PydanticObjectId,
        data: CredentialUpdate,
    ) -> CredentialModel:
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
        CredentialModel
            Retrieved CredentialModel object
        """
        document = await self.service.update_document(
            document_id=credential_id,
            data=CredentialServiceUpdate(**data.dict()),
        )
        assert document is not None
        return document

    async def delete_credential(
        self,
        credential_id: PydanticObjectId,
    ) -> None:
        """
        Delete credential

        Parameters
        ----------
        credential_id: PydanticObjectId
            ID of credential to update
        """
        await self.service.delete_document(document_id=credential_id)
