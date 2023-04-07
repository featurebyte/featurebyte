"""
Credential API routes
"""
from __future__ import annotations

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.credential import CredentialModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.credential import (
    CredentialList,
    CredentialRead,
    CredentialServiceUpdate,
    CredentialUpdate,
)
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
    ) -> CredentialRead:
        """
        Create credential

        Parameters
        ----------
        data: CredentialModel
            CredentialModel object

        Returns
        -------
        CredentialRead
            CredentialRead object
        """
        document = await self.service.create_document(data=data)
        return CredentialRead(**document.json_dict())

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
            data=CredentialServiceUpdate(**data.dict()),
        )
        assert document is not None
        return CredentialRead(**document.json_dict())

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
