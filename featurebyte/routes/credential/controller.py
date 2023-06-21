"""
Credential API routes
"""
from __future__ import annotations

from bson import ObjectId

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


class CredentialController(
    BaseDocumentController[CredentialModel, CredentialService, CredentialList],
):
    """
    Credential controller
    """

    paginated_document_class = CredentialList

    async def create_credential(
        self,
        data: CredentialCreate,
    ) -> CredentialRead:
        """
        Create credential

        Parameters
        ----------
        data: CredentialCreate
            CredentialCreate object

        Returns
        -------
        CredentialRead
            CredentialRead object
        """
        document = await self.service.create_document(data=data)
        return CredentialRead(**document.dict(by_alias=True))

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
        info_document = await self.service.get_credential_info(
            document_id=credential_id, verbose=verbose
        )
        return info_document
