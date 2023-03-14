"""
Workspace API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.workspace import WorkspaceModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.info import WorkspaceInfo
from featurebyte.schema.workspace import (
    WorkspaceCreate,
    WorkspaceList,
    WorkspaceServiceUpdate,
    WorkspaceUpdate,
)
from featurebyte.service.info import InfoService
from featurebyte.service.workspace import WorkspaceService


class WorkspaceController(
    BaseDocumentController[WorkspaceModel, WorkspaceService, WorkspaceList],
):
    """
    Workspace Controller
    """

    paginated_document_class = WorkspaceList

    def __init__(
        self,
        service: WorkspaceService,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.info_service = info_service

    async def create_workspace(
        self,
        data: WorkspaceCreate,
    ) -> WorkspaceModel:
        """
        Create Workspace at persistent

        Parameters
        ----------
        data: WorkspaceCreate
            Workspace creation payload

        Returns
        -------
        WorkspaceModel
            Newly created workspace object
        """
        return await self.service.create_document(data)

    async def update_workspace(
        self,
        workspace_id: ObjectId,
        data: WorkspaceUpdate,
    ) -> WorkspaceModel:
        """
        Update Workspace stored at persistent

        Parameters
        ----------
        workspace_id: ObjectId
            Workspace ID
        data: WorkspaceUpdate
            Workspace update payload

        Returns
        -------
        WorkspaceModel
            Workspace object with updated attribute(s)
        """
        await self.service.update_document(
            document_id=workspace_id,
            data=WorkspaceServiceUpdate(**data.dict()),
            return_document=False,
        )
        return await self.get(document_id=workspace_id)

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> WorkspaceInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        info_document = await self.info_service.get_workspace_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
