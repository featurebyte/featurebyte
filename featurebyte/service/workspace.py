"""
WorkspaceService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from featurebyte.models.workspace import WorkspaceModel
from featurebyte.schema.workspace import WorkspaceCreate, WorkspaceServiceUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.mixin import SortDir


class WorkspaceService(
    BaseDocumentService[WorkspaceModel, WorkspaceCreate, WorkspaceServiceUpdate]
):
    """
    WorkspaceService class
    """

    document_class = WorkspaceModel

    async def _ensure_default_workspace_available(self) -> None:
        """
        Ensure default workspace exists, and create it if it doesn't
        """
        try:
            await super().get_document(document_id=DEFAULT_WORKSPACE_ID)
        except DocumentNotFoundError:
            await super().create_document(WorkspaceCreate(_id=DEFAULT_WORKSPACE_ID, name="default"))

    async def create_document(self, data: WorkspaceCreate) -> WorkspaceModel:
        await self._ensure_default_workspace_available()
        return await super().create_document(data)

    async def get_document(
        self,
        document_id: ObjectId,
        exception_detail: str | None = None,
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> WorkspaceModel:
        await self._ensure_default_workspace_available()
        return await super().get_document(
            document_id=document_id,
            exception_detail=exception_detail,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )

    async def list_documents(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: SortDir = "desc",
        use_raw_query_filter: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        await self._ensure_default_workspace_available()
        return await super().list_documents(
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
            use_raw_query_filter=use_raw_query_filter,
            **kwargs,
        )
