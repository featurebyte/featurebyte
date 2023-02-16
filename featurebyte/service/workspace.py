"""
WorkspaceService class
"""
from __future__ import annotations

from featurebyte.models.workspace import WorkspaceModel
from featurebyte.schema.workspace import WorkspaceCreate, WorkspaceServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class WorkspaceService(
    BaseDocumentService[WorkspaceModel, WorkspaceCreate, WorkspaceServiceUpdate]
):
    """
    WorkspaceService class
    """

    document_class = WorkspaceModel
