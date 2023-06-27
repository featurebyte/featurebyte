"""
Semantic API route controller
"""
from __future__ import annotations

from featurebyte.models.relationship import Parent
from featurebyte.models.semantic import SemanticModel
from featurebyte.routes.common.base import BaseDocumentController, RelationshipMixin
from featurebyte.schema.semantic import SemanticCreate, SemanticList
from featurebyte.service.relationship import SemanticRelationshipService
from featurebyte.service.semantic import SemanticService


class SemanticController(
    BaseDocumentController[SemanticModel, SemanticService, SemanticList],
    RelationshipMixin[SemanticModel, Parent],
):
    """
    Semantic Controller
    """

    paginated_document_class = SemanticList

    def __init__(
        self,
        semantic_service: SemanticService,
        semantic_relationship_service: SemanticRelationshipService,
    ):
        super().__init__(semantic_service)
        self.relationship_service = semantic_relationship_service

    async def create_semantic(
        self,
        data: SemanticCreate,
    ) -> SemanticModel:
        """
        Create Semantic at persistent

        Parameters
        ----------
        data: SemanticCreate
            Semantic creation payload

        Returns
        -------
        SemanticModel
            Newly created semantic object
        """
        return await self.service.create_document(data)
