"""
Semantic API route controller
"""
from __future__ import annotations

from typing import cast

from featurebyte.models.relationship import Parent
from featurebyte.models.semantic import SemanticModel
from featurebyte.routes.common.base import BaseDocumentController, RelationshipMixin
from featurebyte.schema.semantic import SemanticCreate, SemanticList
from featurebyte.service.relationship import SemanticRelationshipService
from featurebyte.service.semantic import SemanticService


class SemanticController(
    BaseDocumentController[SemanticModel, SemanticList],
    RelationshipMixin[SemanticModel, Parent],
):
    """
    Semantic Controller
    """

    paginated_document_class = SemanticList

    def __init__(
        self,
        service: SemanticService,
        semantic_relationship_service: SemanticRelationshipService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
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
        document = await self.service.create_document(data)
        return cast(SemanticModel, document)
