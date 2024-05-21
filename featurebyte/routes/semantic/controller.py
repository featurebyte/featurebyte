"""
Semantic API route controller
"""

from __future__ import annotations

from typing import Any, List, Tuple

from bson import ObjectId

from featurebyte.models.persistent import QueryFilter
from featurebyte.models.relationship import Parent
from featurebyte.models.semantic import SemanticModel
from featurebyte.routes.common.base import BaseDocumentController, RelationshipMixin
from featurebyte.schema.semantic import SemanticList
from featurebyte.service.relationship import SemanticRelationshipService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table import AllTableService


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
        all_table_service: AllTableService,
    ):
        super().__init__(semantic_service)
        self.relationship_service = semantic_relationship_service
        self.all_table_service = all_table_service

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return [
            (self.all_table_service, {"columns_info.semantic_id": document_id}),
        ]
