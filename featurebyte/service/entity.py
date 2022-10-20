"""
EntityService class
"""
from __future__ import annotations

from typing import Any

from featurebyte.models.base import UniqueConstraintResolutionSignature
from featurebyte.models.entity import EntityModel
from featurebyte.schema.entity import EntityCreate, EntityServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class EntityService(BaseDocumentService[EntityModel, EntityCreate, EntityServiceUpdate]):
    """
    EntityService class
    """

    document_class = EntityModel

    @staticmethod
    def _extract_additional_creation_kwargs(data: EntityCreate) -> dict[str, Any]:
        return {"serving_names": [data.serving_name]}

    async def _check_document_unique_constraint_when_update_document(
        self, data: EntityServiceUpdate
    ) -> None:
        await self._check_document_unique_constraint(
            query_filter={"name": data.name},
            conflict_signature={"name": data.name},
            resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
        )
