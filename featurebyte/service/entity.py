"""
EntityService class
"""
from __future__ import annotations

from typing import Any

from featurebyte.models.entity import EntityModel
from featurebyte.routes.app_container import register_service_constructor
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


register_service_constructor(EntityService)
