"""
Base namespace service
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from dataclasses import dataclass

from bson import ObjectId
from redis import Redis

from featurebyte.common.model_util import get_version
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.entity import EntityModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity_relationship_extractor import EntityRelationshipExtractorService
from featurebyte.service.mixin import Document, DocumentCreateSchema
from featurebyte.storage import Storage


@dataclass
class FeatureOrTargetDerivedData:
    """Feature or Target data"""

    primary_entity_ids: List[ObjectId]
    entity_id_to_entity: Dict[ObjectId, EntityModel]
    relationships_info: List[EntityRelationshipInfo]


class BaseFeatureService(
    BaseDocumentService[Document, DocumentCreateSchema, BaseDocumentServiceUpdateSchema]
):
    """
    Base namespace service
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.entity_relationship_extractor_service = entity_relationship_extractor_service
        self.derive_primary_entity_helper = derive_primary_entity_helper

    async def get_document_version(self, name: str) -> VersionIdentifier:
        """
        Get the version of the document.

        Parameters
        ----------
        name: str
            Name of the document

        Returns
        -------
        VersionIdentifier
        """
        version_name = get_version()
        query_result = await self.list_documents_as_dict(
            query_filter={"name": name, "version.name": version_name}
        )
        count = query_result["total"]
        return VersionIdentifier(name=version_name, suffix=count or None)

    async def extract_derived_data(
        self, graph: QueryGraphModel, node_name: str
    ) -> FeatureOrTargetDerivedData:
        """
        Extract derived data from a graph and node name

        Parameters
        ----------
        graph: QueryGraphModel
            Query graph
        node_name: str
            Node name

        Returns
        -------
        FeatureOrTargetDerivedData
        """
        query_graph = QueryGraph(**graph.dict(by_alias=True))
        entity_ids = query_graph.get_entity_ids(node_name=node_name)
        extractor = self.entity_relationship_extractor_service
        entity_id_to_entity = await extractor.get_entity_id_to_entity(entity_ids=entity_ids)
        primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
            entity_ids=entity_ids, entity_id_to_entity=entity_id_to_entity
        )
        relationships_info = await extractor.extract_relationship_from_primary_entity(
            entity_ids=entity_ids, primary_entity_ids=primary_entity_ids
        )
        return FeatureOrTargetDerivedData(
            primary_entity_ids=primary_entity_ids,
            entity_id_to_entity=entity_id_to_entity,
            relationships_info=relationships_info,
        )
