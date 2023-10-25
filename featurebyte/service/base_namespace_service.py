"""
Base namespace service
"""
from typing import Any, List, Optional

from dataclasses import dataclass

from bson import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature import EntityRelationshipInfo
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity_relationship_extractor import EntityRelationshipExtractorService
from featurebyte.service.mixin import Document, DocumentCreateSchema


@dataclass
class FeatureOrTargetDerivedData:
    """Feature or Target data"""

    primary_entity_ids: List[ObjectId]
    relationships_info: List[EntityRelationshipInfo]


class BaseFeatureOrTargetService(
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
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
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
        self, graph: QueryGraph, node_name: str
    ) -> FeatureOrTargetDerivedData:
        """
        Extract derived data from a graph and node name

        Parameters
        ----------
        graph: QueryGraph
            Query graph
        node_name: str
            Node name

        Returns
        -------
        FeatureOrTargetDerivedData
        """
        entity_ids = graph.get_entity_ids(node_name=node_name)
        primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
            entity_ids=entity_ids
        )
        relationships_info = await self.entity_relationship_extractor_service.extract(
            entity_ids=entity_ids
        )
        return FeatureOrTargetDerivedData(
            primary_entity_ids=sorted(primary_entity_ids),
            relationships_info=relationships_info,
        )
