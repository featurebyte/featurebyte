"""
Feature migration service
"""
from typing import Optional

from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseMongoCollectionMigration
from featurebyte.models.persistent import Document
from featurebyte.persistent import Persistent
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.service.entity_relationship_extractor import EntityRelationshipExtractorService
from featurebyte.service.feature import FeatureService
from featurebyte.service.relationship import BaseDocumentServiceT


class FeatureMigrationServiceV4(BaseMongoCollectionMigration):
    """
    FeatureMigrationService class

    This class is used to migrate the feature records to add relationships_info field.
    """

    def __init__(
        self,
        persistent: Persistent,
        feature_service: FeatureService,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
    ):
        super().__init__(persistent)
        self.feature_service = feature_service
        self.entity_relationship_extractor_service = entity_relationship_extractor_service
        self.derive_primary_entity_helper = derive_primary_entity_helper

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_service  # type: ignore[return-value]

    async def migrate_record(self, document: Document, version: Optional[int]) -> None:
        # derive relationships_info and add to the document
        query_graph = QueryGraph(**document["graph"])
        entity_ids = query_graph.get_entity_ids(node_name=document["node_name"])
        primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
            entity_ids=entity_ids
        )
        extractor = self.entity_relationship_extractor_service
        relationships_info = await extractor.extract_relationship_from_primary_entity(
            entity_ids=entity_ids, primary_entity_ids=primary_entity_ids
        )
        document["primary_entity_ids"] = sorted(primary_entity_ids)
        document["relationships_info"] = [
            relationship.dict(by_alias=True) for relationship in relationships_info
        ]

        await self.persistent.migrate_record(
            collection_name=self.collection_name,
            document=document,
            migrate_func=self.migrate_document_record,
        )

    @migrate(version=4, description="Add relationships_info to feature record")
    async def add_relationships_info(self) -> None:
        """Change field name from event_data_ids to tabular_data_ids"""
        sanity_check_sample_size = 10

        # use the normal query filter (contains catalog ID filter)
        query_filter = self.delegate_service.construct_list_query_filter()
        total_before = await self.get_total_migration_records(query_filter=query_filter)  # type: ignore

        # migrate all records and audit records
        await self.migrate_all_records(query_filter=query_filter)

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name=self.collection_name,
            query_filter=query_filter,
            page_size=sanity_check_sample_size,
        )
        assert total_before == total_after, (total_before, total_after)
        for doc in sample_docs_after:
            assert isinstance(doc.get("relationships_info"), list), doc.get("relationships_info")
