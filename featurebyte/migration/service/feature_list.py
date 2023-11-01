"""
Feature list migration service
"""
from typing import Dict, List

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseDocumentServiceT, BaseMongoCollectionMigration
from featurebyte.models.persistent import Document
from featurebyte.persistent import Persistent
from featurebyte.service.entity_relationship_extractor import (
    EntityRelationshipExtractorService,
    ServingEntityEnumeration,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService

logger = get_logger(__name__)


class FeatureListMigrationServiceV5(BaseMongoCollectionMigration):
    """
    FeatureListMigrationService class

    This class is used to migrate the feature list records to add
    relationships_info and supported_serving_entity_ids fields.
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        feature_list_service: FeatureListService,
        feature_service: FeatureService,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
    ):
        super().__init__(persistent)
        self.feature_list_service = feature_list_service
        self.feature_service = feature_service
        self.entity_relationship_extractor_service = entity_relationship_extractor_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_list_service  # type: ignore[return-value]

    async def batch_preprocess_document(self, documents: List[Document]) -> List[Document]:
        """
        Preprocess the documents before migration

        Parameters
        ----------
        documents: List[Document]
            List of documents to be migrated

        Returns
        -------
        List[Document]
        """
        all_feature_ids = set()
        for document in documents:
            all_feature_ids.update(document["feature_ids"])

        # get all feature first to reduce the number of queries
        feature_id_to_primary_entity_ids: Dict[ObjectId, List[ObjectId]] = {
            feature_doc["_id"]: feature_doc["primary_entity_ids"]
            async for feature_doc in self.feature_service.list_documents_as_dict_iterator(
                query_filter={"_id": {"$in": list(all_feature_ids)}}
            )
        }
        extractor = self.entity_relationship_extractor_service
        for document in documents:
            combined_primary_entity_ids = set().union(
                *(
                    feature_id_to_primary_entity_ids[feature_id]
                    for feature_id in document["feature_ids"]
                )
            )
            primary_entity_ids = list(combined_primary_entity_ids)
            relationships_info = await extractor.extract_primary_entity_descendant_relationship(
                primary_entity_ids=primary_entity_ids
            )
            serving_entity_enumeration = ServingEntityEnumeration.create(
                relationships_info=relationships_info
            )
            supported_serving_entity_ids = serving_entity_enumeration.generate(
                entity_ids=primary_entity_ids
            )
            document["relationships_info"] = relationships_info
            document["supported_serving_entity_ids"] = supported_serving_entity_ids
        return documents

    @migrate(
        version=5,
        description="Add relationships_info & supported_serving_entity_ids to feature list record",
    )
    async def add_relationships_info_and_supported_serving_entity_ids(self) -> None:
        """Add relationships_info & supported_serving_entity_ids to feature list record"""
        sanity_check_sample_size = 10

        # use the normal query filter (contains catalog ID filter)
        query_filter = self.delegate_service.construct_list_query_filter()
        total_before = await self.get_total_record(query_filter=query_filter)  # type: ignore

        # migrate all records and audit records
        await self.migrate_all_records(
            query_filter=query_filter,
            batch_preprocess_document_func=self.batch_preprocess_document,
        )

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name=self.collection_name,
            query_filter=query_filter,
            page_size=sanity_check_sample_size,
        )
        assert total_before == total_after, (total_before, total_after)
        for doc in sample_docs_after:
            # after migration, relationships_info should not be None & supported_serving_entity_ids should not be empty
            assert isinstance(doc.get("relationships_info"), list), doc.get("relationships_info")
            assert len(doc["supported_serving_entity_ids"]) > 0, doc["supported_serving_entity_ids"]

        logger.info("Migrated all records successfully (total: %d)", total_after)
