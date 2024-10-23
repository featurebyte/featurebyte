"""
Feature migration service
"""

from typing import Dict, List

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseMongoCollectionMigration
from featurebyte.models.entity import EntityModel
from featurebyte.models.persistent import Document
from featurebyte.persistent import Persistent
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_relationship_extractor import EntityRelationshipExtractorService
from featurebyte.service.feature import FeatureService
from featurebyte.service.relationship import BaseDocumentServiceT
from featurebyte.service.relationship_info import RelationshipInfoService

logger = get_logger(__name__)


class FeatureMigrationServiceV4(BaseMongoCollectionMigration):
    """
    FeatureMigrationService class

    This class is used to migrate the feature records to add relationships_info field.
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        feature_service: FeatureService,
        entity_service: EntityService,
        relationship_info_service: RelationshipInfoService,
        entity_relationship_extractor_service: EntityRelationshipExtractorService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
    ):
        super().__init__(persistent)
        self.feature_service = feature_service
        self.entity_service = entity_service
        self.relationship_info_service = relationship_info_service
        self.entity_relationship_extractor_service = entity_relationship_extractor_service
        self.derive_primary_entity_helper = derive_primary_entity_helper

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_service  # type: ignore[return-value]

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
        all_entity_ids = set()
        for document in documents:
            all_entity_ids.update(document["entity_ids"])

        # get all entity first to reduce the number of queries
        entity_id_to_entity: Dict[ObjectId, EntityModel] = {
            entity.id: entity
            async for entity in self.entity_service.list_documents_iterator(
                query_filter={"_id": {"$in": list(all_entity_ids)}}
            )
        }
        extractor = self.entity_relationship_extractor_service
        for document in documents:
            primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
                entity_ids=document["entity_ids"], entity_id_to_entity=entity_id_to_entity
            )
            relationships_info = await extractor.extract_relationship_from_primary_entity(
                entity_ids=document["entity_ids"],
                primary_entity_ids=primary_entity_ids,
            )
            document["primary_entity_ids"] = sorted(primary_entity_ids)
            document["relationships_info"] = [
                relationship.model_dump(by_alias=True) for relationship in relationships_info
            ]
        return documents

    @migrate(version=4, description="Add primary_entity_ids & relationships_info to feature record")
    async def add_primary_entity_ids_and_relationships_info(self) -> None:
        """Add primary_entity_ids & relationships_info to feature record"""
        sanity_check_sample_size = 10

        # use the normal query filter (contains catalog ID filter)
        query_filter = await self.delegate_service.construct_list_query_filter()
        total_before = await self.get_total_record(query_filter=query_filter)

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
            # after migration, relationships_info should not be None
            assert isinstance(doc.get("relationships_info"), list), doc.get("relationships_info")
            if doc.get("entity_ids"):
                # after migration, if entity_ids is not empty, primary_entity_ids should not be empty
                assert len(doc["primary_entity_ids"]) > 0, doc["primary_entity_ids"]

        logger.info("Migrated all records successfully (total: %d)", total_after)


class FeatureMigrationServiceV8(BaseMongoCollectionMigration):
    """
    FeatureMigrationService class

    This class is used to migrate the feature records to add following fields:
    - table_id_feature_job_settings
    - table_id_cleaning_operations
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        feature_service: FeatureService,
    ):
        super().__init__(persistent)
        self.feature_service = feature_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_service  # type: ignore[return-value]

    @migrate(
        version=8,
        description=(
            "Add table_id_column_names, table_id_feature_job_settings & table_id_cleaning_operations "
            "to feature record"
        ),
    )
    async def add_table_id_feature_job_settings_and_table_id_cleaning_operations(self) -> None:
        """Add table_id_feature_job_settings & table_id_cleaning_operations to feature record"""
        sanity_check_sample_size = 10

        # use the normal query filter (contains catalog ID filter)
        query_filter = await self.delegate_service.construct_list_query_filter()
        total_before = await self.get_total_record(query_filter=query_filter)

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
            assert "table_id_column_names" in doc, doc
            assert "table_id_feature_job_settings" in doc, doc
            assert "table_id_cleaning_operations" in doc, doc

        logger.info("Migrated all records successfully (total: %d)", total_after)

    @migrate(
        version=10, description="Migrate feature job settings to new format (feature collection)."
    )
    async def migrate_feature_job_setting_to_new_format(self) -> None:
        """Migrate feature job settings to new format"""
        sanity_check_sample_size = 10

        # use the normal query filter (contains catalog ID filter)
        query_filter = await self.delegate_service.construct_list_query_filter()
        total_before = await self.get_total_record(query_filter=query_filter)

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
            settings = doc.get("table_id_feature_job_settings") or []
            for setting in settings:
                fjs = setting["feature_job_setting"]
                assert "blind_spot" in fjs, fjs
                assert "period" in fjs, fjs
                assert "offset" in fjs, fjs
                assert "execution_buffer" in fjs, fjs

        logger.info("Migrated all records successfully (total: %d)", total_after)
