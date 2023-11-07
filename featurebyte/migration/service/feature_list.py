"""
Feature list migration service
"""
from typing import Dict, List

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseDocumentServiceT, BaseMongoCollectionMigration
from featurebyte.models.feature import FeatureModel
from featurebyte.models.persistent import Document
from featurebyte.persistent import Persistent
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService

logger = get_logger(__name__)


class BaseFeatureListMigrationService(BaseMongoCollectionMigration):
    """
    BaseFeatureListMigrationService class

    This class is used to migrate the feature list records.
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        feature_list_service: FeatureListService,
        feature_service: FeatureService,
    ):
        super().__init__(persistent)
        self.feature_list_service = feature_list_service
        self.feature_service = feature_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_list_service  # type: ignore[return-value]


class FeatureListMigrationServiceV5(BaseFeatureListMigrationService):
    """
    FeatureListMigrationService class

    This class is used to migrate the feature list records to add following fields:
    - relationships_info
    - supported_serving_entity_ids
    - primary_entity_ids
    """

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
        for document in documents:
            derived_data = await self.feature_list_service.extract_entity_relationship_data(
                feature_primary_entity_ids=[
                    feature_id_to_primary_entity_ids[feature_id]
                    for feature_id in document["feature_ids"]
                ]
            )
            document["primary_entity_ids"] = derived_data.primary_entity_ids
            document["relationships_info"] = derived_data.relationships_info
            document["supported_serving_entity_ids"] = derived_data.supported_serving_entity_ids
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
            assert "primary_entity_ids" in doc, doc

        logger.info("Migrated all records successfully (total: %d)", total_after)


class FeatureListMigrationServiceV6(BaseFeatureListMigrationService):
    """
    FeatureListMigrationService class

    This class is used to migrate the feature list records to add following fields:
    - dtype_distribution
    - features_primary_entity_ids
    - entity_ids
    """

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
        feature_id_to_feature: Dict[ObjectId, FeatureModel] = {
            feature.id: feature
            async for feature in self.feature_service.list_documents_iterator(
                query_filter={"_id": {"$in": list(all_feature_ids)}}
            )
        }
        for document in documents:
            document["features"] = [
                feature_id_to_feature[feature_id] for feature_id in document["feature_ids"]
            ]
        return documents

    @staticmethod
    def post_migration_check(doc: Document) -> None:
        """
        Post migration check

        Parameters
        ----------
        doc: Document
            Document to be checked
        """
        # after migration, dtype_distribution should not be empty
        assert doc["dtype_distribution"], doc["dtype_distribution"]
        assert doc["features_primary_entity_ids"], doc["features_primary_entity_ids"]
        assert set(doc["primary_entity_ids"]).issubset(doc["entity_ids"]), doc

    @migrate(
        version=6,
        description="Add dtype_distribution to feature list record",
    )
    async def run_migration(self) -> None:
        """Add dtype_distribution to feature list record"""
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
            # after migration, dtype_distribution should not be empty
            self.post_migration_check(doc)

        logger.info("Migrated all records successfully (total: %d)", total_after)


class FeatureListMigrationServiceV7(FeatureListMigrationServiceV6):
    """
    FeatureListMigrationService class

    This class is used to migrate the feature list records to add following fields:
    - table_ids
    """

    @staticmethod
    def post_migration_check(doc: Document) -> None:
        assert "table_ids" in doc, "table_ids should be added to feature list record"

    @migrate(version=7, description="Add table_ids to feature list record")
    async def run_migration(self) -> None:
        await super().run_migration()
