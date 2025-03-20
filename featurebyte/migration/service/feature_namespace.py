"""
Feature namespace migration service
"""

from typing import List

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseDocumentServiceT, BaseMongoCollectionMigration
from featurebyte.models.persistent import Document
from featurebyte.persistent import Persistent
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_type import FeatureTypeService

logger = get_logger(__name__)


class FeatureNamespaceMigrationServiceV19(BaseMongoCollectionMigration):
    """
    FeatureNamespaceMigrationServiceV19 class

    This class is used to migrate the feature namespace records to add feature_type field.
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        feature_namespace_service: FeatureNamespaceService,
        feature_service: FeatureService,
        feature_type_service: FeatureTypeService,
    ):
        super().__init__(persistent)
        self.feature_namespace_service = feature_namespace_service
        self.feature_service = feature_service
        self.feature_type_service = feature_type_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_namespace_service  # type: ignore[return-value]

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
        for document in documents:
            feature = await self.feature_service.get_document(
                document_id=document["default_feature_id"]
            )
            document["feature_type"] = await self.feature_type_service.detect_feature_type(
                feature=feature
            )
        return documents

    @migrate(version=19, description="Add feature_type field to feature namespace records")
    async def add_feature_type_to_feature_namespace(self) -> None:
        """Add feature_type field to feature namespace records"""
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
            # after migration, feature_type should be added to all records
            assert doc.get("feature_type")

        logger.info("Migrated all records successfully (total: %d)", total_after)
