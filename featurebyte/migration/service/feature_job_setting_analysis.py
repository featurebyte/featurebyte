"""
Feature Job Settings Analysis migration service
"""

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseDocumentServiceT, BaseMongoCollectionMigration
from featurebyte.persistent import Persistent
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService

logger = get_logger(__name__)


class FeatureJobSettingAnalysisMigrationServiceV13(BaseMongoCollectionMigration):
    """
    FeatureJobSettingAnalysisMigrationService class

    This class is used to migrate the feature job setting analysis records.
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        feature_job_setting_analysis_service: FeatureJobSettingAnalysisService,
    ):
        super().__init__(persistent)
        self.feature_job_setting_analysis_service = feature_job_setting_analysis_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.feature_job_setting_analysis_service  # type: ignore[return-value]

    @migrate(
        version=13,
        description="Migrate feature job settings to new format (feature_job_setting_analysis collection).",
    )
    async def migrate_feature_job_setting_to_new_format(self) -> None:
        """Migration feature job settings to new format"""
        sanity_check_sample_size = 10

        # use the normal query filter (contains catalog ID filter)
        query_filter = await self.delegate_service.construct_list_query_filter()
        total_before = await self.get_total_record(query_filter=query_filter)

        # migrate all records
        await self.migrate_all_records(query_filter=query_filter)

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name=self.collection_name,
            query_filter=query_filter,
            page_size=sanity_check_sample_size,
        )
        assert total_before == total_after, (total_before, total_after)
        for doc in sample_docs_after:
            fjs = doc["analysis_result"]["recommended_feature_job_setting"]
            assert "blind_spot" in fjs, fjs
            assert "period" in fjs, fjs
            assert "offset" in fjs, fjs
            assert "feature_cutoff_modulo_frequency" in fjs, fjs

        logger.info("Migrated all records successfully (total: %d)", total_after)
