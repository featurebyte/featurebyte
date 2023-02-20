"""
FeatureJobSettingAnalysisMigrationService class
"""
from __future__ import annotations

from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService


class FeatureJobSettingAnalysisMigrationService(
    FeatureJobSettingAnalysisService, MigrationServiceMixin
):
    """FeatureJobSettingAnalysisMigrationService class"""

    @migrate(version=19, description="Add field workspace_id")
    async def add_field_workspace_id(self) -> None:
        """Add workspace_id field"""
        # sample first 10 records before migration
        sample_docs_before, total_before = await self.persistent.find(
            collection_name="feature_job_setting_analysis", query_filter={}, page_size=10
        )
        sample_docs_before_map = {doc["_id"]: doc for doc in sample_docs_before}

        # migrate all records and audit records
        await self.migrate_all_records(query_filter={})

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name="feature_job_setting_analysis",
            query_filter={"_id": {"$in": list(sample_docs_before_map)}},
        )
        sample_docs_after_map = {doc["_id"]: doc for doc in sample_docs_after}
        assert total_before == total_after
        for doc in sample_docs_after_map.values():
            assert "workspace_id" in doc
            assert doc["workspace_id"] == DEFAULT_WORKSPACE_ID
