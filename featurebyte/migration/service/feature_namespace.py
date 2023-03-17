"""
FeatureNamespaceMigrationService class
"""
from __future__ import annotations

from typing import Any, Dict

from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.feature_namespace import FeatureNamespaceService


class FeatureNamespaceMigrationService(FeatureNamespaceService, MigrationServiceMixin):
    """FeatureNamespaceMigrationService class"""

    @migrate(version=3, description="Rename field event_data_ids to tabular_data_ids")
    async def change_field_name_from_event_data_ids_to_tabular_data_ids(self) -> None:
        """Change field name from event_data_ids to tabular_data_ids"""
        # sample first 10 records before migration
        query_filter: Dict[str, Any] = {}
        page_size = 10
        _, total_before = await self.persistent.find(
            collection_name="feature_namespace", query_filter=query_filter, page_size=page_size
        )

        # migrate all records and audit records
        await self.migrate_all_records(query_filter=query_filter)

        # check the sample records after migration
        sample_docs_after, total_after = await self.persistent.find(
            collection_name="feature_namespace", query_filter=query_filter, page_size=page_size
        )
        assert total_before == total_after, (total_before, total_after)
        for doc in sample_docs_after:
            assert "event_data_ids" not in doc
            assert "tabular_data_ids" in doc
