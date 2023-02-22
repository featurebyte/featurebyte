"""
FeatureListMigrationService class
"""
from __future__ import annotations

from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.feature_list import FeatureListService


class FeatureListMigrationService(FeatureListService, MigrationServiceMixin):
    """FeatureListMigrationService class"""
