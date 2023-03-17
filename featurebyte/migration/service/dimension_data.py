"""
DimensionDataMigrationService class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.dimension_data import DimensionDataService


class DimensionDataMigrationService(DimensionDataService, MigrationServiceMixin):
    """DimensionDataMigrationService class"""
