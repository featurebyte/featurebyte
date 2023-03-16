"""
SCDDataMigrationService class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.scd_table import SCDTableService


class SCDDataMigrationService(SCDTableService, MigrationServiceMixin):
    """SCDDataMigrationService class"""
