"""
ItemDataMigrationService class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.item_table import ItemTableService


class ItemDataMigrationService(ItemTableService, MigrationServiceMixin):
    """ItemDataMigrationService class"""
