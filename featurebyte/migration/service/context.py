"""
ContextMigrationService class
"""
from __future__ import annotations

from featurebyte.migration.service.mixin import MigrationServiceMixin
from featurebyte.service.context import ContextService


class ContextMigrationService(ContextService, MigrationServiceMixin):
    """ContextMigrationService class"""
