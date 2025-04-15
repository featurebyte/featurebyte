"""
This module contains common fixtures used in tests/unit/migration directory
"""

from unittest.mock import Mock

import pytest
from bson import ObjectId

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.migration.migration_data_service import SchemaMetadataService
from featurebyte.routes.block_modification_handler import BlockModificationHandler


@pytest.fixture(scope="session")
def user():
    """Mock user"""
    user = Mock()
    user.id = ObjectId()
    return user


@pytest.fixture(name="schema_metadata_service")
def schema_metadata_service_fixture(user, persistent, storage):
    """Schema metadata service fixture"""
    return SchemaMetadataService(
        user=user,
        persistent=persistent,
        catalog_id=DEFAULT_CATALOG_ID,
        block_modification_handler=BlockModificationHandler(),
        storage=storage,
        redis=Mock(),
    )
