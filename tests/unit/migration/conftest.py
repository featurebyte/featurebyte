"""
This module contains common fixtures used in tests/unit/migration directory
"""
from contextlib import asynccontextmanager
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from mongomock_motor import AsyncMongoMockClient

from featurebyte.migration.migration_data_service import SchemaMetadataService
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.persistent.mongo import MongoDB


@pytest.fixture(scope="session")
def user():
    """Mock user"""
    user = Mock()
    user.id = ObjectId()
    return user


@pytest.fixture(name="schema_metadata_service")
def schema_metadata_service_fixture(user, persistent):
    """Schema metadata service fixture"""
    return SchemaMetadataService(user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID)
