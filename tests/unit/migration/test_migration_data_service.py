"""
Test migration related service(s)
"""
import pytest

from featurebyte.migration.model import MigrationMetadata
from featurebyte.migration.service import migrate


@pytest.mark.asyncio
async def test_get_or_create_document(schema_metadata_service):
    """Test get or create document"""
    docs = await schema_metadata_service.list_documents_as_dict()
    assert len(docs["data"]) == 0

    created_doc = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA
    )
    assert created_doc.version == 0
    docs = await schema_metadata_service.list_documents_as_dict()
    assert len(docs["data"]) == 1
    assert docs["data"][0] == created_doc.dict(by_alias=True)

    retrieved_doc = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA
    )
    assert retrieved_doc == created_doc
    docs = await schema_metadata_service.list_documents_as_dict()
    assert len(docs["data"]) == 1
    assert docs["data"][0] == retrieved_doc.dict(by_alias=True)


@pytest.mark.asyncio
async def test_migrate_decorator():
    """Test migrate decorator"""

    @migrate(version=1, description="some description")
    async def migration_func():
        """Some migration work"""
        return 100

    migration_marker = migration_func._MigrationInfo__marker
    assert migration_marker.version == 1
    assert migration_marker.description == "some description"

    # check decorated function output
    output = await migration_func()
    assert output == 100
