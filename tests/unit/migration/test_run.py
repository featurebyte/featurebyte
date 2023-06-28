"""
Test functions in migration/run.py
"""
import glob
import json
import os
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from bson import json_util

from featurebyte.app import get_celery
from featurebyte.migration.model import MigrationMetadata, SchemaMetadataUpdate
from featurebyte.migration.run import (
    _extract_migrate_method_marker,
    _extract_migrate_methods,
    migrate_method_generator,
    post_migration_sanity_check,
    retrieve_all_migration_methods,
    run_migration,
)
from featurebyte.migration.service.mixin import DataWarehouseMigrationMixin
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.schema.entity import EntityCreate
from featurebyte.service.entity import EntityService


@pytest.fixture(name="schema_metadata_service")
def schema_metadata_service_fixture(app_container):
    """
    SchemaMetadataService fixture
    """
    return app_container.schema_metadata_service


def test_retrieve_all_migration_methods():
    """Test retrieve_all_migration_methods output"""
    migrate_methods = retrieve_all_migration_methods()

    expected_versions = set(range(1, len(migrate_methods) + 1))
    missing_versions = expected_versions.difference(migrate_methods)
    if missing_versions:
        raise ValueError(f"Missing migrate version detected: {missing_versions}")


@patch("featurebyte.migration.run._extract_migrate_methods")
def test_retrieve_all_migration_methods__duplicated_version(mock_extract_method):
    """Test retrieve_all_migration_methods with duplicated version"""

    def new_extract_method(service_class):
        methods = _extract_migrate_methods(service_class)
        return 2 * [(1, attr_name) for _, attr_name in methods]

    mock_extract_method.side_effect = new_extract_method
    with pytest.raises(ValueError) as exc:
        retrieve_all_migration_methods()
    assert "Duplicated migrate version detected" in str(exc.value)


@pytest.mark.asyncio
async def test_migrate_method_generator(user, persistent, get_credential, schema_metadata_service):
    """Test migrate method generator"""
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA.value
    )

    expected_method_num = len(retrieve_all_migration_methods())
    method_generator = migrate_method_generator(
        user=user,
        persistent=persistent,
        get_credential=get_credential,
        celery=get_celery(),
        schema_metadata=schema_metadata,
        include_data_warehouse_migrations=True,
    )
    assert len([_ async for _ in method_generator]) == expected_method_num

    # bump version to 1
    updated_schema_metadata = await schema_metadata_service.update_document(
        schema_metadata.id, data=SchemaMetadataUpdate(version=1, description="Some description")
    )
    assert updated_schema_metadata.version == 1
    assert updated_schema_metadata.description == "Some description"

    # check generator output
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA.value
    )
    method_generator = migrate_method_generator(
        user=user,
        persistent=persistent,
        get_credential=get_credential,
        celery=get_celery(),
        schema_metadata=schema_metadata,
        include_data_warehouse_migrations=True,
    )
    methods = [method async for method in method_generator]
    assert len(methods) == expected_method_num - 1
    for _, method in methods:
        marker = _extract_migrate_method_marker(method)
        assert marker.version > 1


@pytest.mark.asyncio
async def test_migrate_method_generator__exclude_warehouse(
    user, persistent, get_credential, schema_metadata_service
):
    """Test migrate method generator with include_data_warehouse_migrations=False"""
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA.value
    )

    expected_num_warehouse_migrations = 3
    expected_method_num = len(retrieve_all_migration_methods()) - expected_num_warehouse_migrations
    method_generator = migrate_method_generator(
        user=user,
        persistent=persistent,
        get_credential=get_credential,
        celery=get_celery(),
        schema_metadata=schema_metadata,
        include_data_warehouse_migrations=False,
    )
    assert len([_ async for _ in method_generator]) == expected_method_num


@pytest_asyncio.fixture(name="migration_check_persistent")
async def migration_check_user_persistent_fixture(test_dir, persistent):
    """Insert testing samples into the persistent"""
    fixture_glob_pattern = os.path.join(test_dir, "fixtures/migration/*")
    for file_name in glob.glob(fixture_glob_pattern):
        records = json_util.loads(open(file_name).read())
        collection_name = os.path.basename(file_name)
        await persistent._insert_many(collection_name=collection_name, documents=records)
    return persistent


@pytest.mark.asyncio
async def test_post_migration_sanity_check(app_container):
    """Test post_migration_sanity_check"""
    service = app_container.entity_service
    docs = []
    for i in range(20):
        doc = await service.create_document(
            data=EntityCreate(name=f"entity_{i}", serving_name=f"serving_name_{i}")
        )
        docs.append(doc)

    # run test_post_migration_sanity_check (should run without error as no migration is performed)
    with patch.object(
        EntityService,
        "historical_document_generator",
        wraps=service.historical_document_generator,
    ) as mock_call:
        await post_migration_sanity_check(service)

    docs = sorted(docs, key=lambda d: d.id, reverse=True)
    step_size = len(docs) // 5
    called_document_ids = [
        call_args.kwargs["document_id"] for call_args in mock_call.call_args_list
    ]
    expected_document_ids = [doc.id for i, doc in enumerate(docs) if i % step_size == 0]
    assert called_document_ids == expected_document_ids


@pytest.mark.asyncio
async def test_run_migration(
    migration_check_persistent, user, get_credential, schema_metadata_service
):
    """Test run migration function"""
    persistent = migration_check_persistent
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA.value
    )

    # perform migration on testing samples to check the migration logic
    await run_migration(
        user=user,
        persistent=persistent,
        get_credential=get_credential,
        celery=get_celery(),
        include_data_warehouse_migrations=False,
    )

    # check that all migrated collections contains some examples for testing
    version = 0
    description = "Initial schema"
    async for service, migrate_method in migrate_method_generator(
        user=user,
        persistent=persistent,
        get_credential=get_credential,
        celery=get_celery(),
        schema_metadata=schema_metadata,
        include_data_warehouse_migrations=False,
    ):
        marker = _extract_migrate_method_marker(migrate_method)
        version = max(version, marker.version)
        if marker.version == version:
            description = marker.description

        docs = await service.list_documents()
        assert docs["total"] > 0, service

        # check that must be at least 3 records in the audit docs
        max_audit_record_nums = 0
        for doc in docs["data"]:
            audit_docs = await service.list_document_audits(document_id=doc["_id"])
            max_audit_record_nums = max(max_audit_record_nums, audit_docs["total"])
        assert max_audit_record_nums > 1, service

    # check version in schema_metadata after migration
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA.value
    )
    assert schema_metadata.version == version
    assert schema_metadata.description == description


@pytest.mark.asyncio
@patch("featurebyte.service.session_manager.SessionManager")
async def test_data_warehouse_migration_get_session(
    mock_session_manager,
    migration_check_persistent,
    test_dir,
    user,
    app_container,
):
    """Test data warehouse migration get_session method"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_store.json")
    feature_store_user_id = user.id
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_store = FeatureStoreModel(**{**payload, "user_id": feature_store_user_id})

    get_credential_func = AsyncMock()
    warehouse_migration = DataWarehouseMigrationMixin(
        persistent=migration_check_persistent,
        session_manager_service=app_container.session_manager_service,
    )
    warehouse_migration.set_credential_callback(get_credential=get_credential_func)

    # check get_credential called parameters
    mock_session_manager.return_value.get_session = AsyncMock()
    _ = await warehouse_migration.get_session(feature_store=feature_store)
    get_credential_func.assert_called_with(
        user_id=feature_store_user_id, feature_store_name=feature_store.name
    )
