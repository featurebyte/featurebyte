"""
Test functions in migration/run.py
"""

import glob
import os
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from bson import json_util

from featurebyte import SourceType
from featurebyte.app import get_celery
from featurebyte.migration.model import MigrationMetadata, SchemaMetadataUpdate
from featurebyte.migration.run import (
    _extract_migrate_method_marker,
    _extract_migrate_methods,
    get_migration_methods_to_apply,
    migrate_method_generator,
    post_migration_sanity_check,
    retrieve_all_migration_methods,
    run_migration,
)
from featurebyte.migration.service.mixin import (
    BaseMongoCollectionMigration,
)
from featurebyte.query_graph.node.schema import SnowflakeDetails
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.utils.storage import get_storage, get_temp_storage


@pytest.fixture(name="schema_metadata_service")
def schema_metadata_service_fixture(app_container):
    """
    SchemaMetadataService fixture
    """
    return app_container.schema_metadata_service


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
async def test_migrate_method_generator(user, persistent, schema_metadata_service):
    """Test migrate method generator"""
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA.value
    )

    expected_method_num = len(retrieve_all_migration_methods())
    method_generator = migrate_method_generator(
        user=user,
        persistent=persistent,
        celery=get_celery(),
        storage=get_storage(),
        temp_storage=get_temp_storage(),
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
        celery=get_celery(),
        storage=get_storage(),
        temp_storage=get_temp_storage(),
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
    user,
    persistent,
    schema_metadata_service,
    insert_credential,
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
        celery=get_celery(),
        storage=get_storage(),
        temp_storage=get_temp_storage(),
        schema_metadata=schema_metadata,
        include_data_warehouse_migrations=False,
    )
    assert len([_ async for _ in method_generator]) == expected_method_num


def test_get_migration_methods_to_apply():
    """
    Test get_migration_methods_to_apply method
    """

    def _to_tuples(methods):
        return set([tuple(method.items()) for method in methods])

    methods_asof_v5 = get_migration_methods_to_apply(current_metadata_version=5)
    methods_asof_v10 = get_migration_methods_to_apply(current_metadata_version=10)
    assert len(methods_asof_v5) > len(methods_asof_v10)
    assert _to_tuples(methods_asof_v10).issubset(_to_tuples(methods_asof_v5))


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
    service = app_container.feature_store_service
    migration_service = app_container.data_warehouse_migration_service_v1
    docs = []
    for i in range(20):
        doc = await service.create_document(
            data=FeatureStoreCreate(
                name=f"feature_store_{i}",
                type=SourceType.SNOWFLAKE,
                details=SnowflakeDetails(
                    account=f"<account>_{i}",
                    warehouse="snowflake",
                    database_name="<database_name>",
                    schema_name="<schema_name>",
                    role_name="TESTING",
                ),
            ),
        )
        docs.append(doc)

    # run test_post_migration_sanity_check (should run without error as no migration is performed)
    with patch.object(
        FeatureStoreService,
        "historical_document_generator",
        wraps=migration_service.delegate_service.historical_document_generator,
    ) as mock_call:
        await post_migration_sanity_check(migration_service)

    docs = sorted(docs, key=lambda d: d.id, reverse=True)
    step_size = len(docs) // 5
    called_document_ids = [
        call_args.kwargs["document_id"] for call_args in mock_call.call_args_list
    ]
    expected_document_ids = [doc.id for i, doc in enumerate(docs) if i % step_size == 0]
    # Additional feature store document is created during the test
    assert called_document_ids[:-1] == expected_document_ids


@pytest.mark.asyncio
async def test_run_migration(
    migration_check_persistent, user, insert_credential, schema_metadata_service
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
        celery=get_celery(),
        storage=get_storage(),
        temp_storage=get_temp_storage(),
        include_data_warehouse_migrations=False,
        redis=Mock(),
    )

    # check that all migrated collections contains some examples for testing
    version = 0
    description = "Initial schema"
    async for service, migrate_method in migrate_method_generator(
        user=user,
        persistent=persistent,
        celery=get_celery(),
        storage=get_storage(),
        temp_storage=get_temp_storage(),
        schema_metadata=schema_metadata,
        include_data_warehouse_migrations=False,
    ):
        marker = _extract_migrate_method_marker(migrate_method)
        version = max(version, marker.version)
        if marker.version == version:
            description = marker.description

        delegate_service = service.delegate_service
        with delegate_service.allow_use_raw_query_filter():
            docs = await delegate_service.list_documents_as_dict(use_raw_query_filter=True)
            assert docs["total"] > 0, delegate_service

        if isinstance(service, BaseMongoCollectionMigration) and not service.skip_audit_migration:
            # check audit records only if skip_audit_migration is False
            max_audit_record_nums = 0
            for doc in docs["data"]:
                audit_docs = await delegate_service.list_document_audits(document_id=doc["_id"])
                max_audit_record_nums = max(max_audit_record_nums, audit_docs["total"])

            # check that must be at least 3 records in the audit docs
            assert max_audit_record_nums > 3, delegate_service

    # check version in schema_metadata after migration
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA.value
    )
    assert schema_metadata.version == version
    assert schema_metadata.description == description
