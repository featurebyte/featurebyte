"""
Tests for FeatureStoreMigrationService
"""

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.base import User
from featurebyte.schema.feature_store import FeatureStoreCreate


@pytest_asyncio.fixture
async def migration_service(app_container):
    """
    Fixture for FeatureStoreMigrationService
    """
    from featurebyte.migration.service.feature_store import FeatureStoreMigrationService

    return FeatureStoreMigrationService(
        persistent=app_container.persistent,
        feature_store_service=app_container.feature_store_service,
        feature_store_table_cleanup_scheduler_service=app_container.feature_store_table_cleanup_scheduler_service,
    )


@pytest_asyncio.fixture
async def test_feature_store(app_container, user):
    """
    Fixture for creating test feature store without cleanup task
    """
    feature_store_service = app_container.feature_store_service
    feature_store = await feature_store_service.create_document(
        data=FeatureStoreCreate(
            name="Test Feature Store Migration",
            type="snowflake",
            details={
                "account": "test_account",
                "warehouse": "test_warehouse",
                "database_name": "test_db",
                "schema_name": "test_schema",
                "role_name": "test_role",
            },
        )
    )

    # Verify no cleanup task exists initially
    periodic_task_service = app_container.periodic_task_service
    task_name = f"feature_store_table_cleanup_{feature_store.id}"
    initial_tasks = await periodic_task_service.list_documents_as_dict(
        page=1, page_size=1, query_filter={"name": task_name}
    )
    assert len(initial_tasks["data"]) == 0, "Feature store should not have cleanup task initially"

    return feature_store


@pytest.mark.asyncio
async def test_delegate_service_property(migration_service, app_container):
    """Test delegate_service property returns feature store service"""
    assert migration_service.delegate_service == app_container.feature_store_service


@pytest.mark.asyncio
async def test_schedule_table_cleanup_tasks_creates_periodic_task(
    migration_service,
    test_feature_store,
    app_container,
):
    """Test that migration creates actual periodic task in database"""
    periodic_task_service = app_container.periodic_task_service
    task_name = f"feature_store_table_cleanup_{test_feature_store.id}"

    # Run the migration
    await migration_service.schedule_table_cleanup_tasks()

    # Verify cleanup task was created
    created_tasks = await periodic_task_service.list_documents_as_dict(
        page=1, page_size=1, query_filter={"name": task_name}
    )
    assert len(created_tasks["data"]) == 1, "Migration should create cleanup task"

    task = created_tasks["data"][0]

    # Verify task configuration
    assert task["name"] == task_name
    assert task["interval"]["every"] == 86400  # 24 hours in seconds
    assert task["interval"]["period"] == "seconds"
    assert task["time_modulo_frequency_second"] == 7200  # 2-hour offset

    # Verify task payload has correct parameters
    kwargs = task["kwargs"]
    assert kwargs["user_id"] == str(test_feature_store.user_id)
    assert kwargs["feature_store_id"] == str(test_feature_store.id)
    assert kwargs["command"] == "FEATURE_STORE_TABLE_CLEANUP"


@pytest.mark.asyncio
async def test_schedule_table_cleanup_tasks_idempotent(
    migration_service,
    test_feature_store,
    app_container,
):
    """Test that running migration twice doesn't create duplicate tasks"""
    periodic_task_service = app_container.periodic_task_service
    task_name = f"feature_store_table_cleanup_{test_feature_store.id}"

    # Run migration twice
    await migration_service.schedule_table_cleanup_tasks()
    await migration_service.schedule_table_cleanup_tasks()

    # Verify only one task exists
    tasks = await periodic_task_service.list_documents_as_dict(
        page=1, page_size=10, query_filter={"name": task_name}
    )
    assert len(tasks["data"]) == 1, "Migration should be idempotent - only one task should exist"


@pytest.mark.asyncio
async def test_schedule_table_cleanup_tasks_preserves_user_context(
    migration_service,
    app_container,
):
    """Test that migration uses the correct user context from feature store owner"""
    feature_store_service = app_container.feature_store_service
    periodic_task_service = app_container.periodic_task_service

    # Create feature store with specific user
    specific_user_id = ObjectId()
    feature_store_service.user = User(id=specific_user_id)
    feature_store = await feature_store_service.create_document(
        data=FeatureStoreCreate(
            name="User Context Test Feature Store",
            type="snowflake",
            details={
                "account": "test_account",
                "warehouse": "test_warehouse",
                "database_name": "test_db",
                "schema_name": "test_schema",
                "role_name": "test_role",
            },
        )
    )

    # Run migration
    await migration_service.schedule_table_cleanup_tasks()

    # Verify task was created with correct user context
    task_name = f"feature_store_table_cleanup_{feature_store.id}"
    tasks = await periodic_task_service.list_documents_as_dict(
        page=1, page_size=1, query_filter={"name": task_name}
    )
    assert len(tasks["data"]) == 1

    task = tasks["data"][0]
    assert task["kwargs"]["user_id"] == str(
        specific_user_id
    ), "Migration should preserve feature store owner's user ID"


@pytest.mark.asyncio
async def test_schedule_table_cleanup_tasks_multiple_feature_stores(
    migration_service,
    app_container,
):
    """Test migration handles multiple feature stores with different owners"""
    feature_store_service = app_container.feature_store_service
    periodic_task_service = app_container.periodic_task_service

    # Create two feature stores with different users
    user_1 = ObjectId()
    user_2 = ObjectId()

    feature_store_service.user = User(id=user_1)
    feature_store_1 = await feature_store_service.create_document(
        data=FeatureStoreCreate(
            name="Multi Test Store 1",
            type="snowflake",
            details={
                "account": "test_account_1",
                "warehouse": "test_warehouse",
                "database_name": "test_db",
                "schema_name": "test_schema",
                "role_name": "test_role",
            },
        )
    )

    feature_store_service.user = User(id=user_2)
    feature_store_2 = await feature_store_service.create_document(
        data=FeatureStoreCreate(
            name="Multi Test Store 2",
            type="snowflake",
            details={
                "account": "test_account_2",
                "warehouse": "test_warehouse",
                "database_name": "test_db",
                "schema_name": "test_schema",
                "role_name": "test_role",
            },
        )
    )

    # Run migration
    await migration_service.schedule_table_cleanup_tasks()

    # Verify both cleanup tasks were created
    task_1_name = f"feature_store_table_cleanup_{feature_store_1.id}"
    task_2_name = f"feature_store_table_cleanup_{feature_store_2.id}"

    task_1_results = await periodic_task_service.list_documents_as_dict(
        page=1, page_size=1, query_filter={"name": task_1_name}
    )
    task_2_results = await periodic_task_service.list_documents_as_dict(
        page=1, page_size=1, query_filter={"name": task_2_name}
    )

    assert len(task_1_results["data"]) == 1, "Should create task for feature store 1"
    assert len(task_2_results["data"]) == 1, "Should create task for feature store 2"

    # Verify each task has correct user context
    task_1 = task_1_results["data"][0]
    task_2 = task_2_results["data"][0]

    assert task_1["kwargs"]["user_id"] == str(user_1), "Task 1 should have user 1's ID"
    assert task_2["kwargs"]["user_id"] == str(user_2), "Task 2 should have user 2's ID"
    assert task_1["kwargs"]["feature_store_id"] == str(feature_store_1.id)
    assert task_2["kwargs"]["feature_store_id"] == str(feature_store_2.id)


@pytest.mark.asyncio
async def test_schedule_table_cleanup_tasks_existing_task_unchanged(
    migration_service,
    test_feature_store,
    app_container,
):
    """Test that existing cleanup tasks are left unchanged"""
    periodic_task_service = app_container.periodic_task_service
    scheduler_service = app_container.feature_store_table_cleanup_scheduler_service

    # Pre-create a cleanup task
    await scheduler_service.start_job_if_not_exist(test_feature_store.id)

    # Get the task details before migration
    task_name = f"feature_store_table_cleanup_{test_feature_store.id}"
    before_tasks = await periodic_task_service.list_documents_as_dict(
        page=1, page_size=1, query_filter={"name": task_name}
    )
    assert len(before_tasks["data"]) == 1
    original_task = before_tasks["data"][0]
    original_created_at = original_task["created_at"]

    # Run migration
    await migration_service.schedule_table_cleanup_tasks()

    # Verify task still exists and wasn't recreated
    after_tasks = await periodic_task_service.list_documents_as_dict(
        page=1, page_size=1, query_filter={"name": task_name}
    )
    assert len(after_tasks["data"]) == 1
    final_task = after_tasks["data"][0]

    # Task should be the same (not recreated)
    assert final_task["_id"] == original_task["_id"], "Existing task should not be recreated"
    assert final_task["created_at"] == original_created_at, "Creation time should be unchanged"
