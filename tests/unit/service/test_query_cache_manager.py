"""
Unit tests for QueryCacheManagerService
"""

from datetime import datetime, timedelta
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest
from freezegun import freeze_time
from sqlglot import parse_one

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models.query_cache import QueryCacheType
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="document_service")
def document_service_fixture(app_container):
    """
    Fixture for a QueryCacheDocumentService
    """
    return app_container.query_cache_document_service


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    Fixture for a QueryCacheManagerService
    """
    return app_container.query_cache_manager_service


@pytest.fixture(name="periodic_task_service")
def periodic_task_service_fixture(app_container):
    """
    Fixture for a PeriodicTaskService
    """
    return app_container.periodic_task_service


@pytest.fixture(name="cleanup_service")
def cleanup_service_fixture(app_container):
    """
    Fixture for a QueryCacheCleanupService
    """
    return app_container.query_cache_cleanup_service


@pytest.fixture(name="cleanup_scheduler_service")
def cleanup_scheduler_service_fixture(app_container):
    """
    Fixture for a QueryCacheCleanupSchedulerService
    """
    return app_container.query_cache_cleanup_scheduler_service


@pytest.fixture(name="feature_store_id")
def feature_store_id_fixture(feature_store):
    """
    Fixture for a feature store id
    """
    return feature_store.id


@pytest.fixture(name="mock_snowflake_session")
def mock_snowflake_session_fixture(mock_snowflake_session):
    """
    Patch get_feature_store_session to return a mock session
    """
    with patch(
        "featurebyte.service.session_manager.SessionManagerService.get_feature_store_session",
    ) as patched_get_feature_store_session:
        patched_get_feature_store_session.return_value = mock_snowflake_session
        yield mock_snowflake_session


async def get_all_periodic_tasks(periodic_task_service):
    """
    Helper function to retrieve all scheduled periodic tasks
    """
    with periodic_task_service.allow_use_raw_query_filter():
        tasks = []
        async for doc in periodic_task_service.list_documents_as_dict_iterator(
            query_filter={}, use_raw_query_filter=True
        ):
            tasks.append(doc)
        return tasks


@pytest.mark.asyncio
async def test_cache_and_get_table(service, periodic_task_service, feature_store_id):
    """
    Test caching a table query and retrieval
    """
    query = "SELECT * FROM table_1"

    # No cache
    result = await service.get_cached_table(feature_store_id, query)
    assert result is None
    tasks = await get_all_periodic_tasks(periodic_task_service)
    assert tasks == []

    # Cache table
    await service.cache_table(feature_store_id, query, "created_table_1")
    tasks = await get_all_periodic_tasks(periodic_task_service)
    assert len(tasks) == 1
    assert tasks[0]["name"] == f"query_cache_cleanup_feature_store_id_{feature_store_id}"
    assert tasks[0]["kwargs"] == {
        "task_type": "io_task",
        "priority": 0,
        "output_document_id": tasks[0]["kwargs"]["output_document_id"],
        "is_scheduled_task": True,
        "user_id": str(periodic_task_service.user.id),
        "catalog_id": str(DEFAULT_CATALOG_ID),
        "feature_store_id": str(feature_store_id),
        "command": "QUERY_CACHE_CLEANUP",
        "output_collection_name": None,
        "is_revocable": False,
        "is_rerunnable": False,
    }

    # Check retrieval
    result = await service.get_cached_table(feature_store_id, query)
    assert result == "created_table_1"

    # Check retrieval on a later date after cache should be state (even without cleanup)
    with freeze_time(datetime.utcnow() + timedelta(days=100)):
        result = await service.get_cached_table(feature_store_id, query)
        assert result is None


@pytest.mark.asyncio
async def test_get_or_cache_table(
    service, mock_snowflake_session, feature_store_id, update_fixtures
):
    """
    Test get_or_cache_table
    """
    table_expr = parse_one("SELECT * FROM table_1")
    cached_table_name_1 = await service.get_or_cache_table(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        table_expr=table_expr,
    )
    cached_table_name_2 = await service.get_or_cache_table(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        table_expr=table_expr,
    )
    assert cached_table_name_1 == cached_table_name_2
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/query_cache_manager/get_or_cache_table.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_get_or_cache_table_with_error(
    service, mock_snowflake_session, feature_store_id, update_fixtures
):
    """
    Test get_or_cache_table when cached table became invalid unexpectedly
    """
    table_expr = parse_one("SELECT * FROM table_1")
    cached_table_name_1 = await service.get_or_cache_table(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        table_expr=table_expr,
    )

    # Simulate table not found error for the cached table
    mock_snowflake_session.table_exists.side_effect = lambda _: False
    cached_table_name_2 = await service.get_or_cache_table(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        table_expr=table_expr,
    )

    # Query should be executed and cached again
    assert cached_table_name_1 != cached_table_name_2
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/query_cache_manager/get_or_cache_table_with_error.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_cache_and_get_dataframe(service, periodic_task_service, feature_store_id):
    """
    Test caching a dataframe and retrieval
    """
    query = "SELECT * FROM table_1"
    dataframe = pd.DataFrame({"a": [1, 2, 3]}, index=["x", "y", "z"])

    # No cache
    result = await service.get_cached_dataframe(feature_store_id, query)
    assert result is None
    tasks = await get_all_periodic_tasks(periodic_task_service)
    assert tasks == []

    # Cache table
    await service.cache_dataframe(feature_store_id, query, dataframe)
    tasks = await get_all_periodic_tasks(periodic_task_service)
    assert len(tasks) == 1

    # Check retrieval
    result = await service.get_cached_dataframe(feature_store_id, query)
    pd.testing.assert_frame_equal(result, dataframe)

    # Check retrieval on a later date after cache should be state (even without cleanup)
    with freeze_time(datetime.utcnow() + timedelta(days=100)):
        result = await service.get_cached_dataframe(feature_store_id, query)
        assert result is None


@pytest.mark.asyncio
async def test_get_or_cached_dataframe(service, feature_store_id, mock_snowflake_session):
    """
    Test get_or_cache_dataframe
    """
    query = "SELECT * FROM table_1"
    dataframe = pd.DataFrame({"a": [1, 2, 3]}, index=["x", "y", "z"])
    mock_snowflake_session.execute_query_long_running.return_value = dataframe
    result_1 = await service.get_or_cache_dataframe(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        query=query,
    )
    result_2 = await service.get_or_cache_dataframe(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        query=query,
    )
    pd.testing.assert_frame_equal(result_1, dataframe)
    pd.testing.assert_frame_equal(result_2, dataframe)
    assert mock_snowflake_session.execute_query_long_running.call_count == 1


@pytest.mark.asyncio
async def test_get_or_cached_dataframe_with_error(
    service, feature_store_id, mock_snowflake_session, storage
):
    """
    Test get_or_cache_dataframe when the cached object cannot be retrieved unexpectedly
    """
    query = "SELECT * FROM table_1"
    dataframe = pd.DataFrame({"a": [1, 2, 3]}, index=["x", "y", "z"])
    mock_snowflake_session.execute_query_long_running.return_value = dataframe
    result_1 = await service.get_or_cache_dataframe(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        query=query,
    )
    # Simulate error when retrieving the cached object
    storage.get_dataframe = Mock(side_effect=FileNotFoundError)
    result_2 = await service.get_or_cache_dataframe(
        session=mock_snowflake_session,
        feature_store_id=feature_store_id,
        query=query,
    )
    # Query should be executed and cached again
    pd.testing.assert_frame_equal(result_1, dataframe)
    pd.testing.assert_frame_equal(result_2, dataframe)
    assert mock_snowflake_session.execute_query_long_running.call_count == 2


@pytest.mark.asyncio
async def test_stop_job_with_queries(
    service, cleanup_scheduler_service, feature_store_id, periodic_task_service
):
    """
    Test stopping job no-op if there are still cached queries
    """
    query = "SELECT * FROM table_1"
    await service.cache_table(feature_store_id, query, "created_table_1")
    tasks = await get_all_periodic_tasks(periodic_task_service)
    assert len(tasks) == 1

    # Try stopping job when there are still cached queries
    await cleanup_scheduler_service.stop_job_if_no_longer_needed(feature_store_id)
    tasks = await get_all_periodic_tasks(periodic_task_service)
    assert len(tasks) == 1


@pytest.mark.asyncio
async def test_cleanup_service(
    service,
    cleanup_service,
    periodic_task_service,
    feature_store_id,
    mock_snowflake_session,
):
    """
    Test cleanup service
    """
    query = "SELECT * FROM table_1"

    with freeze_time("2024-01-01"):
        await service.cache_table(feature_store_id, query, "created_table_1")
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 1
        assert mock_snowflake_session.drop_table.call_args_list == []

    with freeze_time("2024-01-02"):
        await service.cache_table(feature_store_id, query, "created_table_2")
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 1
        assert mock_snowflake_session.drop_table.call_args_list == []

    # Cleanup before stale
    with freeze_time("2024-01-03"):
        await cleanup_service.run_cleanup(feature_store_id)
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 1
        assert mock_snowflake_session.drop_table.call_args_list == []

    # Cleanup before stale (first document already not retrievable but not yet ready for cleanup)
    with freeze_time("2024-01-22 03:00:00"):
        await cleanup_service.run_cleanup(feature_store_id)
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 1
        assert mock_snowflake_session.drop_table.call_args_list == []

    # Cleanup after the first query was stale and ready for cleanup
    with freeze_time("2024-01-22 03:00:01"):
        await cleanup_service.run_cleanup(feature_store_id)
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 1
        assert mock_snowflake_session.drop_table.call_args_list == [
            call(table_name="created_table_1", schema_name="sf_schema", database_name="sf_db")
        ]

    # Cleanup after all the remaining query was stale
    mock_snowflake_session.drop_table.reset_mock()
    with freeze_time("2024-01-24"):
        await cleanup_service.run_cleanup(feature_store_id)
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 0
        assert mock_snowflake_session.drop_table.call_args_list == [
            call(table_name="created_table_2", schema_name="sf_schema", database_name="sf_db")
        ]


@pytest.mark.asyncio
async def test_cleanup_service_dataframes(
    app_container,
    service,
    cleanup_service,
    periodic_task_service,
    feature_store_id,
    mock_snowflake_session,
):
    """
    Test cleanup service on cached tables
    """
    _ = mock_snowflake_session

    query = "SELECT * FROM table_1"
    dataframe = pd.DataFrame({"a": [1, 2, 3]}, index=["x", "y", "z"])

    with freeze_time("2024-01-01"):
        await service.cache_dataframe(feature_store_id, query, dataframe)
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 1

    # Check file exists in storage
    cache_key = service._get_cache_key(feature_store_id, query, QueryCacheType.DATAFRAME)
    docs = await service.query_cache_document_service.list_documents_as_dict(
        query_filter={"cache_key": cache_key}
    )
    assert len(docs["data"]) == 1
    storage_path = docs["data"][0]["cached_object"]["storage_path"]
    await app_container.storage.get_bytes(storage_path)

    with freeze_time("2024-01-23"):
        await cleanup_service.run_cleanup(feature_store_id)
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 0

    with pytest.raises(FileNotFoundError):
        await app_container.storage.get_bytes(storage_path)


@pytest.mark.asyncio
async def test_cleanup_service_error_handling(
    service,
    document_service,
    cleanup_service,
    periodic_task_service,
    feature_store_id,
    mock_snowflake_session,
):
    """
    Test cleanup service error handling
    """
    query_1 = "SELECT * FROM table_1"
    query_2 = "SELECT * FROM table_2"

    with freeze_time("2024-01-01"):
        await service.cache_table(feature_store_id, query_1, "created_table_1")
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 1
        assert mock_snowflake_session.drop_table.call_args_list == []

    with freeze_time("2024-01-02"):
        await service.cache_table(feature_store_id, query_2, "created_table_2")
        tasks = await get_all_periodic_tasks(periodic_task_service)
        assert len(tasks) == 1
        assert mock_snowflake_session.drop_table.call_args_list == []

    def _mock_drop_table(**kwargs):
        if kwargs["table_name"] == "created_table_2":
            raise RuntimeError("Error dropping table")

    # Simulate error when cleaning up a query
    mock_snowflake_session.drop_table.reset_mock()
    with freeze_time("2024-01-24"):
        mock_snowflake_session.drop_table.side_effect = _mock_drop_table
        await cleanup_service.run_cleanup(feature_store_id)
        # Check attempts to drop tables
        assert mock_snowflake_session.drop_table.call_args_list == [
            call(table_name="created_table_2", schema_name="sf_schema", database_name="sf_db"),
            call(table_name="created_table_1", schema_name="sf_schema", database_name="sf_db"),
        ]
        # The cache document corresponding to the failed table should still exist
        stale_docs = []
        async for doc in document_service.list_stale_documents_as_dict_iterator(feature_store_id):
            stale_docs.append(doc)
        assert len(stale_docs) == 1
        assert stale_docs[0]["cached_object"]["table_name"] == "created_table_2"

    # Retry cleanup should work
    with freeze_time("2024-01-25"):
        mock_snowflake_session.drop_table.reset_mock()
        mock_snowflake_session.drop_table.side_effect = None
        await cleanup_service.run_cleanup(feature_store_id)
        assert mock_snowflake_session.drop_table.call_args_list == [
            call(table_name="created_table_2", schema_name="sf_schema", database_name="sf_db"),
        ]
        stale_docs = []
        async for doc in document_service.list_stale_documents_as_dict_iterator(feature_store_id):
            stale_docs.append(doc)
        assert len(stale_docs) == 0
